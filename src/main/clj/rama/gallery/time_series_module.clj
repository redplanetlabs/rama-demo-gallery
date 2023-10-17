(ns rama.gallery.time-series-module
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]))

;; This module demonstrates time-series analytics for the example use case of the latency of rendering different URLs
;; on a website. The module receives a stream of latencies for individual renders and accumulates min/max/average
;; stats for windows at minute, hour, daily, and monthly granularities. Accumulating at multiple granularities speeds
;; up queries over larger ranges.
;;
;; See the test file time_series_module_test.clj for how a client interacts with this module to perform various kinds
;; of range queries, including aggregating a range of data server-side.
;;
;; The stats computed for each window are min, max, average, and latest. To capture full distributions, you can add
;; a data structure such as T-Digest to the WindowStats object.

;; As with all the demos, data is represented using plain Clojure records. You can represent
;; data however you want, and we generally recommend using a library with compact serialization,
;; strong schemas, and support for evolving types (like Thrift or Protocol Buffers). We use plain
;; Clojure records in these demos to keep them as simple as possible by not having additional
;; dependencies. Rama uses Nippy for serialization, and you can extend that directly or define a custom
;; serialization through Rama to support your own representations. In all cases you always work with
;; first-class objects all the time when using Rama, whether appending to depots, processing in ETLs,
;; or querying from PStates.
(defrecord RenderLatency
  [url render-millis timestamp-millis])

(defrecord WindowStats
  [cardinality total last-millis min-latency-millis max-latency-millis])

;; This defines an aggregator that combines two WindowStats objects into one aggregated WindowStats object.
;; It is used both in the ETL to update the time-series PState as well as the query topology fetching the
;; WindowStats for a specific range.
(def +combine-measurements
  (combiner
    (fn [window-stats1 window-stats2]
      (->WindowStats
        (+ (:cardinality window-stats1) (:cardinality window-stats2))
        (+ (:total window-stats1) (:total window-stats2))
        (or (:last-millis window-stats2) (:last-millis window-stats1))
        (cond
          (nil? (:min-latency-millis window-stats1))
          (:min-latency-millis window-stats2)

          (nil? (:min-latency-millis window-stats2))
          (:min-latency-millis window-stats1)

          :else
          (min (:min-latency-millis window-stats1)
               (:min-latency-millis window-stats2)))
        (cond
          (nil? (:max-latency-millis window-stats1))
          (:max-latency-millis window-stats2)

          (nil? (:max-latency-millis window-stats2))
          (:max-latency-millis window-stats1)

          :else
          (max (:max-latency-millis window-stats1)
               (:max-latency-millis window-stats2)))))
    :init-fn (fn [] (->WindowStats 0 0 nil nil nil))))

(defn- single-window-stat [render-millis]
  (->WindowStats 1 render-millis render-millis render-millis render-millis))

;; This is a custom operation used in the ETL to emit the time bucket to index each RenderLatency record
;; for each minute, day, hour, and thirty-day granularity.
(deframaop emit-index-granularities [*timestamp-millis]
  (long (/ *timestamp-millis (* 1000 60)) :> *minute-bucket)
  (long (/ *minute-bucket 60) :> *hour-bucket)
  (long (/ *hour-bucket 24) :> *day-bucket)
  (long (/ *day-bucket 30) :> *thirty-day-bucket)
  (:> :m *minute-bucket)
  (:> :d *day-bucket)
  (:> :h *hour-bucket)
  (:> :td *thirty-day-bucket))

;; These constants are used in the helper function below "emitQueryGranularities".
(def NEXT-GRANULARITY
  {:m :h
   :h :d
   :d :td})

(def NEXT-GRANULARITY-DIVISOR
  {:m 60
   :h 24
   :d 30})

;; This helper function is used in the query topology below to compute the minimal number of buckets that need to
;; be queried across all granularities to satisfy a query over an arbitrary range of minute buckets. For example, if querying
;; from minute 58 of hour 6 through minute 3 of hour 20 (both in the same day), then the queries that need to be done are:
;;   - :m granularity, minute 58 of hour 6 through minute 0 of hour 7
;;   - :h granularity, hour 7 through hour 20
;;   - :m granularity, minute 0 of hour 20 through minute 3 of hour 20
(defn query-granularities [granularity start-bucket end-bucket]
  (let [next-granularity (get NEXT-GRANULARITY granularity)]
    (if (nil? next-granularity)
      [[granularity start-bucket end-bucket]]
      (let [divisor (get NEXT-GRANULARITY-DIVISOR granularity)
            next-start-bucket (cond->
                                (long (/ start-bucket divisor))

                                (not= 0 (mod start-bucket divisor))
                                inc)
            next-end-bucket (long (/ end-bucket divisor))
            next-aligned-start-bucket (* next-start-bucket divisor)
            next-aligned-end-bucket (* next-end-bucket divisor)
            more (if (> next-end-bucket next-start-bucket)
                   (query-granularities next-granularity
                                        next-start-bucket
                                        next-end-bucket))]
            (concat more
                    (if (>= next-aligned-start-bucket next-aligned-end-bucket)
                      [[granularity start-bucket end-bucket]]
                      (cond-> []
                        (> next-aligned-start-bucket start-bucket)
                        (conj [granularity start-bucket next-aligned-start-bucket])

                        (> end-bucket next-aligned-end-bucket)
                        (conj [granularity next-aligned-end-bucket end-bucket])
                        )))))))

;; This defines the module, whose body is a regular Clojure function implementation. All depots, ETLs,
;; PStates, and query topologies are defined via this entry point.
(defmodule TimeSeriesModule
  [setup topologies]
  ;; This depot takes in RenderLatency objects. The second argument is a "depot partitioner" that controls
  ;; how appended data is partitioned across the depot, affecting on which task each piece of data begins
  ;; processing in ETLs.
  (declare-depot setup *render-latency-depot (hash-by :url))

  ;; Defines the ETL as a microbatch topology. Microbatch topologies have higher throughput than stream topologies
  ;; with the tradeoff of update latency being in the hundreds of milliseconds range rather than single-digit milliseconds
  ;; range. They are generally preferable for analytics-oriented use cases like this one where the extra latency
  ;; doesn't matter.
  (let [mb (microbatch-topology topologies "timeseries")]
    ;;   PStates are durable and replicated datastores and are represented as an arbitrary combination of data structures. Reads
    ;; and writes to PStates go to disk and are not purely in-memory operations.
    ;;   This PState stores bucketed stats for all granularities for each URL. Minute/hour/day/month granularities use the
    ;; keywords :m, :h, :d, and :td as keys in that position of the data structure. The final map in the data structure
    ;; is subindexed because it can contain millions of elements. Subindexing stores each value of those maps individually
    ;; and enables them to be written and queried efficiently even when they're huge. Subindexed maps are always sorted, and
    ;; it's also easy to do range queries on them. This is demonstrated in the query topology below.
    ;;   This PState is structured so that all granularities for a given URL are stored on the same partition. This allows queries
    ;; for large time ranges that need to fetch data from multiple granularities to be efficient by fetching all data from one
    ;; partition (as opposed to needing to fetch different granularities from different partitions). The query topology below
    ;; demonstrates this.
    ;;   Note that the coarser time granularities take up very little additional space because they have so many fewer buckets.
    ;; The hour granularity has 60x fewer buckets than the minute granularity, and the daily granularity has 24x fewer buckets
    ;; than the hour granularity. Space usage for time-series indexes like this is dominated by the finest granularity.
    (declare-pstate
      mb
      $$window-stats
      {String ; url
        {clojure.lang.Keyword ; granularity
          (map-schema Long ; bucket
                      WindowStats
                      {:subindex? true})}})
    ;; <<sources defines the ETL logic as Rama dataflow code. Rama's dataflow API works differently than Clojure, but it has
    ;; the same expressiveness as any general purpose language while also being able to seamlessly distribute computation.
    (<<sources mb
      ;; This subscribes the ETL to *render-latency-depot, binding the batch of all data in this microbatch to %microbatch.
      ;; %microbatch is an anonymous operation which when invoked emits all data for the microbatch across all partitions.
      (source> *render-latency-depot :> %microbatch)
      ;; Because of the depot partitioner on *render-latency-depot, computation for each piece of data
      ;; starts on the same task where stats are stored for that URL in the $$window-stats PState.
      (%microbatch :> {:keys [*url *render-millis *timestamp-millis]})
      ;; The code for updating the stats in the PState is defined with a combiner aggregator, so it needs as input a WindowStats
      ;; object with just *render-millis in it. This helper function constructs that object and binds it to the variable
      ;; *single-stat.
      (single-window-stat *render-millis :> *single-stat)
      ;; This invokes the helper function above to emit the bucket to index the new dataa for each granularity. Note how this
      ;; operation emits two fields, *granularity and *bucket.
      (emit-index-granularities *timestamp-millis :> *granularity *bucket)
      ;; The writes to the $$window-stats PState are done with a compound aggregator, which specifies the write in the shape
      ;; of the data structure being written to. At the leaf of this aggregator is the +combine-measurements aggregator, defined
      ;; at the beginning of this file. It takes as input whatever WindowStats object is already stored in the PState at
      ;; that position as well as the WindowStats object in *singleStat.
      (+compound $$window-stats
                 {*url
                   {*granularity
                     {*bucket (+combine-measurements *single-stat)}}}))
    ;;   This defines a query topology for getting the aggregated stats for a range of minute buckets. Rather than fetch all
    ;; the minute buckets between the start and end buckets, it uses higher granularity buckets if possible to minimize the
    ;; amount of data that needs to be fetched from the PState.
    ;;   Unlike the ETL code above, query topologies are batched computations. Batched computations in Rama can do the same things
    ;; you can do with relational languages like SQL: inner joins, outer joins, subqueries, and aggregations. This particular
    ;; query topology is straightforward and simply aggregates all fetched WindowStats into a single returned WindowStats object.
    ;;   Since query topologies are colocated with the PStates in their module, they are very efficient. This query topology does
    ;; potentially many queries to the "$$windowStats" PState and aggregates them together without any network transfer in between.
    ;;   This query topology definition specifies it takes as input three arguments – *url, *start-bucket, and *end-bucket –
    ;; and will bind a variable called *stats for the return value.
    (<<query-topology topologies "get-stats-for-minute-range"
      [*url *start-bucket *end-bucket :> *stats]
      ;; First, the query topology switches to the task containing data for this URL. Query topologies are optimized when
      ;; there's a leading partitioner like this, performing the routing client-side instead of when on a module task. This
      ;; means clients of this query topology send their requests directly to the task containing the needed data.
      (|hash *url)
      ;; This uses a helper function to emit all ranges of data for each granularity that need to be fetched.
      (ops/explode (query-granularities :m *start-bucket *end-bucket)
        :> [*granularity *gstart *gend])
      ;; This fetches each individual WindowStats object that needs to be aggregated. sorted-map-range selects a submap from
      ;; the subindexed map at that position, and MAP-VALS navigates to every value of that map individually. Since the function
      ;; call right before this emits many times, this local-select> is executed for each of those emits. Individual WindowStats
      ;; objects are bound to the variable *bucket-stat.
      (local-select> [(keypath *url *granularity)
                      (sorted-map-range *gstart *gend)
                      MAP-VALS]
        $$window-stats
        :> *bucket-stat)
      ;; Every query topology must have an |origin call, which indicates to move the computation back to where the query started.
      (|origin)
      ;;   This aggregates all *bucket-stat objects emitted into a single object bound to the variable *stats. Note that this
      ;; is the variable specified at the start of the query topology to name the return value.
      ;;   The +combine-measurements aggregator here is the same one as used in the ETL above.
      (+combine-measurements *bucket-stat :> *stats))
    ))
