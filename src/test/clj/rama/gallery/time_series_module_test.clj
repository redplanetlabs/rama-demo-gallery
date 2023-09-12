(ns rama.gallery.time-series-module-test
  (:use [com.rpl rama]
        [com.rpl.rama path])
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.rpl.rama.test :as rtest]
   [rama.gallery.time-series-module :as tsm]))

;; gets a random timestamp in the specified minute bucket
(defn minute [bucket]
  (+ (* bucket 60 1000) (rand-int 60000)))

(deftest time-series-module-test
  ;; create-ipc creates an InProcessCluster which simulates a full Rama cluster in-process and is an ideal environment for
  ;; experimentation and unit-testing.
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc tsm/TimeSeriesModule {:tasks 4 :threads 2})
    (let [module-name (get-module-name tsm/TimeSeriesModule)
          ;; Client usage of IPC is identical to using a real cluster. Depot and PState clients are fetched by
          ;; referencing the module name along with the variable used to identify the depot/PState within the module.
          render-latency-depot (foreign-depot ipc module-name "*render-latency-depot")
          window-stats (foreign-pstate ipc module-name "$$window-stats")
          get-stats-for-minute-range (foreign-query ipc module-name "get-stats-for-minute-range")
          expected-stats3 (tsm/->WindowStats 2 30 20 10 20)
          expected-stats10 (tsm/->WindowStats 3 66 33 15 33)]

      ;; add some test data across many time buckets
      (foreign-append! render-latency-depot (tsm/->RenderLatency "foo.com" 10 (minute 3)))
      (foreign-append! render-latency-depot (tsm/->RenderLatency "foo.com" 20 (minute 3)))
      (foreign-append! render-latency-depot (tsm/->RenderLatency "foo.com" 15 (minute 10)))
      (foreign-append! render-latency-depot (tsm/->RenderLatency "foo.com" 18 (minute 10)))
      (foreign-append! render-latency-depot (tsm/->RenderLatency "foo.com" 33 (minute 10)))
      (foreign-append! render-latency-depot (tsm/->RenderLatency "foo.com" 20 (minute 65)))
      (foreign-append! render-latency-depot (tsm/->RenderLatency "foo.com" 30 (minute 65)))
      (foreign-append! render-latency-depot (tsm/->RenderLatency "foo.com" 100 (minute (* 60 24))))
      (foreign-append! render-latency-depot (tsm/->RenderLatency "foo.com" 100 (minute (+ (* 60 24) 8))))
      (foreign-append! render-latency-depot (tsm/->RenderLatency "foo.com" 50 (minute (+ (* 60 48) 122))))

      ;; Microbatching runs asynchronously to depot appends, so this code waits for microbatching to finish
      ;; processing all the depot appends so we can see those appends reflected in PState queries.
      (rtest/wait-for-microbatch-processed-count ipc module-name "timeseries" 10)

      ;; Verify that a single bucket is correct.
      (is (= expected-stats3
             (foreign-select-one (keypath "foo.com" :m 3) window-stats)))

      ;; This is an example of doing a range query. This fetches a submap from the PState from buckets 3 (inclusive) to
      ;; 11 (exclusive) for :m granularity. That inner map is subindexed, and subindexed structures are sorted. Range
      ;; queries on subindexed structures are very efficient.
      (is (= (sorted-map 3 expected-stats3
                         10 expected-stats10)
             (foreign-select-one [(keypath "foo.com" :m)
                                  (sorted-map-range 3 11)]
               window-stats)))

      ;; This is an example of doing an aggregation within a PState query. This fetches the number of buckets between
      ;; minute 0 and minute 60*72. Critically, this entire path executes server-side. So the only thing transferred
      ;; back from the server is the count.
      (is (= 6
             (foreign-select-one [(keypath "foo.com" :m)
                                  (sorted-map-range 0 (* 60 72))
                                  (view count)]
               window-stats)))

      ;; This is an example of invoking a query topology, which is just like invoking any regular function. You pass
      ;; it some arguments, and it returns a result back. The difference is it runs as a distributed computation on
      ;; a Rama cluster. This query topology efficiently fetches the aggregate WindowStats for an arbitrary range of
      ;; minute  buckets, utilizing coarser granularities if possible to minimize the amount of buckets that need to
      ;; be fetched to perform the computation.
      (is (= (tsm/->WindowStats 10 396 50 10 100)
             (foreign-invoke-query get-stats-for-minute-range "foo.com" 0 (* 60 72))))

      ;; Verify that buckets at coarser granularity are aggregated correctly.
      (is (= (tsm/->WindowStats 7 146 30 10 33)
             (foreign-select-one (keypath "foo.com" :d 0) window-stats)))
      )))

(deftest query-granularities-test
  (let [res (tsm/query-granularities :m 63 10033)]
    (is (= 5 (count res)))
    (is (= #{[:m 63 120] [:h 2 24] [:d 1 6] [:h 144 167] [:m 10020 10033]}
           (set res))))
  (let [res (tsm/query-granularities :d 3 122)]
    (is (= 3 (count res)))
    (is (= #{[:d 3 30] [:td 1 4] [:d 120 122]}
           (set res)))))
