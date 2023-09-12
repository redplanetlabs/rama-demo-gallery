(ns rama.gallery.top-users-module
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]))

;; This module demonstrates "top N" analytics in the context of computing the top spending users in
;; an e-commerce application. The module receives a stream of purchase data and incrementally maintains
;; a global list of the top 500 users by total purchase amount.

;; This module only does the analytics portion. It can be combined with code such as in ProfilesModule to
;; also handle things like account registration and profile management.

;; See the test file top_users_module_test.clj for examples of querying the top users.


(def TOP-AMOUNT 500)

;; As with all the demos, data is represented using plain Clojure records. You can represent
;; data however you want, and we generally recommend using a library with compact serialization,
;; strong schemas, and support for evolving types (like Thrift or Protocol Buffers). We use plain
;; Clojure records in these demos to keep them as simple as possible by not having additional
;; dependencies. Rama uses Nippy for serialization, and you can extend that directly or define a custom
;; serialization through Rama to support your own representations. In all cases you always work with
;; first-class objects all the time when using Rama, whether appending to depots, processing in ETLs,
;; or querying from PStates.
(defrecord Purchase [user-id purchase-cents])

;;   This function implements part of the ETL below for maintaining top users. It's responsible for
;; updating the total spend amount for each user and emitting in each iteration of processing two
;; fields: userId's with a purchase and their updated total spend amount.
;;   A "generator" processes a batch of data and emits a new batch of data after performing any amount
;; of computation, aggregation, joins, or other logic on it.
(defgenerator user-spend-subbatch
  [microbatch]
  (batch<- [*user-id *total-spend-cents]
           ;; This emits the input batch of data across all partitions.
           (microbatch :> {:keys [*user-id *purchase-cents]})
           ;; Batch blocks must always declare a partitioner before aggregating. In this case, we wish
           ;; to partition the aggregation of total spend amounts by user ID.
           (|hash *user-id)
           ;; This writes to the PState in the form of an aggregator, which specifies the write in the
           ;; shape of the data structure being written to. At the leaf is the +sum aggregator which
           ;; adds each purchase into the total for that user. :new-val> is a special feature
           ;; available in batch blocks to capture the updated values and emit them along with the keys
           ;; used in the path to that position in the PState. In this case, following the +compound
           ;; the variables *user-id and *total-spend-cents are bound for each user updated in this
           ;; iteration.
           (+compound $$user-total-spend
                      {*user-id (aggs/+sum *purchase-cents
                                           :new-val> *total-spend-cents)})))

;; This defines the module, whose body is a regular Clojure function implementation. All depots, ETLs,
;; PStates, and query topologies are defined via this entry point.
(defmodule TopUsersModule
  [setup topologies]
  ;; This depot takes in Purchase objects. The second argument is a "depot partitioner" that controls
  ;; how appended data is partitioned across the depot, affecting on which task each piece of data begins
  ;; processing in ETLs.
  (declare-depot setup *purchase-depot (hash-by :user-id))
    ;; Defines the ETL as a microbatch topology. Microbatch topologies have higher throughput than stream topologies
    ;; with the tradeoff of update latency being in the hundreds of milliseconds range rather than single-digit milliseconds
    ;; range. They are generally preferable for analytics-oriented use cases like this one where the extra latency
    ;; doesn't matter.
  (let [mb (microbatch-topology topologies "topusers")]
    ;;   PStates are durable and replicated datastores and are represented as an arbitrary combination of data structures. Reads
    ;; and writes to PStates go to disk and are not purely in-memory operations.
    ;;   This PState stores the total spend amount for each user, a map from user ID to spend amount (in cents).
    (declare-pstate mb $$user-total-spend {Long Long})
    ;; This PState stores the list of the top 500 spending users. Since it's just a single list, it's declared as a
    ;; global PState. Global PStates only have a single partition. Note that the schema of the PState is just a plain
    ;; list and not a map like almost all databases are (with a "key" being the central concept to identify a record
    ;; or row).
    (declare-pstate mb $$top-spending-users java.util.List {:global? true})

    ;; <<sources defines the ETL logic as Rama dataflow code. Rama's dataflow API works differently than Clojure, but it has
    ;; the same expressiveness as any general purpose language while also being able to seamlessly distribute computation.
    (<<sources mb
      ;; This subscribes the ETL to *purchase-depot, binding the batch of all data in this microbatch to %microbatch.
      ;; %microbatch is an anonymous operation which when invoked emits all data for the microbatch across all partitions.
      (source> *purchase-depot :> %microbatch)
      ;; Batch blocks are an enhanced computation mode for dataflow with the same capabilities as relational languages
      ;; (like SQL) such as inner joins, outer joins, subqueries, and aggregation. See this section of the Rama docs
      ;; for more details: https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_batch_blocks
      (<<batch
        ;; First, the total spend amounts are updated in a subbatch. This subbatch emits all updated users
        ;; and their new total spend amounts.
        (user-spend-subbatch %microbatch :> *user-id *total-spend-cents)
        ;; This prepares for aggregating the data by combining the two variables into a 2-tuple.
        (vector *user-id *total-spend-cents :> *tuple)
        ;; The list of top users is stored on a global partition, so the aggregation is partitioned
        ;; accordingly.
        (|global)
        ;; The +top-monotonic aggregator updates a list according to the provided specification. This instance
        ;; says to add data in *tuple into the aggregated list, and to keep the top 500. The aggregator
        ;; only keeps the latest record for each ID, which here is specified as the first element of the tuple
        ;; (the user ID). The "sort val" is what the aggregator uses for ranking, in this case the total spend
        ;; amount in the last position of the tuple.
        (aggs/+top-monotonic [TOP-AMOUNT]
                             $$top-spending-users
                             *tuple
                             :+options {:id-fn first
                                        :sort-val-fn last})

        ))))
