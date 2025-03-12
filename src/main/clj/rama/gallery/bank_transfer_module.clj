(ns rama.gallery.bank-transfer-module
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]))

;; This module demonstrates transferring funds from one account to another. The implementation guarantees:
;;   - All transfers are processed exactly once.
;;   - A transfer only goes through if there are sufficient funds available.
;;   - No race conditions with concurrent transfers.
;;
;; See the test file bank_transfer_module_test.clj for how a client interacts with this module to initiate
;; transfers and query for funds and transfer information.

;; As with all the demos, data is represented using plain Clojure records. You can represent
;; data however you want, and we generally recommend using a library with compact serialization,
;; strong schemas, and support for evolving types (like Thrift or Protocol Buffers). We use plain
;; Clojure records in these demos to keep them as simple as possible by not having additional
;; dependencies. Rama uses Nippy for serialization, and you can extend that directly or define a custom
;; serialization through Rama to support your own representations. In all cases you always work with
;; first-class objects all the time when using Rama, whether appending to depots, processing in ETLs,
;; or querying from PStates.
(defrecord Transfer [transfer-id from-user-id to-user-id amt])
(defrecord Deposit [user-id amt])

;; This defines the module, whose body is a regular Clojure function implementation. All depots, ETLs,
;; PStates, and query topologies are defined via this entry point.
(defmodule BankTransferModule
  [setup topologies]
  ;; This depot takes in Transfer objects. The second argument is a "depot partitioner" that controls
  ;; how appended data is partitioned across the depot, affecting on which task each piece of data begins
  ;; processing in ETLs.
  (declare-depot setup *transfer-depot (hash-by :from-user-id))
  ;; This depot takes in Deposit objects.
  (declare-depot setup *deposit-depot (hash-by :user-id))
  ;;   Defines the ETL as a microbatch topology. Microbatch topologies have exactly-once processing semantics, meaning
  ;; that even if there are failures and the work needs to be retried, the updates into PStates will be as if there
  ;; were no failures and every depot record was processed exactly once. The exactly-once semantics are critical
  ;; for this use case.
  ;;   Microbatch topologies also have higher throughput than stream topologies with the tradeoff of update latency
  ;; being in the hundreds of milliseconds range rather than single-digit milliseconds range. This is a suitable latency
  ;; for this task.
  (let [mb (microbatch-topology topologies "banking")]
    ;;   PStates are durable and replicated datastores and are represented as an arbitrary combination of data structures. Reads
    ;; and writes to PStates go to disk and are not purely in-memory operations.
    ;;   This PState stores the total funds for each user, a map from user ID to funds.
    (declare-pstate mb $$funds {Long Long})
    ;; These two PStates store outgoing and incoming transfer information. The inner map in the data structure
    ;; is subindexed because it can contain an unbounded number of elements. Subindexing stores each value of
    ;; those maps individually and enables them to be written and queried efficiently even when they're huge.
    ;; Subindexed maps are always sorted, and it's also easy to do range queries on them.
    (declare-pstate mb $$outgoing-transfers
                       {Long ; user-id
                        (map-schema String ; transfer-id
                                    (fixed-keys-schema {:to-user-id Long
                                                        :amt Long
                                                        :success? Boolean})
                                    {:subindex? true})})
    (declare-pstate mb $$incoming-transfers
                       {Long ; user-id
                        (map-schema String ; transfer-id
                                    (fixed-keys-schema {:from-user-id Long
                                                        :amt Long
                                                        :success? Boolean})
                                    {:subindex? true})})
    ;; <<sources defines the ETL logic as Rama dataflow code. Rama's dataflow API works differently than Clojure, but it has
    ;; the same expressiveness as any general purpose language while also being able to seamlessly distribute computation.
    (<<sources mb
      ;; This subscribes the ETL to *transfer-depot, binding the batch of all data in this microbatch to %microbatch.
      ;; %microbatch is an anonymous operation which when invoked emits all data for the microbatch across all partitions.
      (source> *transfer-depot :> %microbatch)
      ;; Because of the depot partitioner on *transfer-depot, computation for each piece of data
      ;; starts on the same task where funds and transfer information are stored for the "from user ID"
      ;; in the $$funds, $$outgoing-transfers, and $$incoming-transfers PStates.
      (%microbatch :> {:keys [*transfer-id *from-user-id *to-user-id *amt]})
      ;; First check if the user has sufficient funds for the transfer and bind that to the boolean variable
      ;; *success?. Note that because task threads execute events serially, there are no race conditions here
      ;; with concurrent transfers since other transfer requests will be queued behind this event on this task.
      (local-select> [(keypath *from-user-id) (nil->val 0)] $$funds :> *funds)
      (>= *funds *amt :> *success?)
      ;; If this transfer is valid, then deduct the funds for from-user-id from the $$funds PState.
      (<<if *success?
        ;; This defines an anonymous operation that is used in the transform call to deduct the
        ;; transfer amount from the current funds.
        (<<ramafn %deduct [*curr]
          (:> (- *curr *amt)))
        (local-transform> [(keypath *from-user-id) (term %deduct)] $$funds))
      ;; Record the transfer in the $$outgoing-transfers PState for from-user-id.
      (local-transform> [(keypath *from-user-id *transfer-id)
                         (termval {:to-user-id *to-user-id
                                   :amt *amt
                                   :success? *success?})]
                        $$outgoing-transfers)
      ;; This switches to the task storing information for to-user-id, which may be on a different machine.
      (|hash *to-user-id)
      ;; If this transfer is valid, then credit the funds to to-user-id in the $$funds PState. Note that microbatching
      ;; has exactly-once semantics across the whole microbatch, which provides the cross-partition transactionality
      ;; needed for this use case.
      (<<if *success?
        ;; Aggregation is a way to specify a PState update in a slightly higher level way. It specifies the update
        ;; in the shape of the data structure being written to, and it takes care of initializing non-existent values.
        ;; In this, the +sum aggregator knows to initialize the funds to 0 if that value doesn't exist for this user yet.
        (+compound $$funds {*to-user-id (aggs/+sum *amt)}))
      ;; Record the transfer in the $$incoming-transfers PState for to-user-id.
      (local-transform> [(keypath *to-user-id *transfer-id)
                         (termval {:from-user-id *from-user-id
                                   :amt *amt
                                   :success? *success?})]
                        $$incoming-transfers)

      ;; This subscribes the topology to *deposit-depot and defines the ETL logic for it.
      (source> *deposit-depot :> %microbatch)
      (%microbatch :> {:keys [*user-id *amt]})
      (+compound $$funds {*user-id (aggs/+sum *amt)})
      )))
