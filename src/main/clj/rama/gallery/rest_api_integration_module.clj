(ns rama.gallery.rest-api-integration-module
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [taoensso.nippy :as nippy])
  (:import [com.rpl.rama.integration TaskGlobalObject]
           [org.asynchttpclient AsyncHttpClient Dsl]
           [org.asynchttpclient.netty NettyResponse]))

;; This module demonstrates integrating Rama with an external service, in this case a REST API.
;;
;; See the test file rest_api_integration_module_test.clj for examples of interacting with this module.

(defprotocol FetchTaskGlobalClient
  (task-global-client [this]))

;;   This defines a "task global" object, which when used with declare-object (as shown below), creates a value that
;; can be referenced on all tasks in both ETLs and query topologies. This interface specializes the object on each
;; task with lifecycle methods "prepareForTask" and "close". This interface can be used for anything from creating
;; task-specific caches to clients to external systems. The latter use case is demonstrated here by creating an HTTP
;; client and managing its lifecycle through this interface.
;;   Many external client interfaces can be shared on the same thread, or if thread-safe can be shared among all
;; threads in the same worker. The documentation for this API explores how to manage resources like that, and the
;; rama-kafka project is a real-world example of doing so. Links:
;;  - https://redplanetlabs.com/docs/~/integrating.html
;;  - https://github.com/redplanetlabs/rama-kafka
;;   Note that in Clojure you're likely better off using a native Clojure HTTP library such as http-kit. This
;; example is using AsyncHttpClient as a demonstration of integrating with any Java API. From this example you can
;; see how you'd interact with external databases, monitoring systems, or other tools as well.
(deftype AsyncHttpClientTaskGlobal
  [^{:unsynchronized-mutable true
     :tag AsyncHttpClient}
   client]
  TaskGlobalObject
  (prepareForTask [this task-id task-global-context]
    (set! client (Dsl/asyncHttpClient)))
  (close [this]
    (.close client))
  FetchTaskGlobalClient
  (task-global-client [this] client))

;; Rama uses Nippy for serialization, and it doesn't work properly for deftypes. So these
;; calls tell Nippy how to serialize/deserialize the task global type.
(nippy/extend-freeze AsyncHttpClientTaskGlobal ::async-http-client
  [o data-output])

(nippy/extend-thaw ::async-http-client [data-input]
  (AsyncHttpClientTaskGlobal. nil))

(defn http-get-future [^AsyncHttpClient client url]
  (-> client (.prepareGet url) .execute .toCompletableFuture))

(defn get-body [^NettyResponse response]
  (.getResponseBody response))

;; This defines the module, whose body is a regular Clojure function implementation. All depots, ETLs,
;; PStates, and query topologies are defined via this entry point.
(defmodule RestAPIIntegrationModule
  [setup topologies]
  ;; This depot takes in URL strings. The second argument is a "depot partitioner" that controls
  ;; how appended data is partitioned across the depot, affecting on which task each piece of data begins
  ;; processing in ETLs.
  (declare-depot setup *get-depot (hash-by identity))
  ;; This declares a task global with the given value. Since AsyncHttpClientTaskGlobal implements the TaskGlobalObject
  ;; interface, the value is specialized per task. Accessing the variable *http-client in topologies always accesses the
  ;; value local to the task where the topology event is running.
  (declare-object setup *http-client (AsyncHttpClientTaskGlobal. nil))

  ;; Stream topologies process appended data within a few milliseconds and guarantee all data will be fully processed.
  (let [s (stream-topology topologies "get-http")]
    ;;   PStates are durable and replicated datastores and are represented as an arbitrary combination of data structures. Reads
    ;; and writes to PStates go to disk and are not purely in-memory operations.
    ;;   This PState stores the latest response for each URL, a map from a URL to the body of the HTTP response.
    (declare-pstate s $$responses {String String})
    ;; <<sources defines the ETL logic as Rama dataflow code. Rama's dataflow API works differently than Clojure, but it has
    ;; the same expressiveness as any general purpose language while also being able to seamlessly distribute computation.
    (<<sources s
      ;; This subscribes the ETL to *get-depot. The :> keyword separates the inputs and outputs of the form.
      ;; Because of the depot partitioner on *get-depot, computation starts on the same task where responses are
      ;; stored for that URL in the $$responses PState.
      (source> *get-depot :> *url)
      ;; completable-future> integrates arbitrary asynchronous work within a topology. It ties the success/failure of
      ;; the asynchronous task with the success/failure of the topology. So if the asynchronous work fails or times out,
      ;; the topology will fail as well and the depot record will be retried. completable-future> is a non-blocking operation.
      (completable-future>
        (http-get-future (task-global-client *http-client) *url)
        :> *netty-response)
      (get-body *netty-response :> *body)
      ;; This records the latest response in the PState.
      (local-transform> [(keypath *url) (termval *body)] $$responses)
      )))
