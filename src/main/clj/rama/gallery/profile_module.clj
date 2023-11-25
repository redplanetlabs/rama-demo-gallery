(ns rama.gallery.profile-module
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops])
  (:import [com.rpl.rama.helpers ModuleUniqueIdPState]))

;; This module demonstrates account registration, generating unique 64-bit user IDs, and editing profiles.
;; The implementation is fault-tolerant, and there are no race conditions.
;;
;; See the test file profile_modules_test.clj for how a client interacts with this module to perform user registrations
;; and profile edits.

;; As with all the demos, data is represented using plain Clojure records. You can represent
;; data however you want, and we generally recommend using a library with compact serialization,
;; strong schemas, and support for evolving types (like Thrift or Protocol Buffers). We use plain
;; Clojure records in these demos to keep them as simple as possible by not having additional
;; dependencies. Rama uses Nippy for serialization, and you can extend that directly or define a custom
;; serialization through Rama to support your own representations. In all cases you always work with
;; first-class objects all the time when using Rama, whether appending to depots, processing in ETLs,
;; or querying from PStates.
(defrecord Registration [uuid username pwd-hash])
(defrecord ProfileEdit [field value])
(defrecord ProfileEdits [user-id edits])

(defn display-name-edit [value] (->ProfileEdit :display-name value))
(defn pwd-hash-edit [value] (->ProfileEdit :pwd-hash value))
(defn height-inches-edit [value] (->ProfileEdit :height-inches value))

;; This defines the module, whose body is a regular Clojure function implementation. All depots, ETLs,
;; PStates, and query topologies are defined via this entry point.
(defmodule ProfileModule
  [setup topologies]
  ;; This depot takes in Registration objects. The second argument is a "depot partitioner" that controls
  ;; how appended data is partitioned across the depot, affecting on which task each piece of data begins
  ;; processing in ETLs.
  (declare-depot setup *registration-depot (hash-by :username))
  ;; This depot takes in ProfileEdits objects.
  (declare-depot setup *profile-edits-depot (hash-by :user-id))

  ;; Stream topologies process appended data within a few milliseconds and guarantee all data will be fully processed.
  ;; Their low latency makes them appropriate for a use case like this.
  (let [s (stream-topology topologies "profiles")
        ;; ModuleUniqueIdPState is a small utility from rama-helpers that abstracts away the pattern of generating
        ;; unique 64-bit IDs. 64-bit IDs are preferable to UUIDs because they take half the space, but since they're
        ;; smaller generating them randomly has too high a chance of not being globally unique. ModuleUniqueIdPState
        ;; uses a PState to track a task-specific counter, and it combines that counter with the task ID to generate IDs
        ;; that are globally unique.
        id-gen (ModuleUniqueIdPState. "$$id")]
    ;;   PStates are durable and replicated datastores and are represented as an arbitrary combination of data structures.
    ;; Reads and writes to PStates go to disk and are not purely in-memory operations.
    ;;   This PState is used to assign a userId to every registered username. It also prevents race conditions in the case
    ;; of multiple concurrent registrations of the same username. Every registration contains a UUID that uniquely identifies
    ;; the registration request. The first registration records its UUID along with the generated 64-bit userId in this PState.
    ;; A registration request is known to be successful if the UUID used for registration is recorded in this PState.
    ;; Further details are described below with the ETL definition.
    (declare-pstate s $$username->registration {String ; username
                                                 (fixed-keys-schema {:user-id Long
                                                                     :uuid String})})
    ;; This PState stores all profile information for each userId.
    (declare-pstate s $$profiles {Long ; user ID
                                   (fixed-keys-schema {:username String
                                                       :pwd-hash String
                                                       :display-name String
                                                       :height-inches Long})})
    ;; This declares the underlying PState needed by ModuleUniqueIdPState.
    (.declarePState id-gen s)

    ;; <<sources defines the ETL logic as Rama dataflow code. Rama's dataflow API works differently than Clojure, but it has
    ;; the same expressiveness as any general purpose language while also being able to seamlessly distribute computation.
    (<<sources s
      ;;   This subscribes the ETL to *registration-depot. The :> keyword separates the inputs and outputs of the form. The output
      ;; here is destructured to capture the fields "uuid", "username", and "pwd-hash" to Rama variables of the same name.
      ;;   Because of the depot partitioner on *registrationDepot, computation starts on the same task where registration info
      ;; is stored for that username in the $$username->registration PState.
      (source> *registration-depot :> {:keys [*uuid *username *pwd-hash]})
      ;;   The first step of registration is to see if this username is already registered. So the current registration info
      ;; is fetched from the $$username->registration PState and bound to the variable *currInfo.
      ;;   A critical property of Rama is that only one event can run on a task at time. So while an ETL event is running,
      ;; no other ETL events, PState queries, or other events can run on the task. In this case, we know that any other
      ;; registration requests for the same username are queued behind this event, and there are no race conditions with
      ;; concurrent registrations because they are run serially on this task for this username.
      (local-select> (keypath *username) $$username->registration :> {*curr-uuid :uuid :as *curr-info})
      ;; There are two cases where this is a valid registration:
      ;;  - *curr-info is null, meaning this is the first time a registration has been seen for this username
      ;;  - The UUID inside *curr-info matches the registration UUID. This indicates the registration request was retried,
      ;;    either by the stream topology due to a downstream failure (e.g. a node dying), or by the client re-appending
      ;;    the same request to the depot due to receiving an error.
      (<<if (or> (nil? *curr-info)
                 (= *curr-uuid *uuid))

        ;;   This block is run when the condition to <<if was true. No block is provided for the false case since
        ;; a registration of an invalid username is a no-op.
        ;;   java-macro! is a way to insert a snippet of code generated from the Java API into Clojure dataflow code.
        ;; ModuleUniqueIDPState defines the method "genId" to insert code to generate a globally unique ID and bind
        ;; it to the specified variable. The generated code increments the counter  on this task by one and computes
        ;; the ID by combining that counter with the task ID.
        (java-macro! (.genId id-gen "*user-id"))
        ;; This records the registration info in the PState.
        (local-transform> [(keypath *username)
                           (multi-path [:user-id (termval *user-id)]
                                       [:uuid (termval *uuid)])]
          $$username->registration)
        ;; The ETL is currently partitioned by username, but now it needs to record information for a user ID. This
        ;; |hash call relocates computation to the task which will be used to store information for this user ID.
        ;; |hash always chooses the same task ID for the same user ID but evenly spreads different user IDs across
        ;; all tasks. The code before and after this call can run on different processes on different machines, and Rama
        ;; takes care of all serialization and network transfer required.
        (|hash *user-id)
        ;; Finally, this code records the username and pwd-hash for the new user ID in the $$profiles PState.
        (local-transform> [(keypath *user-id)
                           (multi-path [:username (termval *username)]
                                       [:pwd-hash (termval *pwd-hash)])]
          $$profiles)
        ;; Stream topologies can return information back to depot append clients with "ack returns". The client
        ;; receives the resulting "ack return" for each subscribed colocated stream topology in a map from
        ;; topology name to value. Here, the ack return is used to let the client know the user ID for their
        ;; newly registered username. If the ack return is nil, then the client knows the username registration
        ;; failed.
        (ack-return> *user-id))

      ;; This subscribes the ETL to *profile-edits-depot, binding all edit objects to the variable "*data". The depot
      ;; partitioner in this case ensures that processing starts on the task where we're storing information for the user ID.
      (source> *profile-edits-depot :> {:keys [*user-id *edits]})
      ;; *edits is a list, and ops/explode emits one time for every element in that list. Each element is destructured to
      ;; the variables *field and *value.
      (ops/explode *edits :> {:keys [*field *value]})
      ;; This writes the new value for each field into the $$profiles PState.
      (local-transform> [(keypath *user-id *field) (termval *value)] $$profiles)
      )))
