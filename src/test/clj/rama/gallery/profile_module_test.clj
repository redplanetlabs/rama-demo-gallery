(ns rama.gallery.profile-module-test
  (:use [com.rpl rama]
        [com.rpl.rama path])
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.rpl.rama.test :as rtest]
   [rama.gallery.profile-module :as pm]))

;; This function implements username registration, throwing an exception if the username is already registered. It uses
;; the registration UUID to determine if the registration was a success.
(defn register! [registration-depot username->registration username pwd-hash]
  (let [reg-uuid (str (java.util.UUID/randomUUID))
        ;; This depot append blocks until all colocated stream topologies have finished processing the data.
        _ (foreign-append! registration-depot (pm/->Registration reg-uuid username pwd-hash))
        ;; At this point, we're guaranteed the registration has been fully processed. Success/failure can then be determined
        ;; by whether the ETL recorded this UUID in the $$username->registration PState.
        {:keys [uuid user-id]} (foreign-select-one (keypath username) username->registration)]
    (if (= reg-uuid uuid)
      user-id
      (throw (ex-info "Username already registered" {})))))

(deftest profile-module-test
  ;; create-ipc creates an InProcessCluster which simulates a full Rama cluster in-process and is an ideal environment for
  ;; experimentation and unit-testing.
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc pm/ProfileModule {:tasks 4 :threads 2})
    (let [module-name (get-module-name pm/ProfileModule)
          ;; Client usage of IPC is identical to using a real cluster. Depot and PState clients are fetched by
          ;; referencing the module name along with the variable used to identify the depot/PState within the module.
          registration-depot (foreign-depot ipc module-name "*registration-depot")
          profile-edits-depot (foreign-depot ipc module-name "*profile-edits-depot")
          username->registration (foreign-pstate ipc module-name "$$username->registration")
          profiles (foreign-pstate ipc module-name "$$profiles")
          alice-id (register! registration-depot username->registration "alice" "hash1")
          bob-id (register! registration-depot username->registration "bob" "hash2")]
      ;; verify registering alice again fails
      (is (thrown? Exception (register! registration-depot username->registration "alice", "hash3")))
      ;; verify that profiles are initialized correctly
      (is (= "alice" (foreign-select-one (keypath alice-id :username) profiles)))
      (is (= "bob" (foreign-select-one (keypath bob-id :username) profiles)))

      ;; Do many profile edits at once and verify they all go through.
      (foreign-append! profile-edits-depot
                       (pm/->ProfileEdits alice-id
                                          [(pm/display-name-edit "Alice Smith")
                                           (pm/height-inches-edit 65)
                                           (pm/pwd-hash-edit "hash4")]))
      (is (= {:username "alice"
              :display-name "Alice Smith"
              :height-inches 65
              :pwd-hash "hash4"}
             (foreign-select-one (keypath alice-id) profiles)))

      ;; Verify that profile editing only replaces specified fields
      (foreign-append! profile-edits-depot
                       (pm/->ProfileEdits alice-id
                                          [(pm/display-name-edit "Alicia Smith")]))
      (is (= {:username "alice"
              :display-name "Alicia Smith"
              :height-inches 65
              :pwd-hash "hash4"}
             (foreign-select-one (keypath alice-id) profiles)))

      ;; Do a single profile edit on a different user.
      (foreign-append! profile-edits-depot
                       (pm/->ProfileEdits bob-id
                                          [(pm/display-name-edit "Bobby")]))
      (is (= {:username "bob"
              :display-name "Bobby"
              :pwd-hash "hash2"}
             (foreign-select-one (keypath bob-id) profiles)))
      )))
