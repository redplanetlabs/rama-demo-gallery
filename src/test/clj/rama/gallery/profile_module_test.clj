(ns rama.gallery.profile-module-test
  (:use [com.rpl rama]
        [com.rpl.rama path])
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.rpl.rama.test :as rtest]
   [rama.gallery.profile-module :as pm]))

;; This function implements username registration, throwing an exception if the username is already registered.
;; This uses the ack return of the "profiles" topology to know if the registration request succeeded or not.
(defn register! [registration-depot username->registration username pwd-hash]
  (let [{user-id "profiles"} (foreign-append! registration-depot
                                              (pm/->Registration (str (java.util.UUID/randomUUID))
                                              username
                                              pwd-hash))]
    (if (some? user-id)
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
