(ns rama.gallery.top-users-module-test
  (:use [com.rpl rama]
        [com.rpl.rama path])
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.rpl.rama.test :as rtest]
   [rama.gallery.top-users-module :as tum]))

(deftest top-users-module-test
  ;; A redef of this constant is used to simplify testing this module by reducing the amount of users to store in the
  ;; "top users" PState.
  (with-redefs [tum/TOP-AMOUNT 3]
    ;; create-ipc creates an InProcessCluster which simulates a full Rama cluster in-process and is an ideal environment for
    ;; experimentation and unit-testing.
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc tum/TopUsersModule {:tasks 4 :threads 2})
      (let [module-name (get-module-name tum/TopUsersModule)
            ;; Client usage of IPC is identical to using a real cluster. Depot and PState clients are fetched by
            ;; referencing the module name along with the variable used to identify the depot/PState within the module.
            purchase-depot (foreign-depot ipc module-name "*purchase-depot")
            top-spending-users (foreign-pstate ipc module-name "$$top-spending-users")
            ;; declare some constants to make the test code easier to read
            alice-id 0
            bob-id 1
            charlie-id 2
            david-id 3
            emily-id 4]
        ;; add some data to the depot
        (foreign-append! purchase-depot (tum/->Purchase alice-id 300))
        (foreign-append! purchase-depot (tum/->Purchase bob-id 200))
        (foreign-append! purchase-depot (tum/->Purchase charlie-id 100))
        (foreign-append! purchase-depot (tum/->Purchase david-id 100))
        (foreign-append! purchase-depot (tum/->Purchase emily-id 400))

        ;; Microbatching runs asynchronously to depot appends, so this code waits for microbatching to finish
        ;; processing all the depot appends so we can see those appends reflected in PState queries.
        (rtest/wait-for-microbatch-processed-count ipc module-name "topusers" 5)

        ;; To fetch the entire top users list, the navigator STAY is used.
        (is (= [[emily-id 400]
                [alice-id 300]
                [bob-id 200]]
               (foreign-select-one STAY top-spending-users)))

        ;; Add another record which will cause david-id to supplant bob-id on the top users list.
        (foreign-append! purchase-depot (tum/->Purchase david-id 250))
        (rtest/wait-for-microbatch-processed-count ipc module-name "topusers" 6)

        ;; Verify the new list of top users.
        (is (= [[emily-id 400]
                [david-id 350]
                [alice-id 300]]
               (foreign-select-one STAY top-spending-users)))

        ;; Fetch just the user IDs from the top users list by iterating over each tuple and selecting
        ;; just the first field. This path is executed completely server side and only the user IDs are
        ;; returned.
        (is (= [emily-id
                david-id
                alice-id]
               (foreign-select [ALL FIRST] top-spending-users)))
    ))))
