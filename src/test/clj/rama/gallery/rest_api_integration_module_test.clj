(ns rama.gallery.rest-api-integration-module-test
  (:use [com.rpl rama]
        [com.rpl.rama path])
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.rpl.rama.test :as rtest]
   [rama.gallery.rest-api-integration-module :as raim]))

(deftest rest-api-integration-module-test
  ;; create-ipc creates an InProcessCluster which simulates a full Rama cluster in-process and is an ideal environment for
  ;; experimentation and unit-testing.
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc raim/RestAPIIntegrationModule {:tasks 4 :threads 2})
    (let [module-name (get-module-name raim/RestAPIIntegrationModule)
          ;; Client usage of IPC is identical to using a real cluster. Depot and PState clients are fetched by
          ;; referencing the module name along with the variable used to identify the depot/PState within the module.
          get-depot (foreign-depot ipc module-name "*get-depot")
          responses (foreign-pstate ipc module-name "$$responses")

          url1 "https://official-joke-api.appspot.com/random_joke"
          url2 "http://jservice.io/api/random"]

      ;; This checks the behavior of the module by appending a few URLs and printing the responses recorded in the
      ;; PState. To write a real test with actual assertions, it's best to test the behavior of the module with
      ;; the external REST API calls mocked out, such as by using with-redefs.
      (foreign-append! get-depot url1)
      (println "Response 1:" (foreign-select-one (keypath url1) responses))
      (foreign-append! get-depot url2)
      (println "Response 2:" (foreign-select-one (keypath url2) responses))
      )))
