(ns rama.gallery.migrations-music-catalog-modules-test
  (:use [com.rpl rama]
        [com.rpl.rama path])
  (:require
   [clojure.test :refer [deftest is]]
   [com.rpl.rama.test :as rtest]
   [rama.gallery.migrations-music-catalog-modules :as mmcm]))

(deftest migrations-music-catalog-modules-test
  ;; InProcessCluster simulates a full Rama cluster in-process and is an ideal
  ;; environment for experimentation and unit-testing.
  (with-open [ipc (rtest/create-ipc)]
    ;; First we deploy our initial module instance.
    (rtest/launch-module! ipc mmcm/ModuleInstanceA {:tasks 4 :threads 2})
    (let [;; We use our module's name to fetch handles on its depots/PStates.
          module-name  (get-module-name mmcm/ModuleInstanceA)
          ;; Client usage of IPC is identical to using a real cluster. Depot,
          ;; PState, and query topology clients are fetched by referencing the
          ;; module name along with the variable used to identify the
          ;; depot/PState/query within the module.
          albums-depot (foreign-depot ipc module-name "*albums-depot")
          albums       (foreign-pstate ipc module-name "$$albums")]
      ;; Now we construct an Album and append it to the depot.
      (foreign-append! albums-depot
                       (mmcm/->Album "Post Malone"
                                     "F-1 Trillion"
                                     ["Have The Heart ft. Dolly Parton"]))
      ;; We wait for our microbatch to process the album.
      (rtest/wait-for-microbatch-processed-count ipc module-name "albums" 1)
      ;; Once processed, we expect that our album will be indexed in the PState.
      (is (= 1
             (foreign-select-one [(keypath "Post Malone") (view count)]
                                 albums)))
      (is (= "F-1 Trillion"
             (foreign-select-one [(keypath "Post Malone" "F-1 Trillion" :name)]
                                 albums)))
      (is (= "Have The Heart ft. Dolly Parton"
             (foreign-select-one [(keypath "Post Malone" "F-1 Trillion" :songs)
                                  FIRST]
                                 albums)))

      ;; Now we deploy the next iteration of our module, which has a migration,
      ;; via module update.
      (rtest/update-module! ipc mmcm/ModuleInstanceB)

      ;; We expect that albums previously appended will now have the new Song
      ;; structure.
      (is (= "Have The Heart"
             (foreign-select-one [(keypath "Post Malone" "F-1 Trillion" :songs)
                                  FIRST (keypath :name)]
                                 albums)))
      (is (= ["Dolly Parton"]
             (foreign-select-one [(keypath "Post Malone" "F-1 Trillion" :songs)
                                  FIRST (keypath :featured-artists)]
                                 albums)))

      ;; Albums newly appended will go through the new topology code, and have
      ;; their Songs parsed before indexing.
      (foreign-append! albums-depot
                       (mmcm/->Album "Frank Ocean"
                                     "Channel Orange"
                                     ["White feat. John Mayer"]))

      ;; We wait for our microbatch to process the album.
      (rtest/wait-for-microbatch-processed-count ipc module-name "albums" 2)
      (is
       (= "White"
          (foreign-select-one [(keypath "Frank Ocean" "Channel Orange" :songs)
                               FIRST (keypath :name)]
                              albums)))
      (is
       (= ["John Mayer"]
          (foreign-select-one [(keypath "Frank Ocean" "Channel Orange" :songs)
                               FIRST (keypath :featured-artists)]
                              albums))))))
