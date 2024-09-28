(ns rama.gallery.migrations-music-catalog-modules
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require
   [clojure.string :as str]))

;; This toy module demonstrate Rama's PState migration functionality.
;;
;; Two module instances are used in order to demonstrate the evolution of a
;; PState schema. In a real application codebase there would be one module
;; instance defined for a module, and its preceding historical instances would
;; live in version control.
;;
;; This module indexes musical albums as they're added to a catalog.
;;
;; See the test file migrations_music_module.clj for how a migration is
;; initiated with a module update.

;; As with all the demos, data is represented using plain Clojure records. You can represent
;; data however you want, and we generally recommend using a library with compact serialization,
;; strong schemas, and support for evolving types (like Thrift or Protocol Buffers). We use plain
;; Clojure records in these demos to keep them as simple as possible by not having additional
;; dependencies. Rama uses Nippy for serialization, and you can extend that directly or define a custom
;; serialization through Rama to support your own representations. In all cases you always work with
;; first-class objects all the time when using Rama, whether appending to depots, processing in ETLs,
;; or querying from PStates.

(defrecord Album [artist name songs])
(defrecord Song [name featured-artists])

;; The module instances represent the same module as evolved over time, so they
;; share a name.
(def module-name "MusicCatalogModule")

;; This is the first module instance.
(defmodule ModuleInstanceA
  ;; We explicitly set the option for a module's name; otherwise, the default is
  ;; to return the class name, which would make it difficult for us to define
  ;; two module instances at once in the same codebase.
  {:module-name module-name}
  [setup topologies]
  ;; This depot takes in Album objects. Each Album lives on a partition
  ;; according to its artist field.
  (declare-depot setup *albums-depot (hash-by :artist))
  ;; Declare a topology for processing Albums as they're appended. A
  ;; microbatch topology provides exactly-once processing guarantees
  ;; at the cost of higher latency; in the context of a hypothetical
  ;; music catalog service, higher latency would be tolerable as albums
  ;; would presumably be indexed in advance of release.
  (let [mb (microbatch-topology topologies "albums")]
    ;; Declare a PState called $$albums.
    (declare-pstate
     mb
     $$albums
     ;; The top level of our $$albums schema is a map keyed by artist. All of an
     ;; artist's music will live on one partition.
     (map-schema
      String  ; artist
      ;; The values of our top-level map are subindexed maps keyed by
      ;; album name. Subindexing this map allows us to store any
      ;; number of albums for a given artist, and to paginate them in
      ;; alphabetical sort order at query time.
      (map-schema
       String  ; album name
       ;; The values of our inner map are albums, each with a name
       ;; and songs.
       (fixed-keys-schema
        {:name     String
         :songs    (vector-schema String)})
       {:subindex? true})))
    ;; Begin defining our microbatch topology.
    (<<sources mb
      ;; Subscribe our microbatch ETL topology to the albums depot. Each time
      ;; a microbatch runs, the "*microbatch" var will be emitted, representing
      ;; all Album records appended since the last microbatch.
     (source> *albums-depot :> %microbatch)
      ;; For each microbatch, emit each album individually, and destructure it
      ;; into its fields.
      (%microbatch :> {:keys [*artist *name *songs]})
      ;; Construct our album map to be indexed.
      (hash-map :name *name :songs *songs :> *album)
      ;; Add the album to the index.
      (local-transform> [(keypath *artist *name) (termval *album)]
                        $$albums))))

;; Now we begin defining our second module instance. We'll start with some
;; functions that will enable our PState migration

;; Here is a naive function for parsing a song's name and possibly its featured
;; artists from a String. We'll use it in our migration function.
(defn- parse-song
  [song-str]
  (let [[name features] (str/split song-str #"\s*(ft|feat)\.*")
        features        (->> (str/split (or features "") #",")
                             (mapv str/trim)
                             (filterv (complement empty?)))]
    (->Song name features)))

;; This is our migration function. If the input object is already of the new
;; schema, i.e., its songs are Songs and not Strings, it returns it unchanged;
;; otherwise it converts each String song to a proper Song.
(defn- migrate-songs
  [album]
  (if (some-> album
              :songs
              first
              string?)
    (update album :songs (partial mapv parse-song))
    album))

;; This is the second of our two module instances. Only the differences from
;; the previous module instance are called out in comments.
(defmodule ModuleInstanceB
  {:module-name module-name}
  [setup topologies]
  (declare-depot setup *albums-depot (hash-by :artist))
  (let [mb (microbatch-topology topologies "albums")]
    (declare-pstate
     mb
     $$albums
     (map-schema
      String  ; artist
      (map-schema
       String  ; album name
       ;; Here is where we demonstrate Rama's migration functionality. We
       ;; wrap the sub-schema we want to modify with the `migrated` function.
       (migrated
        ;; The first argument is the new schema. We are changing the songs
        ;; field from a List<String> to a List<Song>. Note that we cannot
        ;; directly migrate the below listSchema because it is not indexed
        ;; as a whole - it is one component of the entire in-memory album.
        (fixed-keys-schema
         {:name     String
          :songs    (vector-schema Song)})
        ;; The second argument is the migration's ID. Rama will use this to
        ;; determine whether or not a subsequent migration is a new one (and
        ;; so requires restarting).
        "parse-song-data"
        ;; Finally we provide our migration function (defined above). This
        ;; function may be run on both yet-to-be-migrated and already-
        ;; migrated values, so it must be idempotent.
        migrate-songs)
       {:subindex? true})))
    (<<sources mb
     (source> *albums-depot :> %microbatch)
      (%microbatch :> {:keys [*artist *name *songs]})
      ;; Our topology code is the same except that we parse Songs from our list
      ;; of String song, and shadow the *songs var with the output.
      (mapv parse-song *songs :> *songs)
      (hash-map :name *name :songs *songs :> *album)
      (local-transform> [(keypath *artist *name) (termval *album)]
                        $$albums))))
