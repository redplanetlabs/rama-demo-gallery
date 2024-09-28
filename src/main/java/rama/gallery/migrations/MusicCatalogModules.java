package rama.gallery.migrations;

import com.rpl.rama.*;
import com.rpl.rama.helpers.TopologyUtils.ExtractJavaField;
import com.rpl.rama.module.MicrobatchTopology;
import rama.gallery.migrations.data.Song;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

/*
 * This toy module demonstrate Rama's PState migration functionality.
 *
 * Two module instances are used in order to demonstrate the evolution of a
 * PState schema. In a real application codebase there would be one module
 * instance defined for a module, and its preceding historical instances would
 * live in version control.
 *
 * This module indexes musical albums as they're added to a catalog.
 *
 * See the test class MusticModuleCatalogsTest for how a migration is initiated with a module update.
 *
 * You can see the data types used by the module in the package rama.gallery.migrations.data.
 *
 * As with all the demos, data is represented as plain Java objects with public fields. You can represent
 * data however you want, and we generally recommend using a library with compact serialization and support
 * for evolving types (like Thrift or Protocol Buffers). We use plain Java objects in these demos to keep
 * them as simple as possible by not having additional dependencies. Using other representations is easy to do
 * by defining a custom serialization, and in all cases you always work with first-class objects all the time
 * when using Rama, whether appending to depots, processing in ETLs, or querying from PStates.
 */
public class MusicCatalogModules {

    // The module instances represent the same module as evolved over time, so they share a name.
    public static String MODULE_NAME = "MusicCatalogModule";

    // This class is used below to define a depot partitioner. ExtractJavaField is a helper utility from
    // the open-source rama-helpers project for extracting a public field from an object.
    public static class ExtractArtist extends ExtractJavaField {
        public ExtractArtist() { super("artist"); }
    }

    // This is the first module instance.
    public static class ModuleInstanceA implements RamaModule {

        // We explicitly override the method which returns a module's name; otherwise, the default is to return the
        // class name, which would make it difficult for us to define two module instances at once in the same codebase.
        @Override
        public String getModuleName() { return MODULE_NAME; }

        @Override
        public void define(Setup setup, Topologies topologies) {
            // This depot takes in Album objects. Each Album lives on a partition according to its artist field.
            setup.declareDepot("*albumsDepot", Depot.hashBy(ExtractArtist.class));

            // Declare a topology for processing Albums as they're appended. A microbatch topology provides exactly-once
            // processing guarantees at the cost of higher latency; in the context of a hypothetical music catalog
            // service, higher latency would be tolerable as albums would presumably be indexed in advance of release.
            MicrobatchTopology mb = topologies.microbatch("albums");

            // Declare a pstate called $$albums.
            mb.pstate("$$albums",
                    // The top level of our $$albums schema is a map keyed by artist. All of an artist's music will live
                    // on one partition.
                    PState.mapSchema(
                            String.class, // artist
                            // The values of our top-level map are subindexed maps keyed by album name. Subindexing this
                            // map allows us to store any number of albums for a given artist, and to paginate them in
                            // alphabetical sort order at query time.
                            PState.mapSchema(
                                            String.class, // album name
                                            // The values of our inner map are albums, each with a name and songs.
                                            PState.fixedKeysSchema(
                                                    "name", String.class,
                                                    "songs", PState.listSchema(String.class)))
                                    .subindexed()));

            // Subscribe our microbatch ETL topology to the albums depot. Each time a microbatch runs, the "*microbatch"
            // var will be emitted, representing all Album records appended since the last microbatch. For each Album,
            // we'll run some topology code to process the Album and add it to the $$albums index.
            mb.source("*albumsDepot").out("*microbatch")
                    // From each microbatch, emit each Album individually.
                    .explodeMicrobatch("*microbatch").out("*album")
                    // A helper from the rama-helpers library extracts fields from an object and binds them to vars.
                    .macro(extractJavaFields("*album", "*artist", "*name", "*songs"))
                    // Finally we update our index
                    .localTransform("$$albums", Path.key("*artist").key("*name")
                            .multiPath(
                                    Path.key("name").termVal("*name"),
                                    Path.key("songs").termVal("*songs")));
        }
    }

    // This is the second of our two module instances. Only the differences from the previous module instance are called
    // out in comments.
    public static class ModuleInstanceB implements RamaModule {

        @Override
        public String getModuleName() { return MODULE_NAME; }

        @Override
        public void define(Setup setup, Topologies topologies) {
            setup.declareDepot("*albumsDepot", Depot.hashBy(ExtractArtist.class));

            MicrobatchTopology mb = topologies.microbatch("albums");

            mb.pstate("$$albums",
                    PState.mapSchema(
                            String.class, // artist
                            PState.mapSchema(
                                            String.class, // name
                                            // Here is where we demonstrate Rama's migration functionality. We wrap the
                                            // sub-schema we want to modify with the `migrated` method.
                                            PState.migrated(
                                                    // The first argument is the new schema. We are changing the songs
                                                    // field from a List<String> to a List<Song>. Note that we cannot
                                                    // directly migrate the below listSchema because it is not indexed
                                                    // as a whole - it is one component of the entire in-memory album.
                                                    PState.fixedKeysSchema(
                                                            "name", String.class,
                                                            "songs", PState.listSchema(Song.class)),
                                                    // The second argument is the migration's ID. Rama will use this to
                                                    // determine whether or not a subsequent migration is a new one (and
                                                    // so requires restarting).
                                                    "parse-song-data",
                                                    // Finally we provide our migration function (defined below). This
                                                    // function may be run on both yet-to-be-migrated and already-
                                                    // migrated values, so it must be idempotent.
                                                    ModuleInstanceB::migrateSongs))
                                    .subindexed()));

            mb.source("*albumsDepot").out("*microbatch")
                    .explodeMicrobatch("*microbatch").out("*album")
                    .macro(extractJavaFields("*album", "*artist", "*name", "*songs"))
                    // Our topology code is the same except that we parse Songs from our list of String song, and shadow
                    // the *songs var with the output.
                    .each(ModuleInstanceB::parseSongs, "*songs").out("*songs")
                    .localTransform("$$albums", Path.key("*artist").key("*name")
                            .multiPath(
                                    Path.key("name").termVal("*name"),
                                    Path.key("songs").termVal("*songs")));
        }

        // This is our migration function. If the input object is already of the new schema, i.e., its songs are Songs
        // and not Strings, it returns it unchanged; otherwise it converts each String song to a proper Song.
        private static Object migrateSongs(Object o) {
            Map m = (Map)o;
            List songs = (List) m.get("songs");
            if (songs.get(0) != null && songs.get(0) instanceof String) {
                Map m1 = new HashMap();
                m1.putAll(m);
                m1.put("songs", parseSongs(songs));
                return m1;
            }
            return o;
        }

        // This is a helper for parsing Songs from Strings.
        private static List<Song> parseSongs(List<String> songs) {
            return songs.stream().map(ModuleInstanceB::parseSong).collect(Collectors.toList());
        }

        // Here is a naive method for parsing a song's name and possibly its featured artists from a String.
        private static Song parseSong(String songStr) {
            String[] nameAndFeatures = songStr.split("\\s*(ft|feat)\\.*");
            String name = nameAndFeatures[0].trim();
            String features = nameAndFeatures.length > 1 ? nameAndFeatures[1] : "";
            String[] featuredArtistsArr = features.split(",");
            List<String> featuredArtists = Arrays.stream(featuredArtistsArr)
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
            return new Song(name, featuredArtists);
        }
    }
}
