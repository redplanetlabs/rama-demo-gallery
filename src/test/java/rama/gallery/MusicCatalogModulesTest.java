package rama.gallery;

import com.rpl.rama.ops.Ops;
import org.junit.Test;

import com.rpl.rama.*;
import com.rpl.rama.test.*;
import rama.gallery.migrations.MusicCatalogModules;
import rama.gallery.migrations.data.Album;
import rama.gallery.migrations.data.Song;

import java.util.*;

import static org.junit.Assert.*;

public class MusicCatalogModulesTest {

    @Test
    public void test() throws Exception {
        // InProcessCluster simulates a full Rama cluster in-process and is an ideal environment for experimentation and
        // unit-testing.
        try(InProcessCluster ipc = InProcessCluster.create()) {
            // First we construct our initial module instance and deploy it.
            MusicCatalogModules.ModuleInstanceA moduleInstanceA = new MusicCatalogModules.ModuleInstanceA();
            ipc.launchModule(moduleInstanceA, new LaunchConfig(4, 2));

            // We'll use our module's name to fetch handles on its depots and PStates.
            String moduleName = moduleInstanceA.getModuleName();

            // Client usage of IPC is identical to using a real cluster. Depot, PState, and query topology clients are fetched
            // by referencing the module name along with the variable used to identify the depot/PState/query within the module.
            Depot albumsDepot = ipc.clusterDepot(moduleName, "*albumsDepot");
            PState albums = ipc.clusterPState(moduleName, "$$albums");

            // Now we construct an Album and append it to the depot.
            Album f1Trillion = new Album(
                    "Post Malone",
                    "F-1 Trillion",
                    Collections.singletonList("Have The Heart ft. Dolly Parton"));
            albumsDepot.append(f1Trillion);
            // We wait for our microbatch to process the album.
            ipc.waitForMicrobatchProcessedCount(moduleName, "albums", 1);

            // Once processed, we expect that our album will be indexed in the PState.
            assertEquals((Object)1, albums.selectOne(Path.key("Post Malone").view(Ops.SIZE)));
            assertEquals("F-1 Trillion", albums.selectOne(Path.key("Post Malone", "F-1 Trillion", "name")));
            assertEquals("Have The Heart ft. Dolly Parton",
                    albums.selectOne(Path.key("Post Malone", "F-1 Trillion", "songs")
                            .first()));

            // Now we construct the next iteration of our module and deploy it via module update.
            MusicCatalogModules.ModuleInstanceB moduleInstanceB = new MusicCatalogModules.ModuleInstanceB();
            ipc.updateModule(moduleInstanceB);

            // We expect that albums previously appended will now have the new Song structure
            assertEquals("Have The Heart",
                    albums.selectOne(Path.key("Post Malone", "F-1 Trillion", "songs")
                            .first().view((Song s) -> s.name)));
            assertEquals("Dolly Parton",
                    albums.selectOne(Path.key("Post Malone", "F-1 Trillion", "songs")
                            .first().view((Song s) -> s.featuredArtists).first()));

            // Albums newly appended will go through the new topology code, and have their Songs parsed before indexing.
            Album channelOrange = new Album(
                    "Frank Ocean",
                    "Channel Orange",
                    Collections.singletonList("White feat. John Mayer"));
            albumsDepot.append(channelOrange);
            // We wait for our microbatch to process the album.
            ipc.waitForMicrobatchProcessedCount(moduleName, "albums", 2);
            assertEquals("White",
                    albums.selectOne(Path.key("Frank Ocean", "Channel Orange", "songs")
                            .first().view((Song s) -> s.name)));
            assertEquals("John Mayer",
                    albums.selectOne(Path.key("Frank Ocean", "Channel Orange", "songs")
                            .first().view((Song s) -> s.featuredArtists).first()));
      }
    }
}
