package rama.gallery;

import com.rpl.rama.*;
import com.rpl.rama.test.*;

import rama.gallery.topusers.TopUsersModule;
import rama.gallery.topusers.data.*;

import org.junit.Test;
import java.util.*;

import static org.junit.Assert.*;


public class TopUsersModuleTest {
  @Test
  public void test() throws Exception {
    // InProcessCluster simulates a full Rama cluster in-process and is an ideal environment for experimentation and
    // unit-testing.
    try(InProcessCluster ipc = InProcessCluster.create()) {
      TopUsersModule module = new TopUsersModule();
      // lower the amount of top users to compute to ease testing
      module.topAmount = 3;
      // By default a module's name is the same as its class name.
      String moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(4, 2));

      // Client usage of IPC is identical to using a real cluster. Depot and PState clients are fetched by referencing
      // the module name along with the variable used to identify the depot/PState within the module.
      Depot purchaseDepot = ipc.clusterDepot(moduleName, "*purchaseDepot");
      PState topSpendingUsers = ipc.clusterPState(moduleName, "$$topSpendingUsers");

      // declare some constants to make the test code easier to read
      long aliceId = 0;
      long bobId = 1;
      long charlieId = 2;
      long davidId = 3;
      long emilyId = 4;

      // add some data to the depot
      purchaseDepot.append(new Purchase(aliceId, 300));
      purchaseDepot.append(new Purchase(bobId, 200));
      purchaseDepot.append(new Purchase(charlieId, 100));
      purchaseDepot.append(new Purchase(davidId, 100));
      purchaseDepot.append(new Purchase(emilyId, 400));

      // Microbatching runs asynchronously to depot appends, so this code waits for microbatching to finish
      // processing all the depot appends so we can see those appends reflected in PState queries.
      ipc.waitForMicrobatchProcessedCount(moduleName, "topusers", 5);

      // To fetch the entire top users list, the path "Path.stay()" is used.
      List topUserTuples = topSpendingUsers.selectOne(Path.stay());
      List expected = new ArrayList();
      expected.add(Arrays.asList(emilyId, 400L));
      expected.add(Arrays.asList(aliceId, 300L));
      expected.add(Arrays.asList(bobId, 200L));
      assertEquals(expected, topUserTuples);

      // Add another record which will cause davidId to supplant bobId on the top users list.
      purchaseDepot.append(new Purchase(davidId, 250));
      ipc.waitForMicrobatchProcessedCount(moduleName, "topusers", 6);

      // Verify the new list of top users.
      topUserTuples = topSpendingUsers.selectOne(Path.stay());
      expected = new ArrayList();
      expected.add(Arrays.asList(emilyId, 400L));
      expected.add(Arrays.asList(davidId, 350L));
      expected.add(Arrays.asList(aliceId, 300L));
      assertEquals(expected, topUserTuples);

      // Fetch just the user IDs from the top users list by iterating over each tuple and selecting
      // just the first field. This path is executed completely server side and only the user IDs are
      // returned.
      List topUsers = topSpendingUsers.select(Path.all().first());
      expected = new ArrayList();
      expected.add(emilyId);
      expected.add(davidId);
      expected.add(aliceId);
      assertEquals(expected, topUsers);
    }
  }
}
