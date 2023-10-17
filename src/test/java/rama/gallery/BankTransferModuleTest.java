package rama.gallery;

import org.junit.Test;

import com.rpl.rama.*;
import com.rpl.rama.test.*;

import rama.gallery.banktransfer.BankTransferModule;
import rama.gallery.banktransfer.data.*;

import java.util.*;

import static org.junit.Assert.*;

public class BankTransferModuleTest {
  private Map toMap(Object... kvs) {
    Map ret = new HashMap();
    for(int i=0; i<kvs.length; i+=2) {
      ret.put(kvs[i], kvs[i+1]);
    }
    return ret;
  }

  @Test
  public void test() throws Exception {
    // InProcessCluster simulates a full Rama cluster in-process and is an ideal environment for experimentation and
    // unit-testing.
    try(InProcessCluster ipc = InProcessCluster.create()) {
      BankTransferModule module = new BankTransferModule();
      // By default a module's name is the same as its class name.
      String moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(4, 2));

      // Client usage of IPC is identical to using a real cluster. Depot, PState, and query topology clients are fetched
      // by referencing the module name along with the variable used to identify the depot/PState/query within the module.
      Depot transferDepot = ipc.clusterDepot(moduleName, "*transferDepot");
      Depot depositDepot = ipc.clusterDepot(moduleName, "*depositDepot");
      PState funds = ipc.clusterPState(moduleName, "$$funds");
      PState outgoingTransfers = ipc.clusterPState(moduleName, "$$outgoingTransfers");
      PState incomingTransfers = ipc.clusterPState(moduleName, "$$incomingTransfers");

      // Declare some constants to make the test code easier to read
      long aliceId = 0;
      long bobId = 1;
      long charlieId = 2;

      depositDepot.append(new Deposit(aliceId, 200));
      depositDepot.append(new Deposit(bobId, 100));
      depositDepot.append(new Deposit(charlieId, 100));

      // Microbatching runs asynchronously to depot appends, so this code waits for microbatching to finish
      // processing all the depot appends so we can see those appends reflected in PState queries.
      ipc.waitForMicrobatchProcessedCount(moduleName, "banking", 3);

      // This transfer will succeed.
      transferDepot.append(new Transfer("alice->bob1", aliceId, bobId, 50));
      // This transfer will fail because alice has only 150 funds after the first transfer.
      transferDepot.append(new Transfer("alice->charlie1", aliceId, charlieId, 160));
      // This transfer will succeed.
      transferDepot.append(new Transfer("alice->charlie2", aliceId, charlieId, 25));
      // This transfer will succeed.
      transferDepot.append(new Transfer("charlie->bob1", charlieId, bobId, 10));

      ipc.waitForMicrobatchProcessedCount(moduleName, "banking", 7);

      // Assert on the final funds for each user
      assertEquals(125, (int) funds.selectOne(Path.key(aliceId)));
      assertEquals(160, (int) funds.selectOne(Path.key(bobId)));
      assertEquals(115, (int) funds.selectOne(Path.key(charlieId)));

      //Verify the outgoing transfers of alice
      List transfers = outgoingTransfers.select(Path.key(aliceId).all());
      assertEquals(3, transfers.size());
      Set expected = new HashSet();
      expected.add(Arrays.asList("alice->bob1", toMap("toUserId", bobId, "amt", 50, "isSuccess", true)));
      expected.add(Arrays.asList("alice->charlie1", toMap("toUserId", charlieId, "amt", 160, "isSuccess", false)));
      expected.add(Arrays.asList("alice->charlie2", toMap("toUserId", charlieId, "amt", 25, "isSuccess", true)));
      assertEquals(expected, new HashSet(transfers));

      //Verify the outgoing transfers of charlie
      transfers = outgoingTransfers.select(Path.key(charlieId).all());
      assertEquals(1, transfers.size());
      expected = new HashSet();
      expected.add(Arrays.asList("charlie->bob1", toMap("toUserId", bobId, "amt", 10, "isSuccess", true)));
      assertEquals(expected, new HashSet(transfers));

      //Verify the incoming transfers of bob
      transfers = incomingTransfers.select(Path.key(bobId).all());
      assertEquals(2, transfers.size());
      expected = new HashSet();
      expected.add(Arrays.asList("alice->bob1", toMap("fromUserId", aliceId, "amt", 50, "isSuccess", true)));
      expected.add(Arrays.asList("charlie->bob1", toMap("fromUserId", charlieId, "amt", 10, "isSuccess", true)));
      assertEquals(expected, new HashSet(transfers));

      //Verify the incoming transfers of charlie
      transfers = incomingTransfers.select(Path.key(charlieId).all());
      assertEquals(2, transfers.size());
      expected = new HashSet();
      expected.add(Arrays.asList("alice->charlie1", toMap("fromUserId", aliceId, "amt", 160, "isSuccess", false)));
      expected.add(Arrays.asList("alice->charlie2", toMap("fromUserId", aliceId, "amt", 25, "isSuccess", true)));
      assertEquals(expected, new HashSet(transfers));
    }
  }

}
