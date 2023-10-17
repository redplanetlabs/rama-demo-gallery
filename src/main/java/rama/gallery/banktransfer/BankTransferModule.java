package rama.gallery.banktransfer;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

import com.rpl.rama.*;
import com.rpl.rama.helpers.TopologyUtils.ExtractJavaField;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;

/*
 * This module demonstrates transferring funds from one account to another. The implementation guarantees:
 *   - All transfers are processed exactly once.
 *   - A transfer only goes through if there are sufficient funds available.
 *   - No race conditions with concurrent transfers.
 *
 * See the test class BankTransferModuleTest for how a client interacts with this module to perform deposits
 * and transfers
 *
 * You can see the data types used by the module in the package rama.gallery.banktransfer.data.
 *
 * As with all the demos, data is represented as plain Java objects with public fields. You can represent
 * data however you want, and we generally recommend using a library with compact serialization and support
 * for evolving types (like Thrift or Protocol Buffers). We use plain Java objects in these demos to keep
 * them as simple as possible by not having additional dependencies. Using other representations is easy to do
 * by defining a custom serialization, and in all cases you always work with first-class objects all the time
 * when using Rama, whether appending to depots, processing in ETLs, or querying from PStates.
 */
public class BankTransferModule implements RamaModule {
  // These classes are used below to define depot partitioners. ExtractJavaField is a helper utility from
  // the open-source rama-helpers project for extracting a public field from an object.
  public static class ExtractUserId extends ExtractJavaField {
    public ExtractUserId() { super("userId"); }
  }

  public static class ExtractFromUserId extends ExtractJavaField {
    public ExtractFromUserId() { super("fromUserId"); }
  }

  // This method is the entry point to all modules. It defines all depots, ETLs, PStates, and query topologies.
  @Override
  public void define(Setup setup, Topologies topologies) {
    // This depot takes in Transfer objects. The second argument is a "depot partitioner" that controls
    // how appended data is partitioned across the depot, affecting on which task each piece of data begins
    // processing in ETLs.
    setup.declareDepot("*transferDepot", Depot.hashBy(ExtractFromUserId.class));
    // This depot takes in Deposit objects.
    setup.declareDepot("*depositDepot", Depot.hashBy(ExtractUserId.class));
    //   Defines the ETL as a microbatch topology. Microbatch topologies have exactly-once processing semantics, meaning
    // that even if there are failures and the work needs to be retried, the updates into PStates will be as if there
    // were no failures and every depot record was processed exactly once. The exactly-once semantics are critical
    // for this use case.
    //   Microbatch topologies also have higher throughput than stream topologies with the tradeoff of update latency
    // being in the hundreds of milliseconds range rather than single-digit milliseconds range. This is a suitable latency
    // for this task.
    MicrobatchTopology mb = topologies.microbatch("banking");
    //   PStates are durable and replicated datastores and are represented as an arbitrary combination of data structures. Reads
    // and writes to PStates go to disk and are not purely in-memory operations.
    //   This PState stores the total funds for each user, a map from user ID to funds.
    mb.pstate("$$funds", PState.mapSchema(Long.class, Integer.class));
    // These two PStates store outgoing and incoming transfer information. The inner map in the data structure
    // is subindexed because it can contain an unbounded number of elements. Subindexing stores each value of
    // those maps individually and enables them to be written and queried efficiently even when they're huge.
    // Subindexed maps are always sorted, and it's also easy to do range queries on them.
    mb.pstate("$$outgoingTransfers",
              PState.mapSchema(Long.class,
                               PState.mapSchema(String.class,
                                                PState.fixedKeysSchema("toUserId", Long.class,
                                                                       "amt", Integer.class,
                                                                       "isSuccess", Boolean.class)).subindexed()));
    mb.pstate("$$incomingTransfers",
              PState.mapSchema(Long.class,
                               PState.mapSchema(String.class,
                                                PState.fixedKeysSchema("fromUserId", Long.class,
                                                                       "amt", Integer.class,
                                                                       "isSuccess", Boolean.class)).subindexed()));

   // This subscribes the ETL to "*transferDepot", binding the batch of all data in this microbatch to "*microbatch".
   // "*microbatch" represents a batch of data across all partitions of the depot.
   mb.source("*transferDepot").out("*microbatch")
     // "explodeMicrobatch" emits all individual pieces of data across all partitions of the microbatch and binds each one
     // to the variable "*data". Because of the depot partitioner on "*tranferDepot", computation for each piece of data
     // starts on the same task where information is stored for "fromUserId" in the PStates.
     .explodeMicrobatch("*microbatch").out("*data")
     // "extractJavaFields" is a small utility from rama-helpers for extracting public fields from Java objects
     // and binding them to dataflow variables. Here the public fields "transferId", "fromUserId", "toUserId" and
     // "amt" are extracted from the object in "*data" and bound to corresponding variables.
     .macro(extractJavaFields("*data", "*transferId", "*fromUserId", "*toUserId", "*amt"))
     // First check if the user has sufficient funds for the transfer and bind that to the boolean variable
     // "*isSuccess". Note that because task threads execute events serially, there are no race conditions here
     // with concurrent transfers since other transfer requests will be queued behind this event on this task.
     .localSelect("$$funds", Path.key("*fromUserId").nullToVal(0)).out("*funds")
     .each(Ops.GREATER_THAN_OR_EQUAL, "*funds", "*amt").out("*isSuccess")
     // If this transfer is valid, then deduct the funds for fromUserId from the "$$funds" PState.
     .ifTrue("*isSuccess",
       Block.localTransform("$$funds", Path.key("*fromUserId").term(Ops.MINUS, "*amt")))
     // Record the transfer in the "$$outgoingTransfers" PState for fromUserId.
     .localTransform("$$outgoingTransfers",
                     Path.key("*fromUserId", "*transferId")
                         .multiPath(Path.key("toUserId").termVal("*toUserId"),
                                    Path.key("amt").termVal("*amt"),
                                    Path.key("isSuccess").termVal("*isSuccess")))
     // This switches to the task storing information for toUserId, which may be on a different machine.
     .hashPartition("*toUserId")
     // If this transfer is valid, then credit the funds to toUserId in the "$$funds" PState. Note that microbatching
     // has exactly-once semantics across the whole microbatch, which provides the cross-partition transactionality
     // needed for this use case.
     .ifTrue("*isSuccess",
       Block.localTransform("$$funds", Path.key("*toUserId").nullToVal(0).term(Ops.PLUS, "*amt")))
     // Record the transfer in the $$incomingTransfers PState for toUserId.
     .localTransform("$$incomingTransfers",
                     Path.key("*toUserId", "*transferId")
                         .multiPath(Path.key("fromUserId").termVal("*fromUserId"),
                                    Path.key("amt").termVal("*amt"),
                                    Path.key("isSuccess").termVal("*isSuccess")));


   // This subscribes the topology to *depositDepot and defines the ETL logic for it.
   mb.source("*depositDepot").out("*microbatch")
     .explodeMicrobatch("*microbatch").out("*data")
     .macro(extractJavaFields("*data", "*userId", "*amt"))
     .localTransform("$$funds", Path.key("*userId").nullToVal(0).term(Ops.PLUS, "*amt"));
  }

}
