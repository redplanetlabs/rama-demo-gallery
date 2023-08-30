package rama.gallery.topusers;

import com.rpl.rama.*;
import com.rpl.rama.helpers.TopologyUtils.ExtractJavaField;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;

import java.util.List;

import static com.rpl.rama.helpers.TopologyUtils.*;

/*
 * This module demonstrates "top N" analytics in the context of computing the top spending users in
 * an e-commerce application. The module receives a stream of purchase data and incrementally maintains
 * a global list of the top 500 users by total purchase amount.
 *
 * This module only does the analytics portion. It can be combined with code such as in ProfilesModule to
 * also handle things like account registration and profile management.
 *
 * See the test class TopUsersModuleTest for examples of querying the top users.
 *
 * You can see the data types used by the module in the package rama.gallery.topusers.data.
 *
 * As with all the demos, data is represented as plain Java objects with public fields. You can represent
 * data however you want, and we generally recommend using a library with compact serialization and support
 * for evolving types (like Thrift or Protocol Buffers). We use plain Java objects in these demos to keep
 * them as simple as possible by not having additional dependencies. Using other representations is easy to do
 * by defining a custom serialization, and in all cases you always work with first-class objects all the time
 * when using Rama, whether appending to depots, processing in ETLs, or querying from PStates.
 */
public class TopUsersModule implements RamaModule {
  // Constant specifying the number of users to store in the "top spending users" index. Specified
  // as a public field on the module so it can be overridden in tests.
  public int topAmount = 500;

  // This class is used below to define a depot partitioner. ExtractJavaField is a helper utility from
  // the open-source rama-helpers project for extracting a public field from an object.
  public static class ExtractUserId extends ExtractJavaField {
    public ExtractUserId() { super("userId"); }
  }

  // This function implements part of the ETL below for maintaining top users. It's responsible for
  // updating the total spend amount for each user and emitting in each iteration of processing two
  // fields: userId's with a purchase and their updated total spend amount.
  private static SubBatch userSpendSubBatch(String microbatchVar) {
    // "explodeMicrobatch" emits all individual pieces of data across all partitions of the microbatch and binds each one
    // to the variable "*data".
    Block b = Block.explodeMicrobatch(microbatchVar).out("*data")
                   // "extractJavaFields" is another small utility from rama-helpers for extracting public
                   // fields from Java objects and binding them to dataflow variables. Here the public fields
                   // "userId" and "purchaseCents" are extracted from the object in "*data" and bound to the
                   // variables "*userId" and "*purchaseCents".
                   .macro(extractJavaFields("*data", "*userId", "*purchaseCents"))
                   // Batch blocks must always declare a partitioner before aggregating. In this case, we wish
                   // to partition the aggregation of total spend amounts by userId.
                   .hashPartition("*userId")
                   // This writes to the PState in the form of an aggregator, which specifies the write in the
                   // shape of the data structure being written to. At the leaf is the "sum" aggregator which
                   // adds each purchase into the total for that user. "captureNewValInto" is a special feature
                   // available in batch blocks to capture the updated values and emit them along with the keys
                   // used in the path to that position in the PState. In this case, following the "compoundAgg"
                   // the variables "*userId" and "*totalSpendCents" are bound for each user updated in this
                   // iteration.
                   .compoundAgg("$$userTotalSpend",
                                CompoundAgg.map("*userId", Agg.sum("*purchaseCents")
                                                              .captureNewValInto("*totalSpendCents")));
    // This returns the SubBatch and declares the output variables as "*userId" and "*totalSpendCents".
    return new SubBatch(b, "*userId", "*totalSpendCents");
  }

  // This method is the entry point to all modules. It defines all depots, ETLs, PStates, and query topologies.
  @Override
  public void define(Setup setup, Topologies topologies) {
    // This depot takes in Purchase objects. The second argument is a "depot partitioner" that controls
    // how appended data is partitioned across the depot, affecting on which task each piece of data begins
    // processing in ETLs.
    setup.declareDepot("*purchaseDepot", Depot.hashBy(ExtractUserId.class));


    // Defines the ETL as a microbatch topology. Microbatch topologies have higher throughput than stream topologies
    // with the tradeoff of update latency being in the hundreds of milliseconds range rather than single-digit milliseconds
    // range. They are generally preferable for analytics-oriented use cases like this one where the extra latency
    // doesn't matter.
    MicrobatchTopology mb = topologies.microbatch("topusers");
    //   PStates are durable and replicated datastores and are represented as an arbitrary combination of data structures. Reads
    // and writes to PStates go to disk and are not purely in-memory operations.
    //   This PState stores the total spend amount for each user, a map from user ID to spend amount (in cents). Spend
    // amount is represented as a long rather than an integer because the max integer is ~2B, which is only ~20M dollars.
    // Using longs allows for tracking much larger amounts.
    mb.pstate("$$userTotalSpend", PState.mapSchema(Long.class, Long.class));
    // This PState stores the list of the top 500 spending users. Since it's just a single list, it's declared as a
    // global PState. Global PStates only have a single partition. Note that the schema of the PState is just a plain
    // list and not a map like almost all databases are (with a "key" being the central concept to identify a record
    // or row).
    mb.pstate("$$topSpendingUsers", List.class).global();

    // This subscribes the ETL to "*purchaseDepot", binding the batch of all data in this microbatch to "*microbatch".
    // "*microbatch" represents a batch of data across all partitions of the depot.
    mb.source("*purchaseDepot").out("*microbatch")
      // Batch blocks are an enhanced computation mode for dataflow with the same capabilities as relational languages
      // (like SQL) such as inner joins, outer joins, subqueries, and aggregation. See this section of the Rama docs
      // for more details: https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_batch_blocks
      .batchBlock(
        // First, the total spend amounts are updated in a subbatch. This subbatch emits all updated users
        // and their new total spend amounts.
        Block.subBatch(userSpendSubBatch("*microbatch")).out("*userId", "*totalSpendCents")
             // This prepares for aggregating the data by combining the two variables into a 2-tuple.
             .each(Ops.TUPLE, "*userId", "*totalSpendCents").out("*tuple")
             // The list of top users is stored on a global partition, so the aggregation is partitioned
             // accordingly.
             .globalPartition()
             // The topMonotonic aggregator updates a list according to the provided specification. This instance
             // says to add data in "*tuple" into the aggregated list, and to keep the top 500. The aggregator
             // only keeps the latest record for each ID, which here is specified as the first element of the tuple
             // (the user ID). The "sort val" is what the aggregator uses for ranking, in this case the total spend
             // amount in the last position of the tuple.
             .agg("$$topSpendingUsers", Agg.topMonotonic(topAmount, "*tuple")
                                           .idFunction(Ops.FIRST)
                                           .sortValFunction(Ops.LAST)));
  }
}