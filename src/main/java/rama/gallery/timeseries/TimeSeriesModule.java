package rama.gallery.timeseries;

import com.rpl.rama.*;
import com.rpl.rama.helpers.TopologyUtils.ExtractJavaField;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;

import rama.gallery.timeseries.data.*;

import static com.rpl.rama.helpers.TopologyUtils.*;

import java.util.*;

/*
 * This module demonstrates time-series analytics for the example use case of the latency of rendering different URLs
 * on a website. The module receives a stream of latencies for individual renders and accumulates min/max/average
 * stats for windows at minute, hour, daily, and monthly granularities. Accumulating at multiple granularities speeds
 * up queries over larger ranges.
 *
 * See the test class TimeSeriesModuleTest for how a client interacts with this module to perform various kinds
 * of range queries, including aggregating a range of data server-side.
 *
 * The stats computed for each window are min, max, average, and latest. To capture full distributions, you can add
 * a data structure such as T-Digest to the WindowStats object.
 *
 * You can see the data types used by the module in the package rama.gallery.timeseries.data.
 *
 * As with all the demos, data is represented as plain Java objects with public fields. You can represent
 * data however you want, and we generally recommend using a library with compact serialization and support
 * for evolving types (like Thrift or Protocol Buffers). We use plain Java objects in these demos to keep
 * them as simple as possible by not having additional dependencies. Using other representations is easy to do
 * by defining a custom serialization, and in all cases you always work with first-class objects all the time
 * when using Rama, whether appending to depots, processing in ETLs, or querying from PStates.
 */
public class TimeSeriesModule implements RamaModule {
  // This defines an aggregator that combines two WindowStats objects into one aggregated WindowStats object.
  // It is used both in the ETL to update the time-series PState as well as the query topology fetching the
  // WindowStats for a specific range.
  public static class CombineMeasurements implements RamaCombinerAgg<WindowStats> {
    @Override
    public WindowStats combine(WindowStats curr, WindowStats other) {
      WindowStats ret = new WindowStats();
      ret.cardinality = curr.cardinality + other.cardinality;
      ret.total = curr.total + other.total;
      ret.lastMillis = other.lastMillis;

      if(curr.minLatencyMillis == null) ret.minLatencyMillis = other.minLatencyMillis;
      else if(other.minLatencyMillis == null) ret.minLatencyMillis = curr.minLatencyMillis;
      else ret.minLatencyMillis = Math.min(curr.minLatencyMillis, other.minLatencyMillis);

      if(curr.maxLatencyMillis == null) ret.maxLatencyMillis = other.maxLatencyMillis;
      else if(other.maxLatencyMillis == null) ret.maxLatencyMillis = curr.maxLatencyMillis;
      else ret.maxLatencyMillis = Math.max(curr.maxLatencyMillis, other.maxLatencyMillis);

      return ret;
    }

    @Override
    public WindowStats zeroVal() {
      return new WindowStats();
    }
  }

  // This class is used below to define a depot partitioner. ExtractJavaField is a helper utility from
  // the open-source rama-helpers project for extracting a public field from an object.
  public static class ExtractURL extends ExtractJavaField {
    public ExtractURL() { super("url"); }
  }

  // These constants are used in the helper function below "emitQueryGranularities".
  public static final Map<String, String> NEXT_GRANULARITY = new HashMap<String, String>() {{
    put("m", "h");
    put("h", "d");
    put("d", "td");
  }};

  public static final Map<String, Integer> NEXT_GRANULARITY_DIVISOR = new HashMap<String, Integer>() {{
    put("m", 60);
    put("h", 24);
    put("d", 30);
  }};

  // This helper function is used in the query topology below to compute the minimal number of buckets that need to
  // be queried across all granularities to satisfy a query over an arbitrary range of minute buckets. For example, if querying
  // from minute 58 of hour 6 through minute 3 of hour 20 (both in the same day), then the queries that need to be done are:
  //   - "m" granularity, minute 58 of hour 6 through minute 0 of hour 7
  //   - "h" granularity, hour 7 through hour 20
  //   - "m" granularity, minute 0 of hour 20 through minute 3 of hour 20
  public static void emitQueryGranularities(String granularity, int startBucket, int endBucket, OutputCollector collector) {
    String nextGranularity = NEXT_GRANULARITY.get(granularity);
    if(nextGranularity==null) {
      collector.emit(granularity, startBucket, endBucket);
    } else {
      int divisor = NEXT_GRANULARITY_DIVISOR.get(granularity);
      int nextStartBucket = startBucket / divisor;
      if(startBucket % divisor != 0) nextStartBucket++;
      int nextEndBucket = endBucket / divisor;
      int nextAlignedStartBucket = nextStartBucket * divisor;
      int nextAlignedEndBucket = nextEndBucket * divisor;

      if(nextEndBucket > nextStartBucket) emitQueryGranularities(nextGranularity, nextStartBucket, nextEndBucket, collector);
      if(nextAlignedStartBucket >= nextAlignedEndBucket) {
        collector.emit(granularity, startBucket, endBucket);
      } else {
        if(nextAlignedStartBucket > startBucket) collector.emit(granularity, startBucket, nextAlignedStartBucket);
        if(endBucket > nextAlignedEndBucket) collector.emit(granularity, nextAlignedEndBucket, endBucket);
      }
    }
  }

  // This method is the entry point to all modules. It defines all depots, ETLs, PStates, and query topologies.
  @Override
  public void define(Setup setup, Topologies topologies) {
    // This depot takes in RenderLatency objects. The second argument is a "depot partitioner" that controls
    // how appended data is partitioned across the depot, affecting on which task each piece of data begins
    // processing in ETLs.
    setup.declareDepot("*renderLatencyDepot", Depot.hashBy(ExtractURL.class));

    // Defines the ETL as a microbatch topology. Microbatch topologies have higher throughput than stream topologies
    // with the tradeoff of update latency being in the hundreds of milliseconds range rather than single-digit milliseconds
    // range. They are generally preferable for analytics-oriented use cases like this one where the extra latency
    // doesn't matter.
    MicrobatchTopology mb = topologies.microbatch("timeseries");

    //   PStates are durable and replicated datastores and are represented as an arbitrary combination of data structures. Reads
    // and writes to PStates go to disk and are not purely in-memory operations.
    //   This PState stores bucketed stats for all granularities for each URL. Minute/hour/day/month granularities use the
    // strings "m", "h", "d", and "td" as keys in that position of the data structure. The final map in the data structure
    // is subindexed because it can contain millions of elements. Subindexing stores each value of those maps individually
    // and enables them to be written and queried efficiently even when they're huge. Subindexed maps are always sorted, and it's //  also easy to do range queries on them. This is demonstrated in the query topology below.
    //   This PState is structured so that all granularities for a given URL are stored on the same partition. This allows queries
    // for large time ranges that need to fetch data from multiple granularities to be efficient by fetching all data from one
    // partition (as opposed to needing to fetch different granularities from different partitions). The query topology below
    // demonstrates this.
    //   Note that the coarser time granularities take up very little additional space because they have so many fewer buckets. The hour
    // granularity has 60x fewer buckets than the minute granularity, and the daily granularity has 24x fewer buckets than the hour
    // granularity. Space usage for time-series indexes like this is dominated by the finest granularity.
    mb.pstate("$$windowStats", PState.mapSchema(String.class, // url
                                                PState.mapSchema(String.class, // granularity
                                                                 PState.mapSchema(Integer.class, // bucket
                                                                                  WindowStats.class).subindexed())));
    // This subscribes the ETL to "*renderLatencyDepot", binding the batch of all data in this microbatch to "*microbatch".
    // "*microbatch" represents a batch of data across all partitions of the depot.
    mb.source("*renderLatencyDepot").out("*microbatch")
      // "explodeMicrobatch" emits all individual pieces of data across all partitions of the microbatch and binds each one
      // to the variable "*data". Because of the depot partitioner on "*renderLatencyDepot", computation for each piece of data
      // starts on the same task where stats are stored for that URL in the "$$windowStats" PState.
      .explodeMicrobatch("*microbatch").out("*data")
      // "extractJavaFields" is another small utility from rama-helpers for extracting public fields from Java objects
      // and binding them to dataflow variables. Here the public fields "url", "renderMillis", and "timestampMillis" are
      // extracted from the object in "*data" and bound to the variables "*url", "*renderMillis", and "*timestampMillis".
      .macro(extractJavaFields("*data", "*url", "*renderMillis", "*timestampMillis"))
      // The code for updating the stats in the PState is defined with a combiner aggregator, so it needs as input a WindowStats
      // object with just "*renderMillis" in it. This helper function constructs that object and binds it to the variable "*singleStat".
      .each(WindowStats::makeSingle, "*renderMillis").out("*singleStat")
      // This code uses a lambda to insert custom Java code to emit one time for each granularity. This code computes
      // all granularities and the correct bucket for each granularity given the record's timestamp. Note how this function
      // emits two fields, "*granularity" and "*bucket".
      .each((Long millis, OutputCollector collector) -> {
        Integer minuteBucket = (int) (millis / (1000 * 60));
        Integer hourBucket = minuteBucket / 60;
        Integer dayBucket = hourBucket / 24;
        Integer thirtyDayBucket = dayBucket / 30;
        collector.emit("m", minuteBucket);
        collector.emit("h", hourBucket);
        collector.emit("d", dayBucket);
        collector.emit("td", thirtyDayBucket);
      }, "*timestampMillis").out("*granularity", "*bucket")
      // The writes to the "$$windowStats" PState are done with a compound aggregator, which specifies the write in the shape
      // of the data structure being written to. At the leaf of this aggregator is the "CombineMeasurement" aggregator, defined
      // at the beginning of this class. It takes as input whatever WindowStats object is already stored in the PState at
      // that position as well as the WindowStats object in "*singleStat".
      .compoundAgg("$$windowStats",
                   CompoundAgg.map("*url",
                                   CompoundAgg.map("*granularity",
                                                   CompoundAgg.map("*bucket",
                                                                   Agg.combiner(new CombineMeasurements(), "*singleStat")))));


    //   This defines a query topology for getting the aggregated stats for a range of minute buckets. Rather than fetch all
    // the minute buckets between the start and end buckets, it uses higher granularity buckets if possible to minimize the
    // amount of data that needs to be fetched from the PState.
    //   Unlike the ETL code above, query topologies are batched computations. Batched computations in Rama can do the same things
    // you can do with relational languages like SQL: inner joins, outer joins, subqueries, and aggregations. This particular
    // query topology is straightforward and simply aggregates all fetched WindowStats into a single returned WindowStats object.
    //   Since query topologies are colocated with the PStates in their module, they are very efficient. This query topology does
    // potentially many queries to the "$$windowStats" PState and aggregates them together without any network transfer in between.
    //   This query topology definition specifies it takes as input three arguments – "*url", "*startBucket", and "*endBucket" –
    // and will bind a variable called "*stats" for the return value.
    topologies.query("getStatsForMinuteRange", "*url", "*startBucket", "*endBucket").out("*stats")
              // First, the query topology switches to the task containing data for this URL. Query topologies are optimized when
              // there's a leading partitioner like this, performing the routing client-side instead of when on a module task. This
              // means clients of this query topology send their requests directly to the task containing the needed data.
              .hashPartition("*url")
              // This uses a helper function to emit all ranges of data for each granularity that need to be fetched.
              .each(TimeSeriesModule::emitQueryGranularities, "m", "*startBucket", "*endBucket").out("*granularity", "*gStart", "*gEnd")
              // This fetches each individual WindowStats object that needs to be aggregated. "sortedMapRange" selects a submap from
              // the subindexed map at that position, and "mapVals" navigates to every value of that map individually. Since the function
              // call right before this emits many times, this "localSelect" is executed for each of those emits. Individual WindowStats
              // objects are bound to the variable "*bucketStat".
              .localSelect("$$windowStats", Path.key("*url", "*granularity").sortedMapRange("*gStart", "*gEnd").mapVals()).out("*bucketStat")
              // Every query topology must have an "originPartition", which indicates to move the computation back to where the query started.
              .originPartition()
              //   This aggregates all "*bucketStat" objects emitted into a single object bound to the variable "*stats". Note that this
              // is the variable specified at the start of the query topology to name the return value.
              //   The "CombineMeasurements" aggregator here is the same one as used in the ETL above.
              .agg(Agg.combiner(new CombineMeasurements(), "*bucketStat")).out("*stats");
  }
}
