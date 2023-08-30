package rama.gallery;

import com.rpl.rama.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;

import io.netty.util.internal.ThreadLocalRandom;
import rama.gallery.timeseries.TimeSeriesModule;
import rama.gallery.timeseries.data.*;

import org.junit.Test;
import java.util.*;

import static org.junit.Assert.*;


public class TimeSeriesModuleTest {
  // gets a random timestamp in the specified minute bucket
  private static int minute(int bucket) {
    return bucket * 60 * 1000 + ThreadLocalRandom.current().nextInt(60000);
  }

  @Test
  public void test() throws Exception {
    // InProcessCluster simulates a full Rama cluster in-process and is an ideal environment for experimentation and
    // unit-testing.
    try(InProcessCluster ipc = InProcessCluster.create()) {
      TimeSeriesModule module = new TimeSeriesModule();
      // By default a module's name is the same as its class name.
      String moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(4, 2));

      // Client usage of IPC is identical to using a real cluster. Depot, PState, and query topology clients are fetched
      // by referencing the module name along with the variable used to identify the depot/PState/query within the module.
      Depot renderLatencyDepot = ipc.clusterDepot(moduleName, "*renderLatencyDepot");
      PState windowStats = ipc.clusterPState(moduleName, "$$windowStats");
      QueryTopologyClient<WindowStats> getStatsForMinuteRange = ipc.clusterQuery(moduleName, "getStatsForMinuteRange");

      // add some test data across many time buckets
      renderLatencyDepot.append(new RenderLatency("foo.com", 10, minute(3)));
      renderLatencyDepot.append(new RenderLatency("foo.com", 20, minute(3)));
      renderLatencyDepot.append(new RenderLatency("foo.com", 15, minute(10)));
      renderLatencyDepot.append(new RenderLatency("foo.com", 18, minute(10)));
      renderLatencyDepot.append(new RenderLatency("foo.com", 33, minute(10)));
      renderLatencyDepot.append(new RenderLatency("foo.com", 20, minute(65)));
      renderLatencyDepot.append(new RenderLatency("foo.com", 30, minute(65)));
      renderLatencyDepot.append(new RenderLatency("foo.com", 100, minute(60 * 24)));
      renderLatencyDepot.append(new RenderLatency("foo.com", 100, minute(60 * 24 + 8)));
      renderLatencyDepot.append(new RenderLatency("foo.com", 50, minute(60 * 48 + 122)));

      // Microbatching runs asynchronously to depot appends, so this code waits for microbatching to finish
      // processing all the depot appends so we can see those appends reflected in PState queries.
      ipc.waitForMicrobatchProcessedCount(moduleName, "timeseries", 10);

      // Verify that a single bucket is correct.
      WindowStats bucketStats = windowStats.selectOne(Path.key("foo.com", "m", 3));
      WindowStats expectedStats3 = new WindowStats();
      expectedStats3.cardinality = 2;
      expectedStats3.total = 30;
      expectedStats3.lastMillis = 20;
      expectedStats3.minLatencyMillis = 10;
      expectedStats3.maxLatencyMillis = 20;
      assertEquals(expectedStats3, bucketStats);

      // This is an example of doing a range query. This fetches a submap from the PState from buckets 3 (inclusive) to
      // 11 (exclusive) for "m" granularity. That inner map is subindexed, and subindexed structures are sorted. Range
      // queries on subindexed structures are very efficient.
      SortedMap range = windowStats.selectOne(Path.key("foo.com", "m").sortedMapRange(3, 11));
      WindowStats expectedStats10 = new WindowStats();
      expectedStats10.cardinality = 3;
      expectedStats10.total = 66;
      expectedStats10.lastMillis = 33;
      expectedStats10.minLatencyMillis = 15;
      expectedStats10.maxLatencyMillis = 33;

      SortedMap expectedRange = new TreeMap();
      expectedRange.put(3, expectedStats3);
      expectedRange.put(10, expectedStats10);
      assertEquals(expectedRange, range);

      // This is an example of doing an aggregation within a PState query. This fetches the number of buckets between
      // minute 0 and minute 60*72. Critically, this entire path executes server-side. So the only thing transferred
      // back from the server is the count.
      int numMinuteBuckets = windowStats.selectOne(Path.key("foo.com", "m").sortedMapRange(0, 60 * 72).view(Ops.SIZE));
      assertEquals(6, numMinuteBuckets);


      // This is an example of invoking a query topology, which is just like invoking any regular Java function. You
      // pass it some arguments, and it returns a result back. The difference is it runs as a distributed computation on
      // a Rama cluster. This query topology efficiently fetches the aggregate WindowStats for an arbitrary range of minute
      // buckets, utilizing coarser granularities if possible to minimize the amount of buckets that need to be fetched
      // to perform the computation.
      WindowStats largeRange = getStatsForMinuteRange.invoke("foo.com", 0, 60 * 72);
      WindowStats expectedLarge = new WindowStats();
      expectedLarge.cardinality = 10;
      expectedLarge.total = 396;
      expectedLarge.lastMillis = 50;
      expectedLarge.minLatencyMillis = 10;
      expectedLarge.maxLatencyMillis = 100;
      assertEquals(expectedLarge, largeRange);

      // Verify that buckets at coarser granularity are aggregated correctly.
      WindowStats bucketStatsDay0 = windowStats.selectOne(Path.key("foo.com", "d", 0));
      WindowStats expectedStatsDay0 = new WindowStats();
      expectedStatsDay0.cardinality = 7;
      expectedStatsDay0.total = 146;
      expectedStatsDay0.lastMillis = 30;
      expectedStatsDay0.minLatencyMillis = 10;
      expectedStatsDay0.maxLatencyMillis = 33;
      assertEquals(expectedStatsDay0, bucketStatsDay0);
    }
  }

  // This is an example of unit testing a custom operation using OutputCollector.
  @Test
  public void testEmitQueryGranularities() {
    MockOutputCollector c = new MockOutputCollector();
    TimeSeriesModule.emitQueryGranularities("m", 63, 10033, c);
    Set<List> queries = new HashSet(c.getEmitsByStream().get(null));
    Set<List> expected = new HashSet();
    expected.add(Arrays.asList("m", 63, 120));
    expected.add(Arrays.asList("h", 2, 24));
    expected.add(Arrays.asList("d", 1, 6));
    expected.add(Arrays.asList("h", 144, 167));
    expected.add(Arrays.asList("m", 10020, 10033));
    assertEquals(expected, queries);

    c = new MockOutputCollector();
    TimeSeriesModule.emitQueryGranularities("d", 3, 122, c);
    queries = new HashSet(c.getEmitsByStream().get(null));
    expected = new HashSet();
    expected.add(Arrays.asList("d", 3, 30));
    expected.add(Arrays.asList("td", 1, 4));
    expected.add(Arrays.asList("d", 120, 122));
    assertEquals(expected, queries);
  }
}
