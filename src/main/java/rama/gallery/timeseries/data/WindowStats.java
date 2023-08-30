package rama.gallery.timeseries.data;

import java.util.Objects;

import com.rpl.rama.RamaSerializable;

public class WindowStats implements RamaSerializable {
  public long cardinality = 0;
  public long total = 0;
  public Integer lastMillis = null;
  public Integer minLatencyMillis = null;
  public Integer maxLatencyMillis = null;

  public static WindowStats makeSingle(int initLatency) {
    WindowStats ret = new WindowStats();
    ret.cardinality = 1;
    ret.total = initLatency;
    ret.lastMillis = initLatency;
    ret.minLatencyMillis = initLatency;
    ret.maxLatencyMillis = initLatency;
    return ret;
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof WindowStats)) return false;
    WindowStats other = (WindowStats) obj;
    return cardinality == other.cardinality && total == other.total &&
           Objects.equals(lastMillis, other.lastMillis) &&
           Objects.equals(minLatencyMillis, other.minLatencyMillis) &&
           Objects.equals(maxLatencyMillis, other.maxLatencyMillis);
  }

  @Override
  public String toString() {
    return "<WindowStats> {" +
           "cardinality: " + cardinality + ", " +
           "total: " + total + ", " +
           "lastMillis: " + lastMillis + ", " +
           "minLatencyMillis: " + minLatencyMillis + ", " +
           "maxLatencyMillis: " + maxLatencyMillis + "}";
  }
}
