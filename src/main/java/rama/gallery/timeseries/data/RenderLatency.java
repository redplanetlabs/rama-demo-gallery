package rama.gallery.timeseries.data;

import com.rpl.rama.RamaSerializable;

public class RenderLatency implements RamaSerializable {
  public String url;
  public int renderMillis;
  public long timestampMillis;

  public RenderLatency(String url, int renderMillis, long timestampMillis) {
    this.url = url;
    this.renderMillis = renderMillis;
    this.timestampMillis = timestampMillis;
  }
}
