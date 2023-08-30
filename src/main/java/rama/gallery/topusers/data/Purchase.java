package rama.gallery.topusers.data;

import com.rpl.rama.RamaSerializable;

public class Purchase implements RamaSerializable {
  public long userId;
  public int purchaseCents;

  public Purchase(long userId, int purchaseCents) {
    this.userId = userId;
    this.purchaseCents = purchaseCents;
  }
}
