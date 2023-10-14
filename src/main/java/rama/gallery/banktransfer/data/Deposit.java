package rama.gallery.banktransfer.data;

import com.rpl.rama.RamaSerializable;

public class Deposit implements RamaSerializable {
  public long userId;
  public int amt;

  public Deposit(long userId, int amt) {
    this.userId = userId;
    this.amt = amt;
  }

}
