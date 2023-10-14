package rama.gallery.banktransfer.data;

import com.rpl.rama.RamaSerializable;

public class Transfer implements RamaSerializable {
  public String transferId;
  public long fromUserId;
  public long toUserId;
  public int amt;

  public Transfer(String transferId, long fromUserId, long toUserId, int amt) {
    this.transferId = transferId;
    this.fromUserId = fromUserId;
    this.toUserId = toUserId;
    this.amt = amt;
  }

}
