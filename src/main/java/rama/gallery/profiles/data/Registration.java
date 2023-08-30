package rama.gallery.profiles.data;

import com.rpl.rama.RamaSerializable;

public class Registration implements RamaSerializable {
  public String uuid;
  public String username;
  public String pwdHash;

  public Registration(String uuid, String username, String pwdHash) {
    this.uuid = uuid;
    this.username = username;
    this.pwdHash = pwdHash;
  }
}
