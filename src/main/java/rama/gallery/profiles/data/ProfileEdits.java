package rama.gallery.profiles.data;

import java.util.*;

import com.rpl.rama.RamaSerializable;

public class ProfileEdits implements RamaSerializable {
  public static abstract class Edit implements RamaSerializable {
    public Object value;

    public abstract String getKey();

    public Edit(Object value) { this.value = value; }
  }

  public static class DisplayNameEdit extends Edit {
    public DisplayNameEdit(String name) {
      super(name);
    }

    @Override
    public String getKey() {
      return "displayName";
    }
  }

  public static class PwdHashEdit extends Edit {
    public PwdHashEdit(String pwdHash) {
      super(pwdHash);
    }

    @Override
    public String getKey() {
      return "pwdHash";
    }
  }

  public static class HeightInchesEdit extends Edit {
    public HeightInchesEdit(int inches) {
      super(inches);
    }

    @Override
    public String getKey() {
      return "heightInches";
    }
  }

  public long userId;
  public List<Edit> edits = new ArrayList();

  public ProfileEdits(long userId) { this.userId = userId; }

  public ProfileEdits addDisplayNameEdit(String name) {
    edits.add(new DisplayNameEdit(name));
    return this;
  }

  public ProfileEdits addPwdHashEdit(String pwdHash) {
    edits.add(new PwdHashEdit(pwdHash));
    return this;
  }

  public ProfileEdits addHeightInchesEdit(int heightInches) {
    edits.add(new HeightInchesEdit(heightInches));
    return this;
  }
}
