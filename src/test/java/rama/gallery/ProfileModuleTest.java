package rama.gallery;

import com.rpl.rama.*;
import com.rpl.rama.test.*;

import rama.gallery.profiles.ProfileModule;
import rama.gallery.profiles.data.*;

import org.junit.Test;
import java.util.*;

import static org.junit.Assert.*;


public class ProfileModuleTest {
  // This function implements username registration, throwing an exception if the username is already registered.
  // This uses the ack return of the "profiles" topology to know if the registration request succeeded or not.
  public long register(Depot registrationDepot, PState usernameToRegistration, String username, String pwdHash) {
    String uuid = UUID.randomUUID().toString();
    // This depot append blocks until all colocated stream topologies have finished processing the data.
    Map<String, Object> ackReturns = registrationDepot.append(new Registration(uuid, username, pwdHash));
    Long userId = (Long) ackReturns.get("profiles");
    if(userId != null) return userId;
    else throw new RuntimeException("Username already registered");
  }

  @Test
  public void test() throws Exception {
    // InProcessCluster simulates a full Rama cluster in-process and is an ideal environment for experimentation and
    // unit-testing.
    try(InProcessCluster ipc = InProcessCluster.create()) {
      ProfileModule module = new ProfileModule();
      // By default a module's name is the same as its class name.
      String moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(4, 2));

      // Client usage of IPC is identical to using a real cluster. Depot and PState clients are fetched by
      // referencing the module name along with the variable used to identify the depot/PState within the module.
      Depot registrationDepot = ipc.clusterDepot(moduleName, "*registrationDepot");
      Depot profileEditsDepot = ipc.clusterDepot(moduleName, "*profileEditsDepot");
      PState usernameToRegistration = ipc.clusterPState(moduleName, "$$usernameToRegistration");
      PState profiles = ipc.clusterPState(moduleName, "$$profiles");

      long aliceId = register(registrationDepot, usernameToRegistration, "alice", "hash1");
      long bobId = register(registrationDepot, usernameToRegistration, "bob", "hash2");

      // verify registering alice again fails
      try {
        register(registrationDepot, usernameToRegistration, "alice", "hash3");
        assertTrue(false);
      } catch(Exception e) { }

      // verify that profiles are initialized correctly
      assertEquals("alice", profiles.selectOne(Path.key(aliceId, "username")));
      assertEquals("bob", profiles.selectOne(Path.key(bobId, "username")));

      // Do many profile edits at once and verify they all go through.
      ProfileEdits edits = new ProfileEdits(aliceId);
      edits.addDisplayNameEdit("Alice Smith");
      edits.addHeightInchesEdit(65);
      edits.addPwdHashEdit("hash4");
      profileEditsDepot.append(edits);

      Map expected = new HashMap();
      expected.put("username", "alice");
      expected.put("displayName", "Alice Smith");
      expected.put("heightInches", 65);
      expected.put("pwdHash", "hash4");
      assertEquals(expected, profiles.selectOne(Path.key(aliceId)));


      // Verify that profile editing only replaces specified fields.
      edits = new ProfileEdits(aliceId);
      edits.addDisplayNameEdit("Alicia Smith");
      profileEditsDepot.append(edits);

      expected = new HashMap();
      expected.put("username", "alice");
      expected.put("displayName", "Alicia Smith");
      expected.put("heightInches", 65);
      expected.put("pwdHash", "hash4");
      assertEquals(expected, profiles.selectOne(Path.key(aliceId)));

      // Do a single profile edit on a different user.
      edits = new ProfileEdits(bobId);
      edits.addDisplayNameEdit("Bobby");
      profileEditsDepot.append(edits);

      expected = new HashMap();
      expected.put("username", "bob");
      expected.put("displayName", "Bobby");
      expected.put("pwdHash", "hash2");
      assertEquals(expected, profiles.selectOne(Path.key(bobId)));
    }
  }
}
