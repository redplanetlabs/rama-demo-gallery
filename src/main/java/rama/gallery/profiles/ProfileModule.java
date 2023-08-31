package rama.gallery.profiles;

import com.rpl.rama.*;
import com.rpl.rama.helpers.ModuleUniqueIdPState;
import com.rpl.rama.helpers.TopologyUtils.ExtractJavaField;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;

import rama.gallery.profiles.data.*;

import static com.rpl.rama.helpers.TopologyUtils.*;

/*
 * This module demonstrates account registration, generating unique 64-bit user IDs, and editing profiles.
 * The implementation is fault-tolerant, and there are no race conditions.
 *
 * See the test class ProfileModuleTest for how a client interacts with this module to perform user registrations
 * and profile edits.
 *
 * You can see the data types used by the module in the package rama.gallery.profiles.data.
 *
 * As with all the demos, data is represented as plain Java objects with public fields. You can represent
 * data however you want, and we generally recommend using a library with compact serialization and support
 * for evolving types (like Thrift or Protocol Buffers). We use plain Java objects in these demos to keep
 * them as simple as possible by not having additional dependencies. Using other representations is easy to do
 * by defining a custom serialization, and in all cases you always work with first-class objects all the time
 * when using Rama, whether appending to depots, processing in ETLs, or querying from PStates.
 */
public class ProfileModule implements RamaModule {
  // These classes are used below to define depot partitioners. ExtractJavaField is a helper utility from
  // the open-source rama-helpers project for extracting a public field from an object.
  public static class ExtractUsername extends ExtractJavaField {
    public ExtractUsername() { super("username"); }
  }

  public static class ExtractUserId extends ExtractJavaField {
    public ExtractUserId() { super("userId"); }
  }


  // This method is the entry point to all modules. It defines all depots, ETLs, PStates, and query topologies.
  @Override
  public void define(Setup setup, Topologies topologies) {
    // This depot takes in Registration objects. The second argument is a "depot partitioner" that controls
    // how appended data is partitioned across the depot, affecting on which task each piece of data begins
    // processing in ETLs.
    setup.declareDepot("*registrationDepot", Depot.hashBy(ExtractUsername.class));
    // This depot takes in ProfileEdits objects.
    setup.declareDepot("*profileEditsDepot", Depot.hashBy(ExtractUserId.class));

    // Stream topologies process appended data within a few milliseconds and guarantee all data will be fully processed.
    // Their low latency makes them appropriate for a use case like this.
    StreamTopology profiles = topologies.stream("profiles");
    //   PStates are durable and replicated datastores and are represented as an arbitrary combination of data structures. Reads
    // and writes to PStates go to disk and are not purely in-memory operations.
    //   This PState is used to assign a userId to every registered username. It also prevents race conditions in the case
    // of multiple concurrent registrations of the same username. Every registration contains a UUID that uniquely identifies
    // the registration request. The first registration records its UUID along with the generated 64-bit userId in this PState.
    // A registration request is known to be successful if the UUID used for registration is recorded in this PState.
    // Further details are described below with the ETL definition.
    profiles.pstate("$$usernameToRegistration", PState.mapSchema(String.class,
                                                                 PState.fixedKeysSchema("userId", Long.class,
                                                                                        "uuid", String.class)));
    // This PState stores all profile information for each userId.
    profiles.pstate("$$profiles", PState.mapSchema(Long.class, // userId
                                                   PState.fixedKeysSchema("username", String.class,
                                                                          "pwdHash", String.class,
                                                                          "displayName", String.class,
                                                                          "heightInches", Integer.class)));


    // ModuleUniqueIdPState is a small utility from rama-helpers that abstracts away the pattern of generating unique 64-bit IDs.
    // 64-bit IDs are preferable to UUIDs because they take half the space, but since they're smaller generating them randomly has
    // too high a chance of not being globally unique. ModuleUniqueIdPState uses a PState to track a task-specific counter, and it
    // combines that counter with the task ID to generate IDs that are globally unique.
    ModuleUniqueIdPState idGen = new ModuleUniqueIdPState("$$id");
    idGen.declarePState(profiles);

    // This subscribes the ETL to "*registrationDepot", binding all registration objects to the variable "*data". Because of the depot
    // partitioner on "*registrationDepot", computation starts on the same task where registration info is stored for that username in
    // the "$$usernameToRegistration" PState.
    profiles.source("*registrationDepot").out("*data")
            // "extractJavaFields" is another small utility from rama-helpers for extracting public fields from Java objects
            // and binding them to dataflow variables. Here the public fields "uuid", "username", and "pwdHash are extracted
            // from the object in "*data" and bound to the variables "*uuid", "*username", and "*pwdHash".
            .macro(extractJavaFields("*data", "*uuid", "*username", "*pwdHash"))
            //   The first step of registration is to see if this username is already registered. So the current registration info
            // is fetched from the "$$usernameToRegistration" PState and bound to the variable "*currInfo".
            //   A critical property of Rama is that only one event can run on a task at time. So while an ETL event is running,
            // no other ETL events, PState queries, or other events can run on the task. In this case, we know that any other
            // registration requests for the same username are queued behind this event, and there are no race conditions with
            // concurrent registrations because they are run serially on this task for this username.
            .localSelect("$$usernameToRegistration", Path.key("*username")).out("*currInfo")
            // This extracts the currently registered UUID from the registration info and binds it to "*currUUID". Note that Ops.GET
            // treats null values in the first argument like an empty map.
            .each(Ops.GET, "*currInfo", "uuid").out("*currUUID")
            // There are two cases where this is a valid registration:
            //  - "*currInfo" is null, meaning this is the first time a registration has been seen for this username
            //  - The UUID inside "*currInfo" matches the registration UUID. This indicates the registration request was retried,
            //    either by the stream topology due to a downstream failure (e.g. a node dying), or by the client re-appending
            //    the same request to the depot due to receiving an error.
            .ifTrue(new Expr(Ops.OR, new Expr(Ops.IS_NULL, "*currInfo"),
                                     new Expr(Ops.EQUAL, "*uuid", "*currUUID")),
              //   This block is run when the condition to ifTrue was true. No block is provided for the false case since
              // a registration of an invalid username is a no-op.
              //   Rama macros are a way to insert snippets of dataflow code. ModuleUniqueIDPState defines the method "genId" to insert
              // code to generate a globally unique ID and bind it to the specified variable. The generated code increments the counter
              // on this task by one and computes the ID by combining that counter with the task ID.
              Block.macro(idGen.genId("*userId"))
                   // This records the registration info in the PState.
                   .localTransform("$$usernameToRegistration",
                                   Path.key("*username").multiPath(Path.key("userId").termVal("*userId"),
                                                                   Path.key("uuid").termVal("*uuid")))
                   // The ETL is currently partitioned by username, but now it needs to record information for a userId. This
                   // hashPartition call relocates computation to the task which will be used to store information for this userId.
                   // hashPartition always chooses the same task ID for the same user ID but evenly spreads different user IDs across
                   // all tasks. The code before and after this call can run on different processes on different machines, and Rama
                   // takes care of all serialization and network transfer required.
                   .hashPartition("*userId")
                   // Finally, this code records the username and pwdHash for the new user ID in the "$$profiles" PState.
                   .localTransform("$$profiles", Path.key("*userId").multiPath(Path.key("username").termVal("*username"),
                                                                               Path.key("pwdHash").termVal("*pwdHash"))));

    // This subscribes the ETL to "*profileEditsDepot", binding all edit objects to the variable "*data". The depot partitioner in
    // this case ensures that processing starts on the task where we're storing information for the user ID.
    profiles.source("*profileEditsDepot").out("*data")
            .macro(extractJavaFields("*data", "*userId", "*edits"))
            // "*edits" is a list, and Ops.EXPLODE emits one time for every element in that list. Each element is bound to the variable
            // "*edit".
            .each(Ops.EXPLODE, "*edits").out("*edit")
            // The ProfileEdits.Edit datatype maps how each type of edit maps to keys used to store the profile information in the PState.
            .each(ProfileEdits.Edit::getKey, "*edit").out("*key")
            .macro(extractJavaFields("*edit", "*value"))
            // This writes the new value for the field into the "$$profiles" PState.
            .localTransform("$$profiles", Path.key("*userId", "*key").termVal("*value"));
  }

}
