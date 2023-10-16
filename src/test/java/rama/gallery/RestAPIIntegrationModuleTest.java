package rama.gallery;

import org.junit.Test;

import com.rpl.rama.*;
import com.rpl.rama.test.*;

import rama.gallery.restapi.RestAPIIntegrationModule;

public class RestAPIIntegrationModuleTest {
  @Test
  public void test() throws Exception {
    // InProcessCluster simulates a full Rama cluster in-process and is an ideal environment for experimentation and
    // unit-testing.
    try(InProcessCluster ipc = InProcessCluster.create()) {
      RestAPIIntegrationModule module = new RestAPIIntegrationModule();
      // By default a module's name is the same as its class name.
      String moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(4, 2));

      // Client usage of IPC is identical to using a real cluster. Depot and PState clients are fetched by
      // referencing the module name along with the variable used to identify the depot/PState within the module.
      Depot getDepot = ipc.clusterDepot(moduleName, "*getDepot");
      PState responses = ipc.clusterPState(moduleName, "$$responses");

      String url1 = "https://official-joke-api.appspot.com/random_joke";
      String url2 = "http://jservice.io/api/random";

      // This checks the behavior of the module by appending a few URLs and printing the responses recorded in the
      // PState. To write a real test with actual assertions, it's best to test the behavior of the module with
      // the external REST API calls mocked out by using some sort of dependency injection.
      getDepot.append(url1);
      System.out.println("Response 1: " + responses.selectOne(Path.key(url1)));
      getDepot.append(url2);
      System.out.println("Response 2: " + responses.selectOne(Path.key(url2)));
    }
  }
}
