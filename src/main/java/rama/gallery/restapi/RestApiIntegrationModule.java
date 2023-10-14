package rama.gallery.restapi;

import java.io.IOException;

import com.rpl.rama.*;
import com.rpl.rama.integration.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import org.asynchttpclient.*;
import org.asynchttpclient.netty.NettyResponse;

/*
 * This module demonstrates integrating Rama with an external service, in this case a REST API.
 *
 * See the test class RestApiIntegrationModuleTest for how a client interacts with this module.
 */
public class RestApiIntegrationModule implements RamaModule {

  //   This defines a "task global" object, which when used with declareObject (as shown below), creates a value that
  // can be referenced on all tasks in both ETLs and query topologies. This interface specializes the object on each
  // task with lifecycle methods "prepareForTask" and "close". This interface can be used for anything from creating
  // task-specific caches to clients to external systems. The latter use case is demonstrated here by creating an HTTP
  // client and managing its lifecycle through this interface.
  //   Many external client interfaces can be shared on the same thread, or if thread-safe can be shared among all
  // threads in the same worker. The documentation for this API explores how to manage resources like that, and the
  // rama-kafka project is a real-world example of doing so. Links:
  //  - https://redplanetlabs.com/docs/~/integrating.html
  //  - https://github.com/redplanetlabs/rama-kafka
  //   This example is using AsyncHttpClient as a demonstration of integrating with any Java API. From this example you can
  // see how you'd interact with external databases, monitoring systems, or other tools as well.
  public static class AsyncHttpClientTaskGlobal implements TaskGlobalObject {
    public AsyncHttpClient client;

    @Override
    public void prepareForTask(int taskId, TaskGlobalContext context) {
      client = Dsl.asyncHttpClient();
    }

    @Override
    public void close() throws IOException {
      client.close();
    }
  }

  // This method is the entry point to all modules. It defines all depots, ETLs, PStates, and query topologies.
  @Override
  public void define(Setup setup, Topologies topologies) {
    // This depot takes in URL strings. The second argument is a "depot partitioner" that controls how
    // appended data is partitioned across the depot, affecting on which task each piece of data begins
    // processing in ETLs.
    setup.declareDepot("*getDepot", Depot.hashBy(Ops.IDENTITY));
    // This declares a task global with the given value. Since AsyncHttpClientTaskGlobal implements the TaskGlobalObject
    // interface, the value is specialized per task. Accessing the variable "*httpClient" in topologies always accesses the
    // value local to the task where the topology event is running.
    setup.declareObject("*httpClient", new AsyncHttpClientTaskGlobal());

    // Stream topologies process appended data within a few milliseconds and guarantee all data will be fully processed.
    StreamTopology s = topologies.stream("getHttp");
    //   PStates are durable and replicated datastores and are represented as an arbitrary combination of data structures. Reads
    // and writes to PStates go to disk and are not purely in-memory operations.
    //   This PState stores the latest response for each URL, a map from a URL to the body of the HTTP response.
    s.pstate("$$responses", PState.mapSchema(String.class, String.class));
    // This subscribes the ETL to "*getDepot", binding all URLs to the variable "*url". Because of the depot partitioner
    // on "*getDepot", computation starts on the same task where registration info is stored for that URL in
    // the "$$responses" PState.
    s.source("*getDepot").out("*url")
     // eachAsync integrates arbitrary asynchronous work represented by a CompletableFuture within a topology. It ties
     // the success/failure of the asynchronous task with the success/failure of the topology. So if the asynchronous
     // work fails or times out, the topology will fail as well and the depot record will be retried. eachAsync is a
     // non-blocking operation.
     .eachAsync((AsyncHttpClientTaskGlobal client, String url) ->
                 client.client.prepareGet(url).execute().toCompletableFuture(),
                "*httpClient", "*url").out("*response")
     .each((RamaFunction1<NettyResponse, String>) NettyResponse::getResponseBody, "*response").out("*body")
     // This records the latest response in the PState.
     .localTransform("$$responses", Path.key("*url").termVal("*body"));
  }
}
