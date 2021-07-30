## Graph

The driver provides full support for DSE graph, the distributed graph database available in DataStax
Enterprise. The [CqlSession] interface extends [GraphSession], which adds specialized methods to
execute requests expressed in the [Gremlin] graph traversal language.  

*This manual only covers driver usage; for more information about server-side configuration and data
modeling, refer to the [DSE developer guide].*

Note: graph capabilities require the [Apache TinkerPop™] library to be present on the classpath. The
driver has a non-optional dependency on that library, but if your application does not use graph at
all, it is possible to exclude it to minimize the number of runtime dependencies (see the
[Integration>Driver dependencies](../../integration/#driver-dependencies) section for more
details). If the library cannot be found at runtime, graph queries won't be available and a warning
will be logged, but the driver will otherwise operate normally (this is also valid for OSGi
deployments).

If you do use graph, it is important to keep the precise TinkerPop version that the driver depends
on: unlike the driver, TinkerPop does not follow semantic versioning, so even a patch version change
(e.g. 3.3.0 vs 3.3.3) could introduce incompatibilities. So do not declare an explicit dependency in
your application, let the driver pull it transitively.

### Overview

There are 3 ways to execute graph requests:

1. Passing a Gremlin script directly in a plain Java string. We'll refer to this as the
   [script API](script/):

    ```java
    CqlSession session = CqlSession.builder().build();
 
    String script = "g.V().has('name', name)";
    ScriptGraphStatement statement =
        ScriptGraphStatement.builder(script)
            .withQueryParam("name", "marko")
            .build();
 
    GraphResultSet result = session.execute(statement);
    for (GraphNode node : result) {
      System.out.println(node.asVertex());
    }
    ```
    
2. Building a traversal with the [TinkerPop fluent API](fluent/), and [executing it
   explicitly](fluent/explicit/) with the session:
   
    ```java
    import static com.datastax.dse.driver.api.core.graph.DseGraph.g;
   
    GraphTraversal<Vertex, Vertex> traversal = g.V().has("name", "marko");
    FluentGraphStatement statement = FluentGraphStatement.newInstance(traversal);
 
    GraphResultSet result = session.execute(statement);
    for (GraphNode node : result) {
      System.out.println(node.asVertex());
    }
    ```

3. Building a connected traversal with the fluent API, and [executing it
   implicitly](fluent/implicit/) by invoking a terminal step:
   
    ```java
    GraphTraversalSource g = DseGraph.g
        .withRemote(DseGraph.remoteConnectionBuilder(session).build());

    List<Vertex> vertices = g.V().has("name", "marko").toList();
    ```

All executions modes rely on the same set of [configuration options](options/).

The script and explicit fluent API return driver-specific [result sets](results/). The implicit
fluent API returns Apache TinkerPop™ types directly.

[Apache TinkerPop™]: http://tinkerpop.apache.org/

[CqlSession]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/CqlSession.html
[GraphSession]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/graph/GraphSession.html

[DSE developer guide]: https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/graph/graphTOC.html
[Gremlin]: https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/graph/dseGraphAbout.html#dseGraphAbout__what-is-cql
