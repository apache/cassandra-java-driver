## Handling graph results

[Script queries](../script/) and [explicit fluent traversals](../fluent/explicit/) return graph
result sets, which are essentially iterables of [GraphNode].

### Synchronous / asynchronous result

Like their CQL counterparts, graph result sets come in two forms, depending on the way the query
was executed. 

* `session.execute` returns a [GraphResultSet]. It can be iterated directly, and will return the
   whole result set, triggering background fetches if the query is paged:
   
    ```java
    for (GraphNode n : resultSet) {
      System.out.println(n);
    }
    ```
    
* `session.executeAsync` returns an [AsyncGraphResultSet]. It only holds the current page of
   results, accessible via the `currentPage()` method. If the query is paged, the next pages must be
   fetched explicitly using the `hasMorePages()` and `fetchNextPage()` methods. See [Asynchronous
   paging](../../../paging/#asynchronous-paging) for more details about how to work with async
   types.

*Note: at the time of writing (DSE 6.0), graph queries are never paged. Results are always returned
as a single page. However, paging is on the roadmap for a future DSE version; the driver APIs
reflect that, to avoid breaking changes when the feature is introduced.*

Both types have a `one()` method, to use when you know there is exactly one node, or are only
interested in the first one:

```java
GraphNode n = resultSet.one();
```

### Working with graph nodes

[GraphNode] wraps the responses returned by the server. Use the `asXxx()` methods to coerce a node
to a specific type:

```java
FluentGraphStatement statement = FluentGraphStatement.newInstance(g.V().count());
GraphNode n = session.execute(statement).one();
System.out.printf("The graph has %s vertices%n", n.asInt());
```

If the result is an array or "object" (in the JSON sense: a collection of named fields), you can
iterate its children:

```java
if (n.isList()) {
  for (int i = 0; i < n.size(); i++) {
    GraphNode child = n.getByIndex(i);
    System.out.printf("Element at position %d: %s%n", i, child);
  }

  // Alternatively, convert to a list:
  List<Object> l = n.asList();
}

if (n.isMap()) {
  for (Object key : n.keys()) {
    System.out.printf("Element at key %s: %s%n", key, n.getByKey(key));
  }

  // Alternatively, convert to a map:
  Map<String, Object> m = n.asMap();
}
```

#### Graph structural types

If the traversal returns graph elements (like vertices and edges), the results can be converted to
the corresponding TinkerPop types:

```java
GraphNode n = session.execute(FluentGraphStatement.newInstance(
    g.V().hasLabel("test_vertex")
)).one();
Vertex vertex = n.asVertex();

n = session.execute(FluentGraphStatement.newInstance(
    g.V().hasLabel("test_vertex").outE()
)).one();
Edge edge = n.asEdge();

n = session.execute(FluentGraphStatement.newInstance(
    g.V().hasLabel("test_vertex")
        .outE()
        .inV()
        .path()
)).one();
Path path = n.asPath();

n = session.execute(FluentGraphStatement.newInstance(
    g.V().hasLabel("test_vertex")
        .properties("name")
)).one();
// .properties() returns a list of properties, so we get the first one and transform it as a
// VertexProperty
VertexProperty vertexProperty = n.getByIndex(0).asVertexProperty();
```

#### Data type compatibility matrix

Dse graph exposes several [data types][DSE data types] when defining a schema for a graph. They
translate into specific Java classes when the data is returned from the server.

Here is an exhaustive compatibility matrix (for DSE 6.0):

| DSE graph  | Java driver         |
|------------|---------------------|
| bigint     | Long                |
| blob       | byte[]              |
| boolean    | Boolean             |
| date       | java.time.LocalDate |
| decimal    | BigDecimal          |
| double     | Double              |
| duration   | java.time.Duration  |
| float      | Float               |
| inet       | InetAddress         |
| int        | Integer             |
| linestring | LineString          |
| point      | Point               |
| polygon    | Polygon             |
| smallint   | Short               |
| text       | String              |
| time       | java.time.LocalTime |
| timestamp  | java.time.Instant   |
| uuid       | UUID                |
| varint     | BigInteger          |

If a type doesn't have a corresponding `asXxx()` method, use the variant that takes a type token:

```java
UUID uuid = graphNode.as(UUID.class);
```

[GraphNode]:           https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/graph/GraphNode.html
[GraphResultSet]:      https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/graph/GraphResultSet.html
[AsyncGraphResultSet]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/graph/AsyncGraphResultSet.html

[DSE data types]: https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/graph/reference/refDSEGraphDataTypes.html