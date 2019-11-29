## Graph options

There are various [configuration](../../../configuration/) options that control the execution of
graph statements. They can also be overridden programmatically on individual statements.

### Setting options

Given the following configuration:

```
datastax-java-driver {

  basic.graph.timeout = 3 seconds

  profiles {
    graph-oltp {
      basic.graph.timeout = 30 seconds
    }
  }
}
```

This statement inherits the timeout from the default profile:

```java
ScriptGraphStatement statement = ScriptGraphStatement.newInstance("g.V().next()");
assert statement.getTimeout().equals(Duration.ofSeconds(3));
```

This statement inherits the timeout from a named profile:

```java
ScriptGraphStatement statement =
    ScriptGraphStatement.newInstance("g.V().next()").setExecutionProfileName("graph-oltp");
assert statement.getTimeout().equals(Duration.ofSeconds(30));
```

This statement overrides the timeout programmatically; that takes precedence over the configuration:

```java
ScriptGraphStatement statement =
    ScriptGraphStatement.newInstance("g.V().next()").setTimeout(Duration.ofSeconds(5));
```

Programmatic overrides are also available in statement builders:

```java
ScriptGraphStatement statement =
    ScriptGraphStatement.builder("g.V().next()").withTimeout(Duration.ofSeconds(5)).build();
```

Whether you use the configuration or programmatic API depends on the use case; in general, we
recommend trying execution profiles first, if you can identify static categories of statements that
share the same options. Resort to the API for specific options that only apply to a single
statement, or if the value is only known at runtime.

### Available options

#### Graph name

The `basic.graph.name` option defines the name of the graph you're querying.

This doesn't have to be set all the time. In fact, some queries explicitly require no graph name,
for example those that access the `system` query. If you try to execute them with a graph name set,
you'll get an error:

```java
// Don't do this: executing a system query with the graph name set
ScriptGraphStatement statement =
    ScriptGraphStatement.newInstance("system.graph('demo').ifNotExists().create()")
        .setGraphName("test");
session.execute(statement);
// InvalidQueryException: No such property: system for class: Script2
```

If you set the graph name globally in the configuration, you'll need to unset it for system queries.
To do that, set it to `null`, or use the more explicit equivalent `is-system-query`:

```
datastax-java-driver {
  basic.graph.name = my_graph

  profiles {
    graph-system {
      # Don't inherit the graph name here
      basic.graph.is-system-query = true
    }
  }
}
```

```java
ScriptGraphStatement statement =
    ScriptGraphStatement.newInstance("system.graph('demo').ifNotExists().create()")
        .setExecutionProfileName("graph-system");

// Programmatic alternative:
ScriptGraphStatement statement =
    ScriptGraphStatement.newInstance("system.graph('demo').ifNotExists().create()")
        .setSystemQuery(true);
```

#### Traversal source

`basic.graph.traversal-source` defines the underlying engine used to create traversals.

Set this to `g` for regular OLTP queries, or `a` for OLAP queries.

#### Consistency level

Graph statements use the same option as CQL: `basic.request.consistency`.

However, DSE graph also provides a finer level of tuning: a single traversal may produce multiple
internal storage queries, some of which are reads, and others writes. The read and write consistency
levels can be configured independently with `basic.graph.read-consistency` and
`basic.graph.write-consistency`.

If any of these is set, it overrides the consistency level for that type of query; otherwise, the
global option is used.

#### Timeout

Graph statements have a dedicated timeout option: `basic.graph.timeout`. This is because the timeout
behaves a bit differently with DSE graph: by default, it is unset and the driver will wait until the
server replies (there are server-side timeouts that limit how long the request will take).

If a timeout is defined on the client, the driver will fail the request after that time, without
waiting for a reply. But the timeout is also sent alongside the initial request, and the server will
adjust its own timeout to ensure that it doesn't keep working for a result that the client is no
longer waiting for.

#### Graph protocol version

DSE graph relies on the Cassandra native protocol, but it extends it with a sub-protocol that has
its own versioning scheme.

`advanced.graph.sub-protocol` controls the graph protocol version to use for each statement. It is
unset by default, and you should almost never have to change it: the driver sets it automatically
based on the information it knows about the server.

There is one exception: if you use the [script API](../script/) against a legacy DSE version (5.0.3
or older), the driver infers the wrong protocol version. This manifests as a `ClassCastException`
when you try to deserialize complex result objects, such as vertices:

```java
GraphResultSet result =
    session.execute(ScriptGraphStatement.newInstance("g.V().next()"));
result.one().asVertex();
// ClassCastException: java.util.LinkedHashMap cannot be cast to org.apache.tinkerpop.gremlin.structure.Vertex
```

If you run into that situation, force the sub-protocol to `graphson-1.0` for script statements
(that's not necessary for fluent statements).

Currently, if the Graph sub-protocol version is not specified on a given GraphStatement, and it's
not explicitly set through `advanced.graph.sub-protocol` in configuration, the version of DSE to
which the driver is connected will determine the default sub-protocol version used by the driver.
For DSE 6.8.0 and later, the driver will pick "graph-binary-1.0" as the default sub-protocol
version. For DSE 6.7.x and older (or in cases where the driver can't determine the DSE version), the
driver will pick "graphson-2.0" as the default sub-protocol version.