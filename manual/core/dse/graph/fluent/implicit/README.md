## Implicit execution

Instead of passing traversals to the driver, you can create a *remote traversal source* connected to
the DSE cluster:

```java
CqlSession session = CqlSession.builder().build();

GraphTraversalSource g =
    AnonymousTraversalSource.traversal().withRemote(DseGraph.remoteConnectionBuilder(session).build());
```

Then build traversals from that source. Whenever you reach a [terminal step] \(such as `next()`,
`toList()`...), the DSE driver will be invoked under the covers:

```java
List<Vertex> vertices = g.V().has("name", "marko").toList();
```

This lets you use the traversal as if it were working against a local graph; all the communication
with DSE is done transparently. Note however that the returned objects (vertices, edges...) are
completely *detached*: even though they contain the complete data, modifications made to them will
not be reflected on the server side.

Traversal sources with different configurations can easily be created through execution profiles in
the [configuration](../../../../configuration/):

```
datastax-java-driver {
  profiles {
    graph-oltp {
      basic.graph.traversal-source = a
      basic.graph.timeout = 30 seconds
    }
  }
}
```

Pass the profile name to the remote connection builder:

```java
GraphTraversalSource a = AnonymousTraversalSource.traversal().withRemote(
    DseGraph.remoteConnectionBuilder(session)
        .withExecutionProfileName("graph-oltp")
        .build());
```

-----

See also the [parent page](../) for topics common to all fluent traversals. 

[terminal step]: http://tinkerpop.apache.org/docs/current/reference/#terminal-steps
