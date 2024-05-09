<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Fluent API

The driver depends on [Apache TinkerPop™], a graph computing framework that provides a fluent API to
build Gremlin traversals. This allows you to write your graph requests directly in Java, like you
would in a Gremlin-groovy script:

```java
// How this is initialized will depend on the execution model, see details below
GraphTraversalSource g = ...

GraphTraversal<Vertex, Vertex> traversal = g.V().has("name", "marko");
```

### Execution models

There are two ways to execute fluent traversals:

* [explicitly](explicit/) by wrapping a traversal into a statement and passing it to
  `session.execute`;
* [implicitly](implicit/) by building the traversal from a connected source, and calling a
  terminal step.

### Common topics

The following apply regardless of the execution model:

#### Limitations

At the time of writing (DSE 6.0 / driver 4.0), some types of queries cannot be executed through the
fluent API:

* system queries (e.g. creating / dropping a graph);
* configuration;
* DSE graph schema queries.

You'll have to use the [script API](../script) for those use cases.

#### Performance considerations

Before sending a fluent graph statement over the network, the driver serializes the Gremlin
traversal into a byte array. **Traversal serialization happens on the client thread, even in
asynchronous mode**. In other words, it is done on:

* the thread that calls `session.execute` or `session.executeAsync` for explicit execution;
* the thread that calls the terminal step for implicit execution.

In practice, this shouldn't be an issue, but we've seen it become problematic in some corner cases
of our performance benchmarks: if a single thread issues a lot of `session.executeAsync` calls in a 
tight loop, traversal serialization can dominate CPU usage on that thread, and become a bottleneck
for request throughput.

If you believe that you're running into that scenario, start by profiling your application to
confirm that the client thread maxes out its CPU core; to solve the problem, distribute your
`session.executeAsync` calls onto more threads.

#### Domain specific languages

Gremlin can be extended with domain specific languages to make traversals more natural to write. For
example, considering the following query:

```java
g.V().hasLabel("person").has("name", "marko").
  out("knows").hasLabel("person").has("name", "josh");
```

A "social" DSL could be written to simplify it as:

```java
socialG.persons("marko").knows("josh");
```

TinkerPop provides an annotation processor to generate a DSL from an annotated interface. This is
covered in detail in the [TinkerPop documentation][TinkerPop DSL].

Once your custom traversal source is generated, here's how to use it: 

```java
// Non-connected source for explicit execution:
SocialTraversalSource socialG = DseGraph.g.getGraph().traversal(SocialTraversalSource.class);

// Connected source for implicit execution:
SocialTraversalSource socialG =
    DseGraph.g
        .withRemote(DseGraph.remoteConnectionBuilder(session).build())
        .getGraph()
        .traversal(SocialTraversalSource.class);
```

#### Search and geospatial predicates

All the DSE predicates are available on the driver side:

* for [search][DSE search], use the [Search] class:

    ```java
    GraphTraversal<Vertex, String> traversal =
        g.V().has("recipe", "instructions", Search.token("Saute")).values("name");
    ```
    
* for [geospatial queries][DSE geo], use the [Geo] class:

    ```java
    GraphTraversal<Vertex, String> traversal =
        g.V()
            .has(
                "location",
                "point",
                Geo.inside(Geo.point(2.352222, 48.856614), 4.2, Geo.Unit.DEGREES))
            .values("name");
    ```

[Search]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/graph/predicates/Search.html
[Geo]:    https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/graph/predicates/Geo.html

[Apache TinkerPop™]: http://tinkerpop.apache.org/
[TinkerPop DSL]: http://tinkerpop.apache.org/docs/current/reference/#dsl
[DSE search]: https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/graph/using/useSearchIndexes.html
[DSE geo]: https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/graph/using/queryGeospatial.html
