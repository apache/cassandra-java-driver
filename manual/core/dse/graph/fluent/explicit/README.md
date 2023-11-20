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

## Explicit execution

Fluent traversals can be wrapped into a [FluentGraphStatement] and passed to the session:

```java
// A "dummy", non-connected traversal source that is not meant to be iterated directly, but instead
// serves as the basis to build fluent statements:
import static com.datastax.dse.driver.api.core.graph.DseGraph.g;

GraphTraversal<Vertex, Vertex> traversal = g.V().has("name", "marko");
FluentGraphStatement statement = FluentGraphStatement.newInstance(traversal);

GraphResultSet result = session.execute(statement);
for (GraphNode node : result) {
  System.out.println(node.asVertex());
}
```

### Creating fluent statements

#### Factory method

As shown above, [FluentGraphStatement.newInstance] creates a statement from a traversal directly.

The default implementation returned by the driver is **immutable**; if you call additional methods
on the statement -- for example to set [options](../../options/) -- each method call will create a
new copy:

```java
FluentGraphStatement statement = FluentGraphStatement.newInstance(traversal);
FluentGraphStatement statement2 = statement.setTimeout(Duration.ofSeconds(10));

assert statement2 != statement;
``` 

Immutability is good because it makes statements inherently **thread-safe**: you can share them in
your application and access them concurrently without any risk.

On the other hand, it means a lot of intermediary copies if you often call methods on your
statements. Modern VMs are normally good at dealing with such short-lived objects, but if you're
worried about the performance impact, consider using a builder instead.

Note: contrary to driver statements, Tinkerpop's `GraphTraversal` is mutable and therefore not
thread-safe. This is fine if you just wrap a traversal into a statement and never modify it
afterwards, but be careful not to share traversals and modify them concurrently.

#### Builder

Instead of creating a statement directly, you can pass your traversal to
[FluentGraphStatement.builder], chain method calls to set options, and finally call `build()`:

```java
FluentGraphStatement statement1 =
    FluentGraphStatement.builder(traversal)
        .withTimeout(Duration.ofSeconds(10))
        .withIdempotence(true)
        .build();
```

The builder implementation is **mutable**: every method call returns the same object, only one
builder instance will be created no matter how many methods you call on it. As a consequence, the
builder object is **not thread-safe**.

You can also initialize a builder from an existing statement: it will inherit all of its options.

```java
FluentGraphStatement statement2 =
    FluentGraphStatement.builder(statement1).withTimeout(Duration.ofSeconds(20)).build();

assert statement2.getTraversal().equals(statement1.getTraversal());
assert statement2.getTimeout().equals(Duration.ofSeconds(20)); // overridden by the builder
assert statement2.isIdempotent(); // because statement1 was
```

### Batching traversals

[BatchGraphStatement] allows you to execute multiple mutating traversals in the same transaction.
Like other types of statements, it is immutable and thread-safe, and can be created either with a
[factory method][BatchGraphStatement.newInstance] or a [builder][BatchGraphStatement.builder]: 

```java
GraphTraversal<Vertex, Vertex> traversal1 = g.addV("person").property("name", "batch1").property("age", 1);
GraphTraversal<Vertex, Vertex> traversal2 = g.addV("person").property("name", "batch2").property("age", 2);

// Each method call creates a copy:
BatchGraphStatement batch1 = BatchGraphStatement.newInstance()
    .addTraversal(traversal1)
    .addTraversal(traversal2);

// Uses a single, mutable builder instance:
BatchGraphStatement batch2 = BatchGraphStatement.builder()
        .addTraversal(traversal1)
        .addTraversal(traversal2)
        .build();
```

Traversal batches are only available with DSE 6.0 or above.

### Prepared statements

At the time of writing (DSE 6.0), prepared graph statements are not supported yet; they will be
added in a future version.

-----

See also the [parent page](../) for topics common to all fluent traversals. 

[FluentGraphStatement]:             https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/graph/FluentGraphStatement.html
[FluentGraphStatement.newInstance]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/graph/FluentGraphStatement.html#newInstance-org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal-
[FluentGraphStatement.builder]:     https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/graph/FluentGraphStatement.html#builder-org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal-
[BatchGraphStatement]:              https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/graph/BatchGraphStatement.html
[BatchGraphStatement.newInstance]:  https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/graph/BatchGraphStatement.html#newInstance--
[BatchGraphStatement.builder]:      https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/graph/BatchGraphStatement.html#builder--
