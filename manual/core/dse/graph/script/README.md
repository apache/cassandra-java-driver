## Script API

The script API handles Gremlin-groovy requests provided as plain Java strings. To execute a script,
wrap it into a [ScriptGraphStatement] and pass it to the session:

```java
CqlSession session = CqlSession.builder().build();

String groovyScript = "system.graph('demo').ifNotExists().create()";
ScriptGraphStatement statement = ScriptGraphStatement.newInstance(groovyScript);
session.execute(statement);
```

### Creating script statements

#### Factory method

As demonstrated above, the simplest way to create a script statement is to pass the Gremlin-groovy
string to [ScriptGraphStatement.newInstance].

The default implementation returned by the driver is **immutable**; if you call additional methods
on the statement -- for example to set [options](../options/) -- each method call will create a new
copy:

```java
ScriptGraphStatement statement =
    ScriptGraphStatement.newInstance("system.graph('demo').ifNotExists().create()");
ScriptGraphStatement statement2 = statement.setTimeout(Duration.ofSeconds(10));

assert statement2 != statement;
```

Immutability is good because it makes statements inherently **thread-safe**: you can share them in
your application and access them concurrently without any risk.

On the other hand, it means a lot of intermediary copies if you often call methods on your
statements. Modern VMs are normally good at dealing with such short-lived objects, but if you're
worried about the performance impact, consider using a builder instead.

#### Builder

Instead of creating a statement directly, you can pass your Gremlin-groovy string to
[ScriptGraphStatement.builder], chain method calls to set options, and finally call `build()`:

```java
ScriptGraphStatement statement1 =
    ScriptGraphStatement.builder("system.graph('demo').ifNotExists().create()")
        .withTimeout(Duration.ofSeconds(10))
        .withIdempotence(true)
        .build();
```

The builder implementation is **mutable**: every method call returns the same object, only one
builder instance will be created no matter how many methods you call on it. As a consequence, the
builder object is **not thread-safe**.

You can also initialize a builder from an existing statement: it will inherit all of its options.

```java
ScriptGraphStatement statement2 =
    ScriptGraphStatement.builder(statement1).withTimeout(Duration.ofSeconds(20)).build();

assert statement2.getScript().equals(statement1.getScript());
assert statement2.getTimeout().equals(Duration.ofSeconds(20)); // overridden by the builder
assert statement2.isIdempotent(); // because statement1 was
```

### Parameters

Gremlin-groovy scripts accept parameters, which are always named. Note that, unlike in CQL,
placeholders are not prefixed with ":".

To manage parameters on an existing statement, use `setQueryParam` / `removeQueryParam`:

```java
ScriptGraphStatement statement =
    ScriptGraphStatement.newInstance("g.addV(label, vertexLabel)")
        .setQueryParam("vertexLabel", "test_vertex_2");
```

On the builder, use `withQueryParam` / `withoutQueryParams`:

```java
ScriptGraphStatement statement =
    ScriptGraphStatement.builder("g.addV(label, vertexLabel)")
        .withQueryParam("vertexLabel", "test_vertex_2")
        .build();
```

Alternatively, `withQueryParams` takes multiple parameters as a map.

### Use cases for the script API

Building requests as Java strings can be unwieldy, especially for long scripts. Besides, the script
API is a bit less performant on the server side. Therefore we recommend the
[Fluent API](../fluent/) instead for graph traversals.

Note however that some types of queries can only be performed through the script API:

* system queries (e.g. creating / dropping a graph);
* configuration;
* DSE graph schema queries.

[ScriptGraphStatement]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/graph/ScriptGraphStatement.html
[ScriptGraphStatement.newInstance]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/graph/ScriptGraphStatement.html#newInstance-java.lang.String-
[ScriptGraphStatement.builder]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/graph/ScriptGraphStatement.html#builder-java.lang.String-