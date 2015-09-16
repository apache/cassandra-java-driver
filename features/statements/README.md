## Statements

To execute a query, you create a [Statement] instance and pass it to
`Session#execute()`. The driver provides various implementations:

* [SimpleStatement]: a simple implementation built directly from a
  character string. Typically used for queries that are executed only
  once or a few times.
* [BoundStatement]: obtained by binding values to a [prepared
  statement](prepared/). Typically used for queries that are executed
  often, with different values.
* [BuiltStatement]: a statement built with the [QueryBuilder] DSL. It
  can be executed directly like a simple statement, or prepared.
* [BatchStatement]: a statement that groups multiple statements to be
  executed as a batch.

`Session` also has a shortcut to build and execute a simple statement in
a single call:

```java
session.execute("select release_version from system.local");

// Is equivalent to:
Statement s = new SimpleStatement("select release_version from system.local");
session.execute(s);
```

### Customizing execution

Before executing a statement, you might want to customize certain
aspects of its execution. `Statement` provides a number of methods for
this, for example:

```java
Statement s = new SimpleStatement("select release_version from system.local");
s.enableTracing();
session.execute(s);
```

If you use custom policies ([RetryPolicy], [LoadBalancingPolicy],
[SpeculativeExecutionPolicy]...), you might also want to have custom
properties that influence statement execution. To achieve this, you can
wrap your statements in a custom [StatementWrapper] implementation.

[Statement]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Statement.html
[SimpleStatement]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/SimpleStatement.html
[BoundStatement]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/BoundStatement.html
[BatchStatement]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/BatchStatement.html
[QueryBuilder]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/querybuilder/QueryBuilder.html
[BuiltStatement]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/querybuilder/BuiltStatement.html
[StatementWrapper]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/StatementWrapper.html
[RetryPolicy]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/policies/RetryPolicy.html
[LoadBalancingPolicy]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/policies/LoadBalancingPolicy.html
[SpeculativeExecutionPolicy]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/policies/SpeculativeExecutionPolicy.html
