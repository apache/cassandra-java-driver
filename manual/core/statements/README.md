## Statements

To execute a CQL query, you  create a [Statement] instance and pass it to
[Session#execute][execute] or [Session#executeAsync][executeAsync]. The driver provides various
implementations:

* [SimpleStatement](simple/): a simple implementation built directly from a character string. 
  Typically used for queries that are executed only once or a few times.
* [BoundStatement](prepared/): obtained by binding values to a prepared statement. Typically used
  for queries that are executed often, with different values.
* [BatchStatement](batch/): a statement that groups multiple statements to be executed as a batch.

All statement types share a [common set of options][StatementBuilder], that can be set through
either setters or a builder:

<!-- TODO create relevant sections and link to them -->

* [configuration profile](../configuration/) name, or the configuration profile itself if it's been
  built dynamically.
* custom payload to send arbitrary key/value pairs with the request (you only use this if you have
  a custom query handler on the server).
* idempotent flag.
* tracing flag.
* query timestamp.
* paging state.

When setting these options, keep in mind that statements are immutable, and every method returns a
different instance:

```java
SimpleStatement statement =
    SimpleStatement.newInstance("SELECT release_version FROM system.local");

// Won't work: statement isn't modified in place
statement.setConfigProfileName("oltp");
statement.setIdempotent(true);

// Do this instead:
statement = statement.setConfigProfileName("oltp").setIdempotent(true);
```

[Statement]:        http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/Statement.html
[StatementBuilder]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/StatementBuilder.html
[execute]:          http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/Session.html#execute-com.datastax.oss.driver.api.core.cql.Statement-
[executeAsync]:     http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/Session.html#executeAsync-com.datastax.oss.driver.api.core.cql.Statement-
