## Simple statements

Use [SimpleStatement] for queries that will be executed only once (or a few times) in your application:

```java
SimpleStatement statement = new SimpleStatement(
    "SELECT value FROM application_params WHERE name = 'greeting_message'");
session.execute(statement);
```

When you don't need to customize anything on the `SimpleStatement` object, there is a convenient shortcut:

```java
session.execute("SELECT value FROM application_params WHERE name = 'greeting_message'");
```

Each time you execute a simple statement, Cassandra will parse the query string again; nothing is cached (neither on the
client nor on the server):

```ditaa
client                             driver                Cassandra
--+----------------------------------+---------------------+------
  |                                  |                     |
  | session.execute(SimpleStatement) |                     |
  |--------------------------------->|                     |
  |                                  | QUERY(query_string) |
  |                                  |-------------------->|
  |                                  |                     |
  |                                  |                     |
  |                                  |                     | - parse query string
  |                                  |                     | - execute query
  |                                  |                     |
  |                                  |       ROWS          |
  |                                  |<--------------------|
  |                                  |                     |
  |<---------------------------------|                     |
```

If you execute the same query often (or a similar query with different column values), consider a
[prepared statement](../prepared/) instead.


### Using values

Instead of sending a raw query string, you can use bind markers and provide values separately:

* by position:

    ```java
    String paramName = ...
    session.execute(
        "SELECT value FROM application_params WHERE name = ?",
        paramName);
    ```
* by name:

    ```java
    // Just a convenience to build a java.util.Map with a one-liner
    import com.google.common.collect.ImmutableMap;

    String paramName = ...
    session.execute(
        "SELECT value FROM application_params WHERE name = :n",
        ImmutableMap.<String, Object>of("n", paramName));
    ```

This syntax has a few advantages:

* if the values already come from some other part of your code, it looks cleaner than doing the concatenation yourself;
* you don't need to translate the values to their string representation. The driver will sent them alongside the query,
  in their serialized binary form.

The number of values must match the query string, and their types must match the database schema. Note that the driver
does not parse query strings, so it cannot perform those checks on the client side; if you make a mistake, the query
will be sent anyway, and the error will be caught by Cassandra (`InvalidQueryException` is a server-side error):

```java
session.execute(
        "SELECT value FROM application_params WHERE name = ?",
        "foo", "bar");
// Exception in thread "main" com.datastax.driver.core.exceptions.InvalidQueryException:
// Invalid amount of bind variables
```

### Value type inference

Another consequence of not parsing query strings is that the driver has to make a guess on how to serialize values,
based on their Java type (see the [default type mappings](../../#cql-to-java-type-mapping)). This can be tricky, in
particular for numeric types:

```java
// schema: create table bigints(b bigint primary key)
session.execute(
        "INSERT INTO bigints (b) VALUES (?)",
        1);
// Exception in thread "main" com.datastax.driver.core.exceptions.InvalidQueryException:
// Expected 8 or 0 byte long (4)
```

The problem here is that the literal `1` has the Java type `int`. So the driver serializes it as a CQL `int` (4 bytes),
but the server expects a CQL `bigint` (8 bytes). The fix is to specify the correct Java type:

```java
session.execute(
        "INSERT INTO bigints (b) VALUES (?)",
        1L);
```

In the same vein, strings are always serialized to `varchar`, so you could have a problem if you target an `ascii`
column:

```java
// schema: create table ascii_quotes(id int primary key, t ascii)
session.execute(
        "INSERT INTO ascii_quotes (id, t) VALUES (?, ?)",
        1, "Touché sir, touché...");
// Exception in thread "main" com.datastax.driver.core.exceptions.InvalidQueryException:
// Invalid byte for ascii: -61
```

In that situation, there is no way to hint at the correct type. Your only option is to serialize the value manually:

```java
ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersionEnum();
ByteBuffer bytes = DataType.ascii().serialize("Touché sir, touché...", protocolVersion);
session.execute(
        "INSERT INTO ascii_quotes (id, t) VALUES (?, ?)",
        1, bytes);
```

[SimpleStatement]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/SimpleStatement.html
