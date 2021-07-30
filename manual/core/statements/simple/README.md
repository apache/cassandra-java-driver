## Simple statements

### Quick overview

For one-off executions of a raw query string.

* create with [SimpleStatement.newInstance()] or [SimpleStatement.builder()].
* values: `?` or `:name`, fill with `setPositionalValues()` or `setNamedValues()` respectively.
  Driver has to guess target CQL types, this can lead to ambiguities.
* built-in implementation is **immutable**. Setters always return a new object, don't ignore the
  result.

-----

Use [SimpleStatement] for queries that will be executed only once (or just a few times):

```java
SimpleStatement statement =
    SimpleStatement.newInstance(
        "SELECT value FROM application_params WHERE name = 'greeting_message'");
session.execute(statement);
```

Each time you execute a simple statement, Cassandra parses the query string again; nothing is cached
(neither on the client nor on the server):

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

### Creating an instance

The driver provides various ways to create simple statements instances. First, `SimpleStatement` has
a few static factory methods:

```java
SimpleStatement statement =
    SimpleStatement.newInstance(
        "SELECT value FROM application_params WHERE name = 'greeting_message'");
```

You can then use setter methods to configure additional options. Note that, like all statement
implementations, simple statements are **immutable**, so these methods return a new instance each
time. Make sure you don't ignore the result:

```java
// WRONG: ignores the result
statement.setIdempotent(true);

// Instead, reassign the statement every time:
statement = statement.setIdempotent(true);
```

If you have many options to set, you can use a builder to avoid creating intermediary instances:

```java
SimpleStatement statement =
    SimpleStatement.builder("SELECT value FROM application_params WHERE name = 'greeting_message'")
        .setIdempotence(true)
        .build();
```

Finally, `Session` provides a shorthand method when you only have a simple query string:

```java
session.execute("SELECT value FROM application_params WHERE name = 'greeting_message'");
```

### Using values

Instead of hard-coding everything in the query string, you can use bind markers and provide values
separately:

* by position:

    ```java
    SimpleStatement.builder("SELECT value FROM application_params WHERE name = ?")
        .addPositionalValues("greeting_message")
        .build();
    ```
* by name:

    ```java
    SimpleStatement.builder("SELECT value FROM application_params WHERE name = :n")
        .addNamedValue("n", "greeting_message")
        .build();
    ```

This syntax has a few advantages:

* if the values come from some other part of your code, it looks cleaner than doing the 
  concatenation yourself;
* you don't need to translate the values to their string representation. The driver will send them 
  alongside the query, in their serialized binary form.

The number of values must match the number of placeholders in the query string, and their types must
match the database schema. Note that the driver does not parse simple statements, so it cannot
perform those checks on the client side; if you make a mistake, the query will be sent anyway, and
the server will reply with an error, that gets translated into a driver exception: 

```java
session.execute(
    SimpleStatement.builder("SELECT value FROM application_params WHERE name = :n")
        .addPositionalValues("greeting_message", "extra_value")
        .build());
// Exception in thread "main" com.datastax.oss.driver.api.core.servererrors.InvalidQueryException: 
// Invalid amount of bind variables
```

### Type inference

Another consequence of not parsing query strings is that the driver has to guess how to serialize 
values, based on their Java type (see the [default type mappings](../../#cql-to-java-type-mapping)).
This can be tricky, in particular for numeric types:

```java
// schema: create table bigints(b bigint primary key)
session.execute(
    SimpleStatement.builder("INSERT INTO bigints (b) VALUES (?)")
        .addPositionalValues(1)
        .build());
// Exception in thread "main" com.datastax.oss.driver.api.core.servererrors.InvalidQueryException:
// Expected 8 or 0 byte long (4)
```

The problem here is that the literal `1` has the Java type `int`. So the driver serializes it as a
CQL `int` (4 bytes), but the server expects a CQL `bigint` (8 bytes). The fix is to specify the
correct Java type:

```java
session.execute(
    SimpleStatement.builder("INSERT INTO bigints (b) VALUES (?)")
        .addPositionalValues(1L) // long literal
        .build());
```

Similarly, strings are always serialized to `varchar`, so you could have a problem if you target an
`ascii` column:

```java
// schema: create table ascii_quotes(id int primary key, t ascii)
session.execute(
    SimpleStatement.builder("INSERT INTO ascii_quotes (id, t) VALUES (?, ?)")
        .addPositionalValues(1, "Touché sir, touché...")
        .build());
// Exception in thread "main" com.datastax.oss.driver.api.core.servererrors.InvalidQueryException:
// Invalid byte for ascii: -61
```

In that situation, there is no way to hint at the correct type. Fortunately, you can encode the
value manually as a workaround: 

```java
TypeCodec<Object> codec = session.getContext().getCodecRegistry().codecFor(DataTypes.ASCII);
ByteBuffer bytes =
    codec.encode("Touché sir, touché...", session.getContext().getProtocolVersion());

session.execute(
    SimpleStatement.builder("INSERT INTO ascii_quotes (id, t) VALUES (?, ?)")
        .addPositionalValues(1, bytes)
        .build());
```

Or you could also use [prepared statements](../prepared/), which don't have this limitation since
parameter types are known in advance. 

[SimpleStatement]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/SimpleStatement.html
[SimpleStatement.newInstance()]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/SimpleStatement.html#newInstance-java.lang.String-
[SimpleStatement.builder()]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/SimpleStatement.html#builder-java.lang.String-
