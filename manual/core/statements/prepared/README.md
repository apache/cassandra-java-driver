## Prepared statements

### Quick overview

Prepare a query string once, reuse with different values. More efficient than simple statements for
queries that are used often.

* create the prepared statement with `session.prepare()`, call [bind()][PreparedStatement.bind] or
  [boundStatementBuilder()][PreparedStatement.boundStatementBuilder] on it to create executable
  statements.
* the session has a built-in cache, it's OK to prepare the same string twice.
* values: `?` or `:name`, fill with `setXxx(int, ...)` or `setXxx(String, ...)` respectively.
* some values can be left unset with Cassandra 2.2+ / DSE 5+.   
* built-in implementation is **immutable**. Setters always return a new object, don't ignore the
  result.

-----

Use prepared statements for queries that are executed multiple times in your application:

```java
PreparedStatement prepared = session.prepare(
  "insert into product (sku, description) values (?, ?)");

BoundStatement bound = prepared.bind("234827", "Mouse");
session.execute(bound);
```

When you prepare the statement, Cassandra parses the query string, caches the result and returns a
unique identifier (the `PreparedStatement` object keeps an internal reference to that identifier):

```ditaa
client                   driver           Cassandra
--+------------------------+----------------+------
  |                        |                |
  | session.prepare(query) |                |
  |----------------------->|                |
  |                        | PREPARE(query) |
  |                        |--------------->|
  |                        |                |
  |                        |                |
  |                        |                | - compute id
  |                        |                | - parse query string
  |                        |                | - cache (id, parsed)
  |                        |                |
  |                        | PREPARED(id)   |
  |                        |<---------------|
  |  PreparedStatement(id) |                |
  |<-----------------------|                |
```

When you bind and execute a prepared statement, the driver only sends the identifier, which allows
Cassandra to skip the parsing phase:

```ditaa
client                            driver                Cassandra
--+---------------------------------+---------------------+------
  |                                 |                     |
  | session.execute(BoundStatement) |                     |
  |-------------------------------->|                     |
  |                                 | EXECUTE(id, values) |
  |                                 |-------------------->|
  |                                 |                     |
  |                                 |                     |
  |                                 |                     | - get cache(id)
  |                                 |                     | - execute query
  |                                 |                     |
  |                                 |          ROWS       |
  |                                 |<--------------------|
  |                                 |                     |
  |<--------------------------------|                     |
```

### Advantages of prepared statements

Beyond saving a bit of parsing overhead on the server, prepared statements have other advantages;
the `PREPARED` response also contains useful metadata about the CQL query:

* information about the result set that will be produced when the statement gets executed. The
  driver caches this, so that the server doesn't need to include it with every response. This saves
  a bit of bandwidth, and the resources it would take to decode it every time.
* the CQL types of the bound variables. This allows bound statements' `set` methods to perform
  better checks, and fail fast (without a server round-trip) if the types are wrong.
* which bound variables are part of the partition key. This allows bound statements to automatically
  compute their [routing key](../../load_balancing/#token-aware).
* more optimizations might get added in the future. For example, [CASSANDRA-10813] suggests adding
  an "[idempotent](../../idempotence)" flag to the response.

If you have a unique query that is executed only once, a [simple statement](../simple/) will be more
efficient. But note that this should be pretty rare: most client applications typically repeat the
same queries over and over, and a parameterized version can be extracted and prepared.  

### Preparing

`Session.prepare()` accepts either a plain query string, or a `SimpleStatement` object. If you use a
`SimpleStatement`, its execution parameters will propagate to bound statements:

```java
SimpleStatement simpleStatement =
    SimpleStatement.builder("SELECT * FROM product WHERE sku = ?")
        .setConsistencyLevel(DefaultConsistencyLevel.QUORUM)
        .build();
PreparedStatement preparedStatement = session.prepare(simpleStatement);
BoundStatement boundStatement = preparedStatement.bind();
assert boundStatement.getConsistencyLevel() == DefaultConsistencyLevel.QUORUM;
``` 

For more details, including the complete list of attributes that are copied, refer to
[API docs][Session.prepare].

The driver caches prepared statements: if you call `prepare()` multiple times with the same query
string (or a `SimpleStatement` with the same execution parameters), you will get the same
`PreparedStatement` instance:
 
```java
PreparedStatement ps1 = session.prepare("SELECT * FROM product WHERE sku = ?");
// The second call hits the cache, nothing is sent to the server:
PreparedStatement ps2 = session.prepare("SELECT * FROM product WHERE sku = ?");
assert ps1 == ps2;
``` 
 
We still recommend avoiding repeated calls to `prepare()`; if that's not possible (e.g. if query
strings are generated dynamically), there will just be a small performance overhead to check the
cache on every call.

Note that caching is based on:

* the query string exactly as you provided it: the driver does not perform any kind of trimming or
  sanitizing.
* all other execution parameters: for example, preparing two statements with identical query strings
  but different consistency levels will yield two distinct prepared statements (that each produce
  bound statements with their respective consistency level).

The size of the cache is exposed as a session-level [metric](../../metrics/)
`cql-prepared-cache-size`. The cache uses [weak values]([guava eviction]) eviction, so this
represents the number of `PreparedStatement` instances that your application has created, and is
still holding a reference to.

### Parameters and binding

The prepared query string will usually contain placeholders, which can be either anonymous or named:

```java
ps1 = session.prepare("insert into product (sku, description) values (?, ?)");
ps2 = session.prepare("insert into product (sku, description) values (:s, :d)");
```

To turn the statement into its executable form, you need to *bind* it in order to create a
[BoundStatement]. As shown previously, there is a shorthand to provide the parameters in the same
call:

```java
BoundStatement bound = ps1.bind("324378", "LCD screen");
```

You can also bind first, then use setters, which is slightly more explicit. Bound statements are 
**immutable**, so each method returns a new instance; make sure you don't accidentally discard the
result:

```java
// Positional setters:
BoundStatement bound = ps1.bind()
  .setString(0, "324378")
  .setString(1, "LCD screen");

// Named setters:
BoundStatement bound = ps2.bind()
  .setString("s", "324378")
  .setString("d", "LCD screen");
```

Finally, you can use a builder to avoid creating intermediary instances, especially if you have a
lot of methods to call: 

```java
BoundStatement bound =
  ps1
      .boundStatementBuilder()
      .setString(0, "324378")
      .setString(1, "LCD screen")
      .setExecutionProfileName("oltp")
      .setQueryTimestamp(123456789L)
      .build();
```

You can use named setters even if the query uses anonymous parameters; Cassandra names the
parameters after the column they apply to:

```java
BoundStatement bound = ps1.bind()
  .setString("sku", "324378")
  .setString("description", "LCD screen");
```

This can be ambiguous if the query uses the same column multiple times, like in `select * from sales
where sku = ? and date > ? and date < ?`. In these situations, use positional setters or named
parameters.

#### Unset values

With [native protocol](../../native_protocol/) V3, all variables must be bound. With native protocol
V4 (Cassandra 2.2 / DSE 5) or above, variables can be left unset, in which case they will be ignored
(no tombstones will be generated). If you're reusing a bound statement, you can use the `unset`
method to unset variables that were previously set:

```java
BoundStatement bound = ps1.bind()
  .setString("sku", "324378")
  .setString("description", "LCD screen");

// Named:
bound = bound.unset("description");

// Positional:
bound = bound.unset(1);
```

A bound statement also has getters to retrieve the values. Note that this has a small performance
overhead, since values are stored in their serialized form.

Since bound statements are immutable, they are safe to reuse across threads and asynchronous 
executions.


### How the driver prepares

Cassandra does not replicate prepared statements across the cluster. It is the driver's
responsibility to ensure that each node's cache is up to date. It uses a number of strategies to
achieve this:

1.  When a statement is initially prepared, it is first sent to a single node in the cluster (this
    avoids hitting all nodes in case the query string is wrong). Once that node replies
    successfully, the driver re-prepares on all remaining nodes:

    ```ditaa
    client                   driver           node1          node2  node3
    --+------------------------+----------------+--------------+------+---
      |                        |                |              |      |
      | session.prepare(query) |                |              |      |
      |----------------------->|                |              |      |
      |                        | PREPARE(query) |              |      |
      |                        |--------------->|              |      |
      |                        |                |              |      |
      |                        | PREPARED(id)   |              |      |
      |                        |<---------------|              |      |
      |                        |                |              |      |
      |                        |                |              |      |
      |                        |           PREPARE(query)      |      |
      |                        |------------------------------>|      |
      |                        |                |              |      |
      |                        |           PREPARE(query)      |      |
      |                        |------------------------------------->|
      |                        |                |              |      |
      |<-----------------------|                |              |      |
    ```

    The prepared statement identifier is deterministic (it's a hash of the query string), so it is
    the same for all nodes.

2.  if a node crashes, it might lose all of its prepared statements (this depends on the version:
    since Cassandra 3.10, prepared statements are stored in a table, and the node is able to 
    reprepare on its own when it restarts). So the driver keeps a client-side cache; anytime a node
    is marked back up, the driver re-prepares all statements on it;

3.  finally, if the driver tries to execute a statement and finds out that the coordinator doesn't 
    know about it, it will re-prepare the statement on the fly (this is transparent for the client,
    but will cost two extra roundtrips):

    ```ditaa
    client                          driver                         node1
    --+-------------------------------+------------------------------+--
      |                               |                              |
      |session.execute(boundStatement)|                              |
      +------------------------------>|                              |
      |                               |     EXECUTE(id, values)      |
      |                               |----------------------------->|
      |                               |                              |
      |                               |         UNPREPARED           |
      |                               |<-----------------------------|
      |                               |                              |
      |                               |                              |
      |                               |       PREPARE(query)         |
      |                               |----------------------------->|
      |                               |                              |
      |                               |        PREPARED(id)          |
      |                               |<-----------------------------|
      |                               |                              |
      |                               |                              |
      |                               |     EXECUTE(id, values)      |
      |                               |----------------------------->|
      |                               |                              |
      |                               |             ROWS             |
      |                               |<-----------------------------|
      |                               |                              |
      |<------------------------------|                              |
    ```

You can customize these strategies through the [configuration](../../configuration/):

* `datastax-java-driver.advanced.prepared-statements.prepare-on-all-nodes` controls whether
  statements are initially re-prepared on other hosts (step 1 above);
* `datastax-java-driver.advanced.prepared-statements.reprepare-on-up` controls how statements are
  re-prepared on a node that comes back up (step 2 above).

Read the [reference configuration](../../configuration/reference/) for a detailed description of each
of those options.

### Prepared statements and schema changes 

**With Cassandra 3 and below, avoid preparing `SELECT *` queries**; the driver does not handle
schema changes that would affect the results of a prepared statement. Therefore `SELECT *` queries
can create issues, for example:

* table `foo` contains columns `b` and `c`.
* the driver prepares `SELECT * FROM foo`. It gets a reply indicating that executing this statement
  will return columns `b` and `c`, and caches that metadata locally (for performance reasons: this
  avoids sending it with each response later).
* someone alters table `foo` to add a new column `a`.
* the next time the driver executes the prepared statement, it gets a response that now contains
  columns `a`, `b` and `c`. However, it's still using its stale copy of the metadata, so it decodes
  `a` thinking it's `b`. In the best case scenario, `a` and `b` have different types and decoding
  fails; in the worst case, they have compatible types and the client gets corrupt data.

To avoid this, do not create prepared statements for `SELECT *` queries if you plan on making schema
changes involving adding or dropping columns. Instead, always list all columns of interest in your
statement, i.e.: `SELECT b, c FROM foo`.

With Cassandra 4 and [native protocol](../../native_protocol/) v5, this issue is fixed
([CASSANDRA-10786]): the server detects that the driver is operating on stale metadata and sends the
new version with the response; the driver updates its local cache transparently, and the client can
observe the new columns in the result set.

[BoundStatement]:  https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/BoundStatement.html
[Session.prepare]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/CqlSession.html#prepare-com.datastax.oss.driver.api.core.cql.SimpleStatement-
[CASSANDRA-10786]: https://issues.apache.org/jira/browse/CASSANDRA-10786
[CASSANDRA-10813]: https://issues.apache.org/jira/browse/CASSANDRA-10813
[guava eviction]: https://github.com/google/guava/wiki/CachesExplained#reference-based-eviction
[PreparedStatement.bind]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/PreparedStatement.html#bind-java.lang.Object...-
[PreparedStatement.boundStatementBuilder]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/PreparedStatement.html#boundStatementBuilder-java.lang.Object...-
