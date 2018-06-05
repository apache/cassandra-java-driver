## Prepared statements

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

You should prepare only once, and cache the `PreparedStatement` in your application (it is
thread-safe). If you call `prepare` multiple times with the same query string, the driver will log a
warning.

If you execute a query only once, a prepared statement is inefficient because it requires two round
trips. Consider a [simple statement](../simple/) instead.

### Preparing

The `Session.prepare` method accepts either a query string or a `SimpleStatement` object. If you use
the object variant, both the initial prepare request and future bound statements will share some of
the options of that simple statement:

* initial prepare request: configuration profile name (or instance) and custom payload.
* bound statements: configuration profile name (or instance) and custom payload, idempotent flag.

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
immutable, so each method returns a new instance; make sure you don't accidentally discard the
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
      .withConfigProfileName("oltp")
      .withTimestamp(123456789L)
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
V4 or above, variables can be left unset, in which case they will be ignored (no tombstones will be
generated). If you're reusing a bound statement, you can use the `unset` method to unset variables
that were previously set:

```java
BoundStatement bound = ps1.bind()
  .setString("sku", "324378")
  .setString("description", "LCD screen");

// Positional:
bound = bound.unset("description");

// Named:
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

[BoundStatement]:  http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/BoundStatement.html

[CASSANDRA-10786]: https://issues.apache.org/jira/browse/CASSANDRA-10786
