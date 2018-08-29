## Prepared statements

Use [PreparedStatement] for queries that are executed multiple times in your application:

```java
PreparedStatement prepared = session.prepare(
  "insert into product (sku, description) values (?, ?)");

BoundStatement bound = prepared.bind("234827", "Mouse");
session.execute(bound);

session.execute(prepared.bind("987274", "Keyboard"));
```

When you prepare the statement, Cassandra will parse the query string, cache the result and return a unique identifier
(the `PreparedStatement` object keeps an internal reference to that identifier):

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

When you bind and execute a prepared statement, the driver will only send the identifier, which allows Cassandra to
skip the parsing phase:

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


You should prepare only once, and cache the `PreparedStatement` in your application (it is thread-safe). If you call
`prepare` multiple times with the same query string, the driver will log a warning.

If you execute a query only once, a prepared statement is inefficient because it requires two roundtrips. Consider a
[simple statement](../simple/) instead.

### Parameters and binding

Parameters can be either anonymous or named (named parameters are only
available with [native protocol](../../native_protocol) v2 or above):

```java
ps1 = session.prepare("insert into product (sku, description) values (?, ?)");
ps2 = session.prepare("insert into product (sku, description) values (:s, :d)");
```

To turn the statement into its executable form, you need to *bind* it to create a [BoundStatement]. As shown previously,
there is a shorthand to provide the parameters in the same call:

```java
BoundStatement bound = ps1.bind("324378", "LCD screen");
```

You can also bind first, then use setters, which is slightly more
explicit:

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

You can use named setters even if the query uses anonymous parameters;
Cassandra will name the parameters after the column they apply to:

```java
BoundStatement bound = ps1.bind()
  .setString("sku", "324378")
  .setString("description", "LCD screen");
```

This can be ambiguous if the query uses the same column multiple times,
for example: `select * from sales where sku = ? and date > ? and date <
?`. In these situations, use positional setters or named parameters.

For native protocol V3 or below, all variables must be bound.  With native
protocol V4 or above, variables can be left unset, in which case they
will be ignored server side (no tombstones will be generated).  If you're
reusing a bound statement you can use the `unset` method to unset variables
that were previously set:

```java
BoundStatement bound = ps1.bind()
  .setString("sku", "324378")
  .setString("description", "LCD screen");

// Using the unset method to unset previously set value.
// Positional setter:
bound.unset(1);

// Named setter:
bound.unset("description");
```

A bound statement also has getters to retrieve the values. Note that
this has a small performance overhead since values are stored in their
serialized form.

`BoundStatement` is **not thread-safe**. You can reuse an instance multiple times with different parameters, but only
from a single thread and only if you use synchronous calls:

```java
BoundStatement bound = ps1.bind();

// This is safe:
bound.setString("sku", "324378");
session.execute(bound);

bound.setString("sku", "324379");
session.execute(bound);

// This is NOT SAFE. executeAsync runs concurrently with your code, so the first execution might actually read the
// values after the second setString call, and you would insert 324381 twice:
bound.setString("sku", "324380");
session.executeAsync(bound);

bound.setString("sku", "324381");
session.executeAsync(bound);
```

Also, make sure you don't accidentally reuse parameters from previous executions.

### Preparing on multiple nodes

Cassandra does not replicate prepared statements across the cluster. It is the
driver's responsibility to ensure that each node's cache is up to
date. It uses a number of strategies to achieve this:

1.  When a statement is initially prepared, it is first sent to a single
    node in the cluster (this avoids hitting all nodes in case
    the query string is wrong). Once that node replies successfully, the
    driver re-prepares on all remaining nodes:

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

    The prepared statement identifier is deterministic (it's a hash of the query string), so it is the same
    for all nodes.

2.  if a node crashes, it loses all of its prepared statements. So the
    driver keeps a client-side cache; anytime a node is marked back up,
    the driver re-prepares all statements on it;

3.  finally, if the driver tries to execute a statement and finds out
    that the coordinator doesn't know about it, it will re-prepare the
    statement on the fly (this is transparent for the client, but will cost
    two extra roundtrips):

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

You can customize these strategies through `QueryOptions`:

* [setPrepareOnAllHosts] controls whether statements are initially
  re-prepared on other hosts (step 1 above);
* [setReprepareOnUp] controls whether statements are re-prepared on a
  node that comes back up (step 2 above).

Changing the driver's defaults should be done with care and only in
specific situations; read each method's Javadoc for detailed
explanations.

### Avoid preparing 'SELECT *' queries

Both the driver and Cassandra maintain a mapping of `PreparedStatement` queries to their
metadata.  When a change is made to a table, such as a column being added or dropped, there
is currently no mechanism for Cassandra to invalidate the existing metadata.  Because of this,
the driver is not able to properly react to these changes and will improperly read rows after
a schema change is made.

Therefore it is currently recommended to list all columns of interest in
your prepared statements (i.e. `SELECT a, b, c FROM table`), instead of
relying on `SELECT *`.

This will be addressed in a future release of both Cassandra and the driver.  Follow
[CASSANDRA-10786] and [JAVA-1196] for more information.

[PreparedStatement]:    http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/PreparedStatement.html
[BoundStatement]:       http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/BoundStatement.html
[setPrepareOnAllHosts]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/QueryOptions.html#setPrepareOnAllHosts-boolean-
[setReprepareOnUp]:     http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/QueryOptions.html#setReprepareOnUp-boolean-
[execute]:              http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/Session.html#execute-com.datastax.driver.core.Statement-
[executeAsync]:         http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/Session.html#executeAsync-com.datastax.driver.core.Statement-
[CASSANDRA-10786]:      https://issues.apache.org/jira/browse/CASSANDRA-10786
[JAVA-1196]:            https://datastax-oss.atlassian.net/browse/JAVA-1196
