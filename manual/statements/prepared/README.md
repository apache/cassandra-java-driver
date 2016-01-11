## Prepared statements

When Cassandra executes a query, the first thing it does is parse the
query string to an internal representation. If the same query is used
often, you can use a prepared statement, which allows Cassandra to cache
that representation, and save time and resources for subsequent
executions. Prepared statements are typically parameterized, using
different values for each execution:

```java
PreparedStatement prepared = session.prepare(
  "insert into product (sku, description) values (?, ?)");

BoundStatement bound;

bound = prepared.bind("234827", "Mouse");
session.execute(bound);

bound = prepared.bind("987274", "Keyboard");
session.execute(bound);
```

Statements should be be prepared only once. If you call `prepare`
multiple times with the same query string, the driver will log a
warning. So your application should cache the `PreparedStatement` object
once it's been created (this can be as simple as storing it as a field
in a DAO).

### Parameters and binding

Parameters can be either anonymous or named (named parameters are only
available with [native protocol](../../native_protocol) v2 or above):

```java
ps1 = session.prepare("insert into product (sku, description) values (?, ?)");
ps2 = session.prepare("insert into product (sku, description) values (:s, :d)");
```

To turn the statement into its executable form, you need to *bind* it
and provide values for the parameters. As shown previously, there is a
shorthand form to do it all in one call:

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
  .setString("description", "LCD screen")

// Using the unset method to unset previously set value.
// Positional setter:
bound.unset("description");

// Named setter:
bound.unset(1);
```

A bound statement also has getters to retrieve the values. Note that
this has a small performance overhead since values are stored in their
serialized form.

### How the driver handles prepared statements

When the driver prepares a statement, it sends the query string to
Cassandra, which caches the statement and returns an identifier. Later,
when the driver needs to execute the statement, it just sends the
identifier and parameter values. Note that the identifier is
deterministic, so it will always be the same for all nodes (it's a
actually a hash of the query string).

Prepared statements are not replicated across the cluster. It is the
driver's responsibility to ensure that each node's cache is up to
date. It uses a number of strategies to achieve this:

1.  When a statement is initially prepared, it is first sent to a single
    node in the cluster (this prevents from hitting all nodes in case
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

[setPrepareOnAllHosts]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/QueryOptions.html#setPrepareOnAllHosts(boolean)
[setReprepareOnUp]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/QueryOptions.html#setReprepareOnUp(boolean)
