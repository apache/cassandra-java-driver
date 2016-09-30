## Frequently Asked Questions

### How do I implement paging?

When using [native protocol](../manual/native_protocol/) version 2 or
higher, the driver automatically pages large result sets under the hood.
You can also save the paging state to resume iteration later. See [this
page](../manual/paging/) for more information.

Native protocol v1 does not support paging, but you can emulate it in
CQL with `LIMIT` and the `token()` function. See
[this conversation](https://groups.google.com/a/lists.datastax.com/d/msg/java-driver-user/U2KzAHruWO4/6vDmUVDDkOwJ) on the mailing list.


### Can I check if a conditional statement (lightweight transaction) was successful?

When executing a conditional statement, the `ResultSet` will contain a single `Row` with a
column named "applied" of type boolean. This tells whether the conditional statement was
successful or not.

The driver provides a convenience method [wasApplied] to check this on the result set directly:

```java
ResultSet rset = session.execute(conditionalStatement);
rset.wasApplied();
```

You may also inspect the value yourself:

```java
ResultSet rset = session.execute(conditionalStatement);
Row row = rset.one();
row.getBool(0);       // this is equivalent row.getBool("applied")
```

Note that, unlike manual inspection, `wasApplied` does not consume the first row.

[wasApplied]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/ResultSet.html#wasApplied--


### What is a parameterized statement and how can I use it?

Starting with Cassandra 2.0, normal statements (that is non-prepared statements) do
not need to concatenate parameter values inside a query string. Instead you can use
`?` markers and provide the values separately:

```java
session.execute( "INSERT INTO contacts (email, firstname, lastname)
      VALUES (?, ?, ?)", "clint.barton@hawkeye.com", "Barney", "Barton");
```

See [Simple statements](../manual/statements/simple/) for more information.


### Does a parameterized statement escape parameters?

A parameterized statement sends the values of parameters separate from the query
(similar to the way a prepared statement does) as bytes so there is no need to escape
parameters.


### What's the difference between a parameterized statement and a Prepared statement?

The only similarity between a parameterized statement and a prepared statement is in
the way that the parameters are sent. The difference is that a prepared statement:

* is already known on the cluster side (it has been compiled and there is an execution
  plan available for it) which leads to better performance
* sends only the statement id and its parameters (thus reducing the amount of data sent
  to the cluster)

See [Prepared statements](../manual/statements/prepared/) for more information.


### Can I combine `PreparedStatements` and normal statements in a batch?

Yes. A batch can include both bound statements and simple statements:

```java
PreparedStatement ps = session.prepare( "INSERT INTO contacts (email, firstname, lastname)
      VALUES (?, ?, ?)");
BatchStatement batch = new BatchStatement();
batch.add(ps.bind(...));
batch.add(ps.bind(...));
// here's a simple statement
batch.add(new SimpleStatement( "INSERT INTO contacts (email, firstname, lastname) VALUES (?, ?, ?)", ...));
session.execute(batch);
```


### Why do my 'SELECT *' `PreparedStatement`-based queries stop working after a schema change?

Both the driver and Cassandra maintain a mapping of `PreparedStatement` queries to their
metadata.  When a change is made to a table, such as a column being added or dropped, there
is currently no mechanism for Cassandra to invalidate the existing metadata.  Because of this,
the driver is not able to properly react to these changes and will improperly read rows after
a schema change is made.

See [Prepared statements](../manual/statements/prepared) for more information.


### Can I get the raw bytes of a text column?

If you need to access the raw bytes of a text column, call the
`Row.getBytesUnsafe("columnName")` method.

Trying to use `Row.getBytes("columnName")` for the same purpose results in an
exception, as the `getBytes` method can only be used if the column has the CQL type `BLOB`.


### How do I increment counters with `QueryBuilder`?

Considering the following query:

```java
UPDATE clickstream SET clicks = clicks + 1 WHERE userid = id;
```

To do this using `QueryBuilder`:

```java
Statement query = QueryBuilder.update("clickstream")
                              .with(incr("clicks", 1)) // Use incr for counters
                              .where(eq("userid", id));
```


### Is there a way to control the batch size of the results returned from a query?

Use the `setFetchSize()` method on your `Statement` object. The fetch size controls
how many resulting rows are retrieved simultaneously (the goal being to avoid
loading too many results in memory for queries yielding large result sets).

Keep in mind that if your code iterates the `ResultSet` entirely, the driver may
run additional background queries to fetch the rest of the data. The fetch size
only affects what is retrieved at a time, not the overall number of rows.

See [Paging](../manual/paging/) for more information.


### What's the difference between using `setFetchSize()` and `LIMIT`?

Basically, `LIMIT` controls the maximum number of results returned by the query,
while the `setFetchSize()` method controls the amount of data transferred at a time.

For example, if you limit is 30 and your fetch size is 10, the `ResultSet` will contain
30 rows, but under the hood the driver will perform 3 requests that will transfer 10
rows each.

See [Paging](../manual/paging/) for more information.


### I'm reading a BLOB column and the driver returns incorrect data.

Check your code to ensure that you read the returned `ByteBuffer` correctly. `ByteBuffer` is a very error-prone API,
and we've had many reports where the problem turned out to be in user code.

See [Blobs.java] in the `driver-examples` module for some examples and explanations.

[Blobs.java]: https://github.com/datastax/java-driver/tree/3.x/driver-examples/src/main/java/com/datastax/driver/examples/datatypes/Blobs.java


### How do I use the driver in an OSGi application?

Read our [OSGi-specific FAQ section](osgi/) to find out.


### Why am I seeing messages about `tombstone_warn_threshold` or `tombstone_fail_threshold` being exceeded in my Cassandra logs?

Applications which use the object mapper or set `null` values in their
statements may observe that many tombstones are being stored in their tables
which subsequently may lead to poor query performance, failed queries, or
columns being mysteriously deleted.

This is caused by INSERT/UPDATE statements containing `null` values for columns
that a user does not intend to change.  Common circumstances around this come
from using the object mapper or writing your own persistence layer and
attempting to reuse the same `PreparedStatement` for inserting data, even with
partial updates.

Prior to cassandra 2.2, there was no means of reusing the same
`PreparedStatement` for making partial updates to different columns.

For example, given the following code:

```java
PreparedStatement prepared = session.prepare("INSERT INTO contacts (email, firstname, lastname) VALUES (?, ?, ?)");
BoundStatement bound = prepared.bind();
bound.set("email", "clint.barton@hawkeye.com");
bound.set("firstname", "Barney");
// creates a tombstone!!
bound.set("lastname", null);
```

If one wanted to use this query to update only `firstname` this would not
be achievable without binding the `lastname` parameter to `null`.  This would
have an undesired side effect of creating a tombstone for `lastname` and thus
to the user giving the impression that `lastname` was deleted.

In cassandra 2.2 and later with protocol v4, bind parameters (`?`) can
optionally be left unset
([CASSANDRA-7304]):

 ```java
 PreparedStatement prepared = session.prepare("INSERT INTO contacts (email, firstname, lastname) VALUES (?, ?, ?)");
 BoundStatement bound = prepared.bind();
 bound.set("email", "clint.barton@hawkeye.com");
 bound.set("firstname", "Barney");
 // lastname is left unset.
 ```

See [Parameters and Binding] for more details about unset parameters.

Another possible root cause for this is using the object mapper and leaving
fields set to `null`.  This also causes tombstones to be inserted unless
setting `saveNullFields` option to false.  See [Mapper options] for more
details.

[Blobs.java]: https://github.com/datastax/java-driver/tree/3.0.x/driver-examples/src/main/java/com/datastax/driver/examples/datatypes/Blobs.java
[CASSANDRA-7304]: https://issues.apache.org/jira/browse/CASSANDRA-7304
[Parameters and Binding]: ../manual/statements/prepared/#parameters-and-binding
[Mapper options]: ../manual/object_mapper/using/#mapper-options
