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

[wasApplied]: http://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/core/ResultSet.html#wasApplied--


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
optionally be left unset.
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
setting `saveNullFields` option to false.
See [Mapper options] for more details.

### I am encountering `BusyPoolException`, what does this mean and how do I avoid it?

Often while writing a bulk loading program or an application that does many concurrent operations a user may encounter
exceptions such as the following:

> com.datastax.driver.core.exceptions.BusyPoolException: [/X.X.X.X] Pool is busy (no available connection and the queue
> has reached its max size 256)

This typically means that your application is submitting too many concurrent requests and not enforcing any limitation
on the number of requests that are being processed at once.  A common mistake users might make is firing off a bunch
of queries using `executeAsync` and not waiting for them to complete before submitting more.

One simple approach to remedying this is to use something like a [Semaphore] to limit the number of concurrent
`executeAsync` requests at a time.  Alternatively, one could submit requests X at a time and collect the returned
`ResultSetFuture`s from `executeAsync` and use [Futures.allAsList] and wait on completion of the resulting future
before submitting the next batch.

See the [Acquisition queue] section of the Pooling section in the manual for explanation of how the driver enqueues
requests when connections are over-utilized.

### What is Netty's native epoll transport and how do I enable or disable it?

Netty provides [native transport libraries](http://netty.io/wiki/native-transports.html) which generally generate less
garbage and improve performance when compared to the default NIO-based transport.
By default if the driver detects the  `netty-transport-native-epoll` library in its classpath it will attempt to use
[`EpollEventLoopGroup`](https://netty.io/4.0/api/io/netty/channel/epoll/EpollEventLoopGroup.html) for its underlying
event loop.

In the usual case this works fine in linux environments.  On the other hand, many users have run into compatibility
issues when the version of `netty-transport-native-epoll` is not compatible with a version of Netty in an application's
classpath.  One such case is where an application depends on a version of `netty-all` that is different than the
version of `netty-handler` that the driver depends on.  In such a case, a user may encounter an exception such as the
one described in [JAVA-1535](https://datastax-oss.atlassian.net/browse/JAVA-1535).

While the epoll transport may in general improve performance, we expect the improvement to be marginal for a lot of use
cases.  Therefore, if you don't want `netty-transport-native-epoll` to be used by the driver even if the library is
present in an application's classpath, the most direct way to disable this is to provide the system property value
`-Dcom.datastax.driver.FORCE_NIO=true` to your application to force the use of the default Netty NIO-based event loop.
If properly used, the following log message will be logged at INFO on startup:

> Found Netty's native epoll transport in the classpath, but NIO was forced through the FORCE_NIO system property.

### Why am I encountering `NoSuchMethodFoundException`, `NoClassDefFoundError`, or `VerifyError`s and how do I avoid them?

Incompatibilities between the java driver and other libraries may cause these exceptions to surface in your
application at runtime.

It could be that an older or newer version of a library that the driver depends on, such as Netty
or Guava, may be present in your application's classpath.  If using Maven or another dependency
management tool, the tool should offer a command, such as `mvn dependency:tree` to identify the dependencies
in your project to help you understand the dependent versions across the various libraries you use in your
project.  You may also want to evaluate your classpath to see if there are multiple JARs present for a library,
but with different versions, which could cause compatibility issues.  In addition, consider evaluating
using the Logback logging framework, which provides the capability to include [packaging data] for classes
in stack traces.

For Netty in particular, the driver offers an alternative artifact that shades its Netty dependency,
allowing you to use newer or older versions of Netty in your application without impacting the driver.
See [Using the shaded JAR] for more details.

Another possibility could be that another library depends on a different version of the driver.
In this case, observe the stacktrace of the exception to see which library is attempting to use
the driver.  To identify compatible versions, check that library's dependency on the driver to understand
what version is compatible.

Finally, some monitoring and agent-based tools such as [DynaTrace] offer solutions that instrument the driver to
observe and record useful metrics such as request rates, latencies and more.  It is possible that the tool
is not compatible with the version of the java driver you are using.  In this case, check to
see if a newer version of that tool is available that works with this version of the driver.  If no such
version is available, you may want to reach out to the maintainer of that tool to request that they provide
an update with compatibility to this driver version.


[Blobs.java]: https://github.com/datastax/java-driver/tree/3.7.2/driver-examples/src/main/java/com/datastax/driver/examples/datatypes/Blobs.java
[CASSANDRA-7304]: https://issues.apache.org/jira/browse/CASSANDRA-7304
[Parameters and Binding]: ../manual/statements/prepared/#parameters-and-binding
[Mapper options]: ../manual/object_mapper/using/#mapper-options
[Acquisition queue]: ../manual/pooling/#acquisition-queue
[Semaphore]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/Semaphore.html
[Futures.allAsList]: https://google.github.io/guava/releases/19.0/api/docs/com/google/common/util/concurrent/Futures.html#allAsList(java.lang.Iterable)
[DynaTrace]: https://www.dynatrace.com/
[packaging data]: https://logback.qos.ch/reasonsToSwitch.html#packagingData
[Using the shaded JAR]: ../manual/shaded_jar
