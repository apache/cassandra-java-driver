## Manual

### Quick start

Here's a short program that connects to Cassandra and executes a query:

```java
Cluster cluster = null;
try {
    cluster = Cluster.builder()                                                    // (1)
            .addContactPoint("127.0.0.1")
            .build();
    Session session = cluster.connect();                                           // (2)

    ResultSet rs = session.execute("select release_version from system.local");    // (3)
    Row row = rs.one();
    System.out.println(row.getString("release_version"));                          // (4)
} finally {
    if (cluster != null) cluster.close();                                          // (5)
}
```

1. the [Cluster] object is the main entry point of the driver. It holds the known state of the actual Cassandra cluster
   (notably the [Metadata](metadata/)). This class is thread-safe, you should create a single instance (per target
   Cassandra cluster), and share it throughout your application;
2. the [Session] is what you use to execute queries. Likewise, it is thread-safe and should be reused;
3. we use `execute` to send a query to Cassandra. This returns a [ResultSet], which is essentially a collection of [Row]
   objects. On the next line, we extract the first row (which is the only one in this case);
4. we extract the value of the first (and only) column from the row;
5. finally, we close the cluster after we're done with it. This will also close any session that was created from this
   cluster. This step is important because it frees underlying resources (TCP connections, thread pools...). In a real
   application, you would typically do this at shutdown (for example, when undeploying your webapp).

Note: this example uses the synchronous API. Most methods have [asynchronous](async/) equivalents.


### Setting up the driver

#### [Cluster]

##### Creating an instance

The simplest approach is to do it programmatically with [Cluster.Builder], which provides a fluent API:

```java
Cluster cluster = Cluster.builder()
        .withClusterName("myCluster")
        .addContactPoint("127.0.0.1")
        .build();
```

Alternatively, you might want to retrieve the settings from an external source (like a properties file or a web
service). You'll need to provide an implementation of [Initializer] that loads these settings:

```java
Initializer myInitializer = ... // your implementation
Cluster cluster = Cluster.buildFrom(myInitializer);
```

##### Creation options

The only required option is the list of contact points, i.e. the hosts that the driver will initially contact to
discover the cluster topology. You can provide a single contact point, but it is usually a good idea to provide more, so
that the driver can fallback if the first one is down.

The other aspects that you can configure on the `Cluster` are:

* [address translation](address_resolution/);
* [authentication](auth/);
* [compression](compression/);
* [load balancing](load_balancing/);
* [metrics](metrics/);
* low-level [Netty configuration][NettyOptions];
* [query options][QueryOptions];
* [reconnections](reconnection/);
* [retries](retries/);
* [socket options][SocketOptions];
* [SSL](ssl/);
* [speculative executions](speculative_execution/);
* [query timestamps](query_timestamps/).

In addition, you can register various types of listeners to be notified of cluster events; see [Host.StateListener],
[LatencyTracker], and [SchemaChangeListener].

##### Cluster initialization

A freshly-built `Cluster` instance does not initialize automatically; that will be triggered by one of the following
actions:

* an explicit call to `cluster.init()`;
* a call to `cluster.getMetadata()`;
* creating a session with `cluster.connect()` or one of its variants;
* calling `session.init()` on a session that was created with `cluster.newSession()`.

The initialization sequence is the following:

* initialize internal state (thread pools, utility components, etc.);
* try to connect to each of the contact points in sequence. The order is not deterministic (in fact, the driver shuffles
  the list to avoid hotspots if a large number of clients share the same contact points). If no contact point replies,
  a [NoHostAvailableException] is thrown and the process stops here;
* otherwise, the successful contact point is elected as the [control host](control_connection/). The driver negotiates
  the [native protocol version](native_protocol/) with it, and queries its system tables to discover the addresses of
  the other hosts.

Note that, at this stage, only the control connection has been established. Connections to other hosts will only be
opened when a session gets created.

#### [Session]

By default, a session isn't tied to any specific keyspace. You'll need to prefix table names in your queries:

```java
Session session = cluster.connect();
session.execute("select * from myKeyspace.myTable where id = 1");
```

You can also specify a keyspace name at construction time, it will be used as the default when table names are not
qualified:

```java
Session session = cluster.connect("myKeyspace");
session.execute("select * from myTable where id = 1");
session.execute("select * from otherKeyspace.otherTable where id = 1");
```

You might be tempted to open a separate session for each keyspace used in your application; however, note that
[connection pools](pooling/) are created at the session level, so each new session will consume additional system
resources:

```java
// Warning: creating two sessions doubles the number of TCP connections opened by the driver
Session session1 = cluster.connect("ks1");
Session session2 = cluster.connect("ks2");
```

Also, there is currently a [known limitation](async/#known-limitations) with named sessions, that causes the driver to
unexpectedly block the calling thread in certain circumstances; if you use a fully asynchronous model, you should use a
session with no keyspace.

Finally, if you issue a `USE` statement, it will change the default keyspace on that session:

```java
Session session = cluster.connect();
// No default keyspace set, need to prefix:
session.execute("select * from myKeyspace.myTable where id = 1");

session.execute("USE myKeyspace");
// Now the keyspace is set, unqualified query works:
session.execute("select * from myTable where id = 1");
```

Be very careful though: if the session is shared by multiple threads, switching the keyspace at runtime could easily
cause unexpected query failures.

Generally, the recommended approach is to use a single session with no keyspace, and prefix all your queries.


### More information

If you're reading this from the [generated HTML documentation on
github.io](http://datastax.github.io/java-driver/), use the "Contents"
menu on the left hand side to navigate sub-sections. If you're [browsing the source files on
github.com](https://github.com/datastax/java-driver/tree/2.1/manual),
simply navigate to each sub-directory.

[Cluster]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Cluster.html
[Cluster.Builder]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Cluster.Builder.html
[Initializer]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Cluster.Initializer.html
[Session]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Session.html
[ResultSet]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/ResultSet.html
[Row]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Row.html
[NettyOptions]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/NettyOptions.html
[QueryOptions]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/QueryOptions.html
[SocketOptions]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/SocketOptions.html
[Host.StateListener]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Host.StateListener.html
[LatencyTracker]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/LatencyTracker.html
[SchemaChangeListener]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/SchemaChangeListener.html
[NoHostAvailableException]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/exceptions/NoHostAvailableException.html