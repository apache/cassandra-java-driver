## Core driver

The core module handles cluster connectivity and request execution. It is published under the
following coordinates:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>4.0.0-alpha1</version>
</dependency>
```

### Quick start

Here's a short program that connects to Cassandra and executes a query:

```java
try (Cluster cluster =
    Cluster.builder().addContactPoint(new InetSocketAddress("127.0.0.1", 9042)).build()) { // (1)

  CqlSession session = cluster.connect();                                                  // (2)

  ResultSet rs = session.execute("select release_version from system.local");              // (3)
  Row row = rs.iterator().next();
  System.out.println(row.getString("release_version"));                                    // (4)
}
```

1. the [Cluster] is the main entry point of the driver. It holds the known state of the actual 
   Cassandra cluster. It is thread-safe, you should create a single instance (per target Cassandra
   cluster), and share it throughout your application;
2. the [Session] is what you use to execute queries. Likewise, it is thread-safe and should be 
   reused;
3. we use `execute` to send a query to Cassandra. This returns a [ResultSet], which is an iterable 
   of [Row] objects. On the next line, we extract the first row (which is the only one in this case);
4. we extract the value of the first (and only) column from the row.

Always close the `Cluster` once you're done with it, in order to free underlying resources (TCP 
connections, thread pools...). Closing a `Cluster` also closes any `Session` that was created from
it. In this simple example, we can use a try-with-resources block because `Cluster` implements
`java.lang.AutoCloseable`; in a real application, you'll probably call one of the close methods 
(`close`, `closeAsync`, `forceCloseAsync`) explicitly.

[Cluster]:   http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/Cluster.html
[Session]:   http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/session/Session.html
[ResultSet]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/ResultSet.html
[Row]:       http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/Row.html
