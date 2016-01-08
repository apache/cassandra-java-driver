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

### More information

If you're reading this from the [generated HTML documentation on
github.io](http://datastax.github.io/java-driver/), use the "Contents"
menu on the left hand side to navigate sub-sections. If you're [browsing the source files on
github.com](https://github.com/datastax/java-driver/tree/2.1/manual),
simply navigate to each sub-directory.

[Cluster]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Cluster.html
[Session]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Session.html
[ResultSet]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/ResultSet.html
[Row]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Row.html