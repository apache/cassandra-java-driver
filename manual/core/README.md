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

  Session session = cluster.connect();                                                     // (2)

  ResultSet rs = session.execute("select release_version from system.local");              // (3)
  Row row = rs.one();
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

### CQL to Java type mapping

<table border="1" style="text-align:center; width:100%;margin-bottom:1em;">
    <tr> <td><b>CQL3 data type</b></td> <td><b>Getter name</b></td> <td><b>Java type</b></td> </tr>
    <tr> <td>ascii</td> <td>getString</td> <td>java.lang.String</td> </tr>
    <tr> <td>bigint</td> <td>getLong</td> <td>long</td> </tr>
    <tr> <td>blob</td> <td>getBytes</td> <td>java.nio.ByteBuffer</td> </tr>
    <tr> <td>boolean</td> <td>getBoolean</td> <td>boolean</td> </tr>
    <tr> <td>counter</td> <td>getLong</td> <td>long</td> </tr>
    <tr> <td>date</td> <td>getLocalDate</td> <td>java.time.LocalDate</td> </tr>
    <tr> <td>decimal</td> <td>getBigDecimal</td> <td>java.math.BigDecimal</td> </tr>
    <tr> <td>double</td> <td>getDouble</td> <td>double</td> </tr>
    <tr> <td>duration</td> <td>getCqlDuration</td> <td>CqlDuration</td> </tr>
    <tr> <td>float</td> <td>getFloat</td> <td>float</td> </tr>
    <tr> <td>inet</td> <td>getInetAddress</td> <td>java.net.InetAddress</td> </tr>
    <tr> <td>int</td> <td>getInt</td> <td>int</td> </tr>
    <tr> <td>list</td> <td>getList</td> <td>java.util.List<T></td> </tr>
    <tr> <td>map</td> <td>getMap</td> <td>java.util.Map<K, V></td> </tr>
    <tr> <td>set</td> <td>getSet</td> <td>java.util.Set<T></td> </tr>
    <tr> <td>smallint</td> <td>getShort</td> <td>short</td> </tr>
    <tr> <td>text</td> <td>getString</td> <td>java.lang.String</td> </tr>
    <tr> <td>time</td> <td>getLocalTime</td> <td>java.time.LocalTime</td> </tr>
    <tr> <td>timestamp</td> <td>getInstant</td> <td>java.time.Instant</td> </tr>
    <tr> <td>timeuuid</td> <td>getUuid</td> <td>java.util.UUID</td> </tr>
    <tr> <td>tinyint</td> <td>getByte</td> <td>byte</td> </tr>
    <tr> <td>tuple</td> <td>getTupleValue</td> <td>TupleValue</td> </tr>
    <tr> <td>user-defined types</td> <td>getUDTValue</td> <td>UDTValue</td> </tr>
    <tr> <td>uuid</td> <td>getUuid</td> <td>java.util.UUID</td> </tr>
    <tr> <td>varchar</td> <td>getString</td> <td>java.lang.String</td> </tr>
    <tr> <td>varint</td> <td>getVarint</td> <td>java.math.BigInteger</td> </tr>
</table>

Sometimes the driver has to infer a CQL type from a Java type (for example when handling the values 
of [simple statements](statements/simple/)); for those that have multiple CQL equivalents, it makes
the following choices:

* `java.lang.String`: `text`
* `long`: `bigint`
* `java.util.UUID`: `uuid`

[Cluster]:   http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/Cluster.html
[Session]:   http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/session/Session.html
[ResultSet]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/ResultSet.html
[Row]:       http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/Row.html
