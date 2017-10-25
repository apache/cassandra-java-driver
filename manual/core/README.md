## Core driver

The core module handles cluster connectivity and request execution. It is published under the
following coordinates:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>4.0.0-alpha2</version>
</dependency>
```

### Quick start

Here's a short program that connects to Cassandra and executes a query:

```java
try (Cluster<CqlSession> cluster = Cluster.builder().build()) {                            // (1)

  CqlSession session = cluster.connect();                                                  // (2)

  ResultSet rs = session.execute("select release_version from system.local");              // (3)
  Row row = rs.one();
  System.out.println(row.getString("release_version"));                                    // (4)
}
```

1. [Cluster] is the main entry point of the driver. It holds the known state of the actual Cassandra 
   cluster. It is thread-safe, you should create a single instance (per target Cassandra cluster),
    and share it throughout your application;
2. [CqlSession] is what you use to execute queries. Likewise, it is thread-safe and should be 
   reused;
3. we use `execute` to send a query to Cassandra. This returns a [ResultSet], which is an iterable 
   of [Row] objects. On the next line, we extract the first row (which is the only one in this case);
4. we extract the value of the first (and only) column from the row.

Always close the `Cluster` once you're done with it, in order to free underlying resources (TCP 
connections, thread pools...). Closing a `Cluster` also closes any `CqlSession` that was created
from it. In this simple example, we can use a try-with-resources block because `Cluster` implements
`java.lang.AutoCloseable`; in a real application, you'll probably call one of the close methods 
(`close`, `closeAsync`, `forceCloseAsync`) explicitly.

Note: this example uses the synchronous API. Most methods have asynchronous equivalents (look for
`*Async` variants that return a `CompletionStage`).


### Setting up the driver

#### [Cluster]

[Cluster#builder()] provides a fluent API to create an instance programmatically. Most of the
customization is done through the driver configuration (refer to the
[corresponding section](configuration/) of this manual for full details).

We recommend that you take a look at the `reference.conf` file bundled with the driver for the list
of available options, and cross-reference with the sub-sections in this manual for more
explanations.

#### [CqlSession]

By default, a session isn't tied to any specific keyspace. You'll need to prefix table names in your
queries:

```java
CqlSession session = cluster.connect();
session.execute("SELECT * FROM myKeyspace.myTable WHERE id = 1");
```

You can also specify a keyspace name at construction time, it will be used as the default when table
names are not qualified:

```java
CqlSession session = cluster.connect(CqlIdentifier.fromCql("myKeyspace"));
session.execute("SELECT * FROM myTable WHERE id = 1");
session.execute("SELECT * FROM otherKeyspace.otherTable WHERE id = 1");
```

You might be tempted to open a separate session for each keyspace used in your application; however,
connection pools are created at the session level, so each new session will consume additional
system resources:

```java
// Warning: creating two sessions doubles the number of TCP connections opened by the driver
CqlSession session1 = cluster.connect(CqlIdentifier.fromCql("ks1"));
CqlSession session2 = cluster.connect(CqlIdentifier.fromCql("ks2"));
```

If you issue a `USE` statement, it will change the default keyspace on that session:

```java
CqlSession session = cluster.connect();
// No default keyspace set, need to prefix:
session.execute("SELECT * FROM myKeyspace.myTable WHERE id = 1");

session.execute("USE myKeyspace");
// Now the keyspace is set, unqualified query works:
session.execute("SELECT * FROM myTable WHERE id = 1");
```

Be very careful though: switching the keyspace at runtime is inherently thread-unsafe, so if the
session is shared by multiple threads (and is usually is), it could easily cause unexpected query
failures.

Finally, [CASSANDRA-10145] \(coming in Cassandra 4) will allow specifying the keyspace on a per
query basis instead of relying on session state, which should greatly simplify multiple keyspace
handling. 

### Running queries

You run queries with the session's `execute*` methods:

```java
ResultSet rs = session.execute("SELECT release_version FROM system.local");
```

As shown here, the simplest form is to pass a query string directly. You can also pass a
[Statement](statements/) instance.

#### Processing rows

Executing a query produces a [ResultSet], which is an iterable of [Row]. The basic way to process
all rows is to use Java's for-each loop:

```java
for (Row row : rs) {
    // process the row
}
```

This will return **all results** without limit (even though the driver might use multiple queries in
the background). To handle large result sets, you might want to use a `LIMIT` clause in your CQL
query, or use one of the techniques described in the [paging](paging/) documentation.

When you know that there is only one row (or are only interested in the first one), the driver
provides a convenience method:

```java
Row row = rs.one();
```

#### Reading columns

[Row] provides getters to extract column values; they can be either positional or named:

```java
Row row = session.execute("SELECT first_name, last_name FROM users WHERE id = 1").one();

// The two are equivalent:
String firstName = row.getString(0);
String firstName = row.getString(CqlIdentifier.fromCql("first_name"));
```

[CqlIdentifier] is a string wrapper that deals with case-sensitivity. If you don't want to create an
instance for each getter call, the driver also provides convenience methods that take a raw string:

```java
String firstName = row.getString("first_name");
```

See [AccessibleByName] for an explanation of the conversion rules.

##### CQL to Java type mapping

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

In addition to these default mappings, you can register your own types with
[custom codecs](custom_codecs/).

##### Primitive types

For performance reasons, the driver uses primitive Java types wherever possible (`boolean`,
`int`...); the CQL value `NULL` is encoded as the type's default value (`false`, `0`...), which can
be ambiguous. To distinguish `NULL` from actual values, use `isNull`:

```java
Integer age = row.isNull("age") ? null : row.getInt("age");
```
##### Collection types

To ensure type safety, collection getters are generic. You need to provide type parameters matching
your CQL type when calling the methods:

```java
// Assuming given_names is a list<text>:
List<String> givenNames = row.getList("given_names", String.class);
```

For nested collections, element types are generic and cannot be expressed as Java `Class` instances.
Use [GenericType] instead:

```java
// Assuming teams is a set<list<text>>:
GenericType<Set<List<String>>> listOfStrings = new GenericType<Set<List<String>>>() {};
Set<List<String>> teams = row.get("teams", listOfStrings);
```

Since generic types are anonymous inner classes, it's recommended to store them as constants in a
utility class instead of re-creating them each time.

##### Row metadata

[ResultSet] and [Row] expose an API to explore the column metadata at runtime:

```java
for (ColumnDefinitions.Definition definition : row.getColumnDefinitions()) {
    System.out.printf("Column %s has type %s%n",
            definition.getName(),
            definition.getType());
}
```

[Cluster]:           http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/Cluster.html
[Cluster#builder()]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/Cluster.html#builder--
[CqlSession]:        http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/CqlSession.html
[ResultSet]:         http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/ResultSet.html
[Row]:               http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/Row.html
[CqlIdentifier]:     http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/CqlIdentifier.html
[AccessibleByName]:  http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/data/AccessibleByName.html
[GenericType]:       http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/type/reflect/GenericType.html

[CASSANDRA-10145]: https://issues.apache.org/jira/browse/CASSANDRA-10145