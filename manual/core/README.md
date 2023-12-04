<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Core driver

The core module handles cluster connectivity and request execution. It is published under the
following coordinates:

```xml
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>${driver.version}</version>
</dependency>
```

(For more details on setting up your build tool, see the [integration](integration/) page.)

### Quick start

Here's a short program that connects to Cassandra and executes a query:

```java
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

try (CqlSession session = CqlSession.builder().build()) {                                  // (1)
  ResultSet rs = session.execute("select release_version from system.local");              // (2)
  Row row = rs.one();
  System.out.println(row.getString("release_version"));                                    // (3)
}
```

1. [CqlSession] is the main entry point of the driver. It holds the known state of the actual
   Cassandra cluster, and is what you use to execute queries. It is thread-safe, you should create a
   single instance (per target Cassandra cluster), and share it throughout your application;
2. we use `execute` to send a query to Cassandra. This returns a [ResultSet], which is an iterable 
   of [Row] objects. On the next line, we extract the first row (which is the only one in this case);
3. we extract the value of the first (and only) column from the row.

Always close the `CqlSession` once you're done with it, in order to free underlying resources (TCP 
connections, thread pools...). In this simple example, we can use a try-with-resources block because
`CqlSession` implements `java.lang.AutoCloseable`; in a real application, you'll probably call one
of the close methods (`close`, `closeAsync`, `forceCloseAsync`) explicitly.

This example uses the synchronous API. Most methods have asynchronous equivalents (look for `*Async`
variants that return a `CompletionStage`).


### Setting up the driver

#### [CqlSession]

[CqlSession#builder()] provides a fluent API to create an instance programmatically. Most of the
customization is done through the driver configuration (refer to the
[corresponding section](configuration/) of this manual for full details).

We recommend that you take a look at the [reference configuration](configuration/reference/) for the
list of available options, and cross-reference with the sub-sections in this manual for more
explanations.

##### Contact points

If you don't specify any contact point, the driver defaults to `127.0.0.1:9042`:

```java
CqlSession session = CqlSession.builder().build();
```

This is fine for a quick start on a developer workstation, but you'll quickly want to provide
specific addresses. There are two ways to do this:

* via [SessionBuilder.addContactPoint()] or [SessionBuilder.addContactPoints()];
* in the [configuration](configuration/) via the `basic.contact-points` option.

As soon as there are explicit contact points, you also need to provide the name of the local
datacenter. All contact points must belong to it (as reported in their system tables:
`system.local.data_center` and `system.peers.data_center`). Again this can be specified either:

* via [SessionBuilder.withLocalDatacenter()];
* in the configuration via the `basic.load-balancing-policy.local-datacenter` option.

Here is a full programmatic example:

```java
CqlSession session = CqlSession.builder()
    .addContactPoint(new InetSocketAddress("1.2.3.4", 9042))
    .addContactPoint(new InetSocketAddress("5.6.7.8", 9042))
    .withLocalDatacenter("datacenter1")
    .build();
```

And a full configuration example:

```
// Add `application.conf` to your classpath with the following contents:
datastax-java-driver {
  basic {
    contact-points = [ "1.2.3.4:9042", "5.6.7.8:9042" ]
    load-balancing-policy.local-datacenter = datacenter1
  }
}
```

For more details about the local datacenter, refer to the [load balancing
policy](load_balancing/#local-only) section.

##### Keyspace

By default, a session isn't tied to any specific keyspace. You'll need to prefix table names in your
queries:

```java
session.execute("SELECT * FROM my_keyspace.my_table WHERE id = 1");
```

You can also specify a keyspace at construction time, either through the
[configuration](configuration/):

```
datastax-java-driver {
  basic.session-keyspace = my_keyspace
}
```

Or with the builder:

```java
CqlSession session = CqlSession.builder()
  .withKeyspace(CqlIdentifier.fromCql("my_keyspace"))
  .build();
```

That keyspace will be used as the default when table names are not qualified:

```java
session.execute("SELECT * FROM my_table WHERE id = 1");
session.execute("SELECT * FROM other_keyspace.other_table WHERE id = 1");
```

You might be tempted to open a separate session for each keyspace used in your application; however,
connection pools are created at the session level, so each new session will consume additional
system resources:

```java
// Anti-pattern: creating two sessions doubles the number of TCP connections opened by the driver
CqlSession session1 = CqlSession.builder().withKeyspace(CqlIdentifier.fromCql("ks1")).build();
CqlSession session2 = CqlSession.builder().withKeyspace(CqlIdentifier.fromCql("ks2")).build();
```

If you issue a `USE` statement, it will change the default keyspace on that session:

```java
CqlSession session = CqlSession.builder().build();
// No default keyspace set, need to prefix:
session.execute("SELECT * FROM my_keyspace.my_table WHERE id = 1");

session.execute("USE my_keyspace");
// Now the keyspace is set, unqualified query works:
session.execute("SELECT * FROM my_table WHERE id = 1");
```

Be very careful though: switching the keyspace at runtime is inherently thread-unsafe, so if the
session is shared by multiple threads (and is usually is), it could easily cause unexpected query
failures.

Finally, if you're connecting to Cassandra 4 or above, you can specify the keyspace independently
for each request:

```java
CqlSession session = CqlSession.builder().build();
session.execute(
  SimpleStatement.newInstance("SELECT * FROM my_table WHERE id = 1")
      .setKeyspace(CqlIdentifier.fromCql("my_keyspace")));
``` 

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

| CQL3 data type      | Getter name    | Java type            | See also                            |
|---------------------|----------------|----------------------|-------------------------------------|
| ascii               | getString      | java.lang.String     |                                     |
| bigint              | getLong        | long                 |                                     |
| blob                | getByteBuffer  | java.nio.ByteBuffer  |                                     |
| boolean             | getBoolean     | boolean              |                                     |
| counter             | getLong        | long                 |                                     |
| date                | getLocalDate   | java.time.LocalDate  | [Temporal types](temporal_types/)   |
| decimal             | getBigDecimal  | java.math.BigDecimal |                                     |
| double              | getDouble      | double               |                                     |
| duration            | getCqlDuration | [CqlDuration]        | [Temporal types](temporal_types/)   |
| float               | getFloat       | float                |                                     |
| inet                | getInetAddress | java.net.InetAddress |                                     |
| int                 | getInt         | int                  |                                     |
| list                | getList        | java.util.List<T>    |                                     |
| map                 | getMap         | java.util.Map<K, V>  |                                     |
| set                 | getSet         | java.util.Set<T>     |                                     |
| smallint            | getShort       | short                |                                     |
| text                | getString      | java.lang.String     |                                     |
| time                | getLocalTime   | java.time.LocalTime  | [Temporal types](temporal_types/)   |
| timestamp           | getInstant     | java.time.Instant    | [Temporal types](temporal_types/)   |
| timeuuid            | getUuid        | java.util.UUID       |                                     |
| tinyint             | getByte        | byte                 |                                     |
| tuple               | getTupleValue  | [TupleValue]         | [Tuples](tuples/)                   |
| user-defined types  | getUDTValue    | [UDTValue]           | [User-defined types](udts/)         |
| uuid                | getUuid        | java.util.UUID       |                                     |
| varchar             | getString      | java.lang.String     |                                     |
| varint              | getBigInteger  | java.math.BigInteger |                                     |

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

[CqlSession]:                           https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/CqlSession.html
[CqlSession#builder()]:                 https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/CqlSession.html#builder--
[ResultSet]:                            https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/cql/ResultSet.html
[Row]:                                  https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/cql/Row.html
[CqlIdentifier]:                        https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/CqlIdentifier.html
[AccessibleByName]:                     https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/data/AccessibleByName.html
[GenericType]:                          https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/type/reflect/GenericType.html
[CqlDuration]:                          https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/data/CqlDuration.html
[TupleValue]:                           https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/data/TupleValue.html
[UdtValue]:                             https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/data/UdtValue.html
[SessionBuilder.addContactPoint()]:     https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/session/SessionBuilder.html#addContactPoint-java.net.InetSocketAddress-
[SessionBuilder.addContactPoints()]:    https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/session/SessionBuilder.html#addContactPoints-java.util.Collection-
[SessionBuilder.withLocalDatacenter()]: https://docs.datastax.com/en/drivers/java/4.4/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withLocalDatacenter-java.lang.String-

[CASSANDRA-10145]: https://issues.apache.org/jira/browse/CASSANDRA-10145
