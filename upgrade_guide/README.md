## Upgrade guide

### 4.1.0

#### Object mapper

4.1.0 marks the introduction of the new object mapper in the 4.x series.

Like driver 3, it relies on annotations to configure mapped entities and queries. However, there are
a few notable differences:

* it uses compile-time annotation processing instead of runtime reflection;
* the "mapper" and "accessor" concepts have been unified into a single "DAO" component, that handles
  both pre-defined CRUD patterns, and user-provided queries.

Refer to the [mapper manual](../manual/mapper/) for all the details.

#### Internal API

`NettyOptions#afterBootstrapInitialized` is now responsible for setting socket options on driver
connections (see `advanced.socket` in the configuration). If you had written a custom `NettyOptions`
for 4.0, you'll have to copy over -- and possibly adapt -- the contents of
`DefaultNettyOptions#afterBootstrapInitialized` (if you didn't override `NettyOptions`, you don't
have to change anything).

### 4.0.0

Version 4 is major redesign of the internal architecture. As such, it is **not binary compatible**
with previous versions. However, most of the concepts remain unchanged, and the new API will look
very familiar to 2.x and 3.x users.

#### New Maven coordinates

The core driver is available from:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>4.0.0</version>
</dependency>
```

#### Runtime requirements

The driver now requires **Java 8 or above**. It does not depend on Guava anymore (we still use it
internally but it's shaded).

We have dropped support for legacy protocol versions v1 and v2. As a result, the driver is
compatible with:

* **Apache Cassandra®: 2.1 and above**;
* **Datastax Enterprise: 4.7 and above**.

#### Packages

We've adopted new [API conventions] to better organize the driver code and make it more modular. As
a result, package names have changed. However most public API types have the same names; you can use
the auto-import or "find class" features of your IDE to discover the new locations.

Here's a side-by-side comparison with the legacy driver for a basic example:

```java
// Driver 3:
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

SimpleStatement statement =
  new SimpleStatement("SELECT release_version FROM system.local");
ResultSet resultSet = session.execute(statement);
Row row = resultSet.one();
System.out.println(row.getString("release_version"));


// Driver 4:
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

SimpleStatement statement =
  SimpleStatement.newInstance("SELECT release_version FROM system.local");
ResultSet resultSet = session.execute(statement);
Row row = resultSet.one();
System.out.println(row.getString("release_version"));
```

Notable changes:

* the imports;
* simple statement instances are now created with the `newInstance` static factory method. This is
  because `SimpleStatement` is now an interface (as most public API types).

[API conventions]: ../manual/api_conventions

#### Configuration

The configuration has been completely revamped. Instead of ad-hoc configuration classes, the default
mechanism is now file-based, using the [Typesafe Config] library. This is a better choice for most
deployments, since it allows configuration changes without recompiling the client application (note
that there are still programmatic setters for things that are likely to be injected dynamically,
such as contact points).

The driver JAR contains a `reference.conf` file that defines the options with their defaults:

```
datastax-java-driver {
  basic.request {
    timeout = 2 seconds
    consistency = LOCAL_ONE
    page-size = 5000
  }
  // ... and many more (~10 basic options, 70 advanced ones)
}
```

You can place an `application.conf` in your application's classpath to override options selectively:

```
datastax-java-driver {
  basic.request.consistency = ONE
}
```

Options can also be overridden with system properties when launching your application:

```
java -Ddatastax-java-driver.basic.request.consistency=ONE MyApp
``` 

The configuration also supports *execution profiles*, that allow you to capture and reuse common
sets of options:

```java
// application.conf:
datastax-java-driver {
  profiles {
    profile1 { basic.request.consistency = QUORUM }
    profile2 { basic.request.consistency = ONE }
  }
}

// Application code:
SimpleStatement statement1 =
  SimpleStatement.newInstance("...").setExecutionProfileName("profile1");
SimpleStatement statement2 =
  SimpleStatement.newInstance("...").setExecutionProfileName("profile2");
```

The configuration can be reloaded periodically at runtime:

```
datastax-java-driver {
  basic.config-reload-interval = 5 minutes
}
```

This is fully customizable: the configuration is exposed to the rest of the driver as an abstract
`DriverConfig` interface; if the default implementation doesn't work for you, you can write your
own.

For more details, refer to the [manual](../manual/core/configuration).

[Typesafe Config]: https://github.com/typesafehub/config

#### Session

`Cluster` does not exist anymore; the session is now the main component, initialized in a single
step:

```java
CqlSession session = CqlSession.builder().build();
session.execute("...");
```

Protocol negotiation in mixed clusters has been improved: you no longer need to force the protocol
version during a rolling upgrade. The driver will detect that there are older nodes, and downgrade
to the best common denominator (see
[JAVA-1295](https://datastax-oss.atlassian.net/browse/JAVA-1295)).

Reconnection is now possible at startup: if no contact point is reachable, the driver will retry at
periodic intervals (controlled by the [reconnection policy](../manual/core/reconnection/)) instead
of throwing an error. To turn this on, set the following configuration option:

```
datastax-java-driver {
  advanced.reconnect-on-init = true
}
```

The session now has a built-in [throttler](../manual/core/throttling/) to limit how many requests
can execute concurrently. Here's an example based on the number of requests (a rate-based variant is
also available):

```
datastax-java-driver {
  advanced.throttler {
    class = ConcurrencyLimitingRequestThrottler
    max-concurrent-requests = 10000
    max-queue-size = 100000
  }
}
```

#### Load balancing policy

Previous driver versions came with multiple load balancing policies that could be nested into each
other. In our experience, this was one of the most complicated aspects of the configuration.

In driver 4, we are taking a more opinionated approach: we provide a single [default
policy](../manual/core/load_balancing/#default-policy), with what we consider as the best practices:

* local only: we believe that failover should be handled at infrastructure level, not by application
  code.
* token-aware.
* optionally filtering nodes with a custom predicate.

You can still provide your own policy by implementing the `LoadBalancingPolicy` interface.

#### Statements

Simple, bound and batch [statements](../manual/core/statements/) are now exposed in the public API
as interfaces. The internal implementations are **immutable**. This makes them automatically
thread-safe: you don't need to worry anymore about sharing them or reusing them between asynchronous
executions.

Note that all mutating methods return a new instance, so **make sure you don't accidentally ignore
their result**:

```java
BoundStatement boundSelect = preparedSelect.bind();

// This doesn't work: setInt doesn't modify boundSelect in place:
boundSelect.setInt("k", key);
session.execute(boundSelect);

// Instead, reassign the statement every time:
boundSelect = boundSelect.setInt("k", key);
```

These methods are annotated with `@CheckReturnValue`. Some code analysis tools -- such as
[ErrorProne](https://errorprone.info/) -- can check correct usage at build time, and report mistakes
as compiler errors.

Unlike 3.x, the request timeout now spans the <em>entire</em> request. In other words, it's the
maximum amount of time that `session.execute` will take, including any retry, speculative execution,
etc. You can set it with `Statement.setTimeout`, or globally in the configuration with the
`basic.request.timeout` option.

[Prepared statements](../manual/core/statements/prepared/) are now cached client-side: if you call
`session.prepare()` twice with the same query string, it will no longer log a warning. The second
call will return the same statement instance, without sending anything to the server:

```java
PreparedStatement ps1 = session.prepare("SELECT * FROM product WHERE sku = ?");
PreparedStatement ps2 = session.prepare("SELECT * FROM product WHERE sku = ?");
assert ps1 == ps2;
```

This cache takes into account all execution parameters. For example, if you prepare the same query
string with different consistency levels, you will get two distinct prepared statements, each
propagating its own consistency level to its bound statements:

```java
PreparedStatement ps1 =
  session.prepare(
      SimpleStatement.newInstance("SELECT * FROM product WHERE sku = ?")
          .setConsistencyLevel(DefaultConsistencyLevel.ONE));
PreparedStatement ps2 =
  session.prepare(
      SimpleStatement.newInstance("SELECT * FROM product WHERE sku = ?")
          .setConsistencyLevel(DefaultConsistencyLevel.TWO));

assert ps1 != ps2;

BoundStatement bs1 = ps1.bind();
assert bs1.getConsistencyLevel() == DefaultConsistencyLevel.ONE;

BoundStatement bs2 = ps2.bind();
assert bs2.getConsistencyLevel() == DefaultConsistencyLevel.TWO;
```

DDL statements are now debounced; see [Why do DDL queries have a higher latency than driver
3?](../faq/#why-do-ddl-queries-have-a-higher-latency-than-driver-3) in the FAQ.

#### Dual result set APIs

In 3.x, both synchronous and asynchronous execution models shared a common result set
implementation. This made asynchronous usage [notably error-prone][3.x async paging], because of the
risk of accidentally triggering background synchronous fetches.

There are now two separate APIs: synchronous queries return a `ResultSet`; asynchronous queries 
return a future of `AsyncResultSet`.

`ResultSet` behaves much like its 3.x counterpart, except that background pre-fetching
(`fetchMoreResults`) was deliberately removed, in order to keep this interface simple and intuitive.
If you were using synchronous iterations with background pre-fetching, you should now switch to
fully asynchronous iterations (see below).

`AsyncResultSet` is a simplified type that only contains the rows of the current page. When 
iterating asynchronously, you no longer need to stop the iteration manually: just consume all the 
rows in `currentPage()`, and then call `fetchNextPage` to retrieve the next page asynchronously. You
will find more information about asynchronous iterations in the manual pages about [asynchronous 
programming][4.x async programming] and [paging][4.x paging].

[3.x async paging]: http://docs.datastax.com/en/developer/java-driver/3.2/manual/async/#async-paging
[4.x async programming]: ../manual/core/async/
[4.x paging]: ../manual/core/paging/

#### CQL to Java type mappings

Since the driver now has access to Java 8 types, some of the [CQL to Java type mappings] have
changed when it comes to [temporal types] such as `date` and `timestamp`:

* `getDate` has been replaced by `getLocalDate` and returns [java.time.LocalDate];
* `getTime` has been replaced by `getLocalTime` and returns [java.time.LocalTime] instead of a
  `long` representing nanoseconds since midnight;
* `getTimestamp` has been replaced by `getInstant` and returns [java.time.Instant] instead of
  [java.util.Date].

The corresponding setter methods were also changed to expect these new types as inputs.

[CQL to Java type mappings]: ../manual/core#cql-to-java-type-mapping
[temporal types]: ../manual/core/temporal_types
[java.time.LocalDate]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDate.html
[java.time.LocalTime]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalTime.html
[java.time.Instant]: https://docs.oracle.com/javase/8/docs/api/java/time/Instant.html
[java.util.Date]: https://docs.oracle.com/javase/8/docs/api/java/util/Date.html

#### Metrics

[Metrics](../manual/core/metrics/) are now divided into two categories: session-wide and per-node. 
Each metric can be enabled or disabled individually in the configuration:

```
datastax-java-driver {
  advanced.metrics {
    // more are available, see reference.conf for the full list
    session.enabled = [ bytes-sent, bytes-received, cql-requests ]
    node.enabled = [ bytes-sent, bytes-received, pool.in-flight ]
  }
}
```

Note that unlike 3.x, JMX is not supported out of the box. You'll need to add the dependency
explicitly:

```xml
<dependency>
  <groupId>io.dropwizard.metrics</groupId>
  <artifactId>metrics-jmx</artifactId>
  <version>4.0.2</version>
</dependency>
```

#### Metadata

`Session.getMetadata()` is now immutable and updated atomically. The node list, schema metadata and
token map exposed by a given `Metadata` instance are guaranteed to be in sync. This is convenient
for analytics clients that need a consistent view of the cluster at a given point in time; for
example, a keyspace in `metadata.getKeyspaces()` will always have a corresponding entry in
`metadata.getTokenMap()`.

On the other hand, this means you have to call `getMetadata()` again each time you need a fresh
copy; do not cache the result:

```java
Metadata metadata = session.getMetadata();
Optional<KeyspaceMetadata> ks = metadata.getKeyspace("test");
assert !ks.isPresent();

session.execute(
  "CREATE KEYSPACE IF NOT EXISTS test "
      + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

// This is still the same metadata from before the CREATE
ks = metadata.getKeyspace("test");
assert !ks.isPresent();

// You need to fetch the whole metadata again
metadata = session.getMetadata();
ks = metadata.getKeyspace("test");
assert ks.isPresent();
```

Refreshing the metadata can be CPU-intensive, in particular the token map. To help alleviate that,
it can now be filtered to a subset of keyspaces. This is useful if your application connects to a
shared cluster, but does not use the whole schema: 

```
datastax-java-driver {
  // defaults to empty (= all keyspaces)
  advanced.metadata.schema.refreshed-keyspaces = [ "users", "products" ]
}
```

See the [manual](../manual/core/metadata/) for all the details.

#### Query builder

The query builder is now distributed as a separate artifact:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-query-builder</artifactId>
  <version>4.0.0</version>
</dependency>
```

It is more cleanly separated from the core driver, and only focuses on query string generation.
Built queries are no longer directly executable, you need to convert them into a string or a
statement:

```java
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

BuildableQuery query =
    insertInto("user")
        .value("id", bindMarker())
        .value("first_name", bindMarker())
        .value("last_name", bindMarker());

String cql = query.asCql();
// INSERT INTO user (id,first_name,last_name) VALUES (?,?,?)

SimpleStatement statement = query
    .builder()
    .addNamedValue("id", 0)
    .addNamedValue("first_name", "Jane")
    .addNamedValue("last_name", "Doe")
    .build();
```

All query builder types are immutable, making them inherently thread-safe and share-safe.

The query builder has its own [manual chapter](../manual/query_builder/), where the syntax is
covered in detail.

#### Dedicated type for CQL identifiers

Instead of raw strings, the names of schema objects (keyspaces, tables, columns, etc.) are now 
wrapped in a dedicated `CqlIdentifier` type. This avoids ambiguities with regard to [case
sensitivity](../manual/case_sensitivity).

#### Pluggable request execution logic

`Session` is now a high-level abstraction capable of executing arbitrary requests. Out of the box,
the driver exposes a more familiar subtype `CqlSession`, that provides familiar signatures for CQL
queries (`execute(Statement)`, `prepare(String)`, etc).

However, the request execution logic is completely pluggable, and supports arbitrary request types
(as long as you write the boilerplate to convert them to protocol messages).

We use that in our DSE driver to implement a reactive API and support for DSE graph. You can also
take advantage of it to plug your own request types (if you're interested, take a look at
`RequestProcessor` in the internal API).
