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

## Performance

This page is intended as a checklist for everything related to driver performance. Most of the
information is already available in other sections of the manual, but it's linked here for
easy reference if you're benchmarking your application or diagnosing performance issues.
 

### Statements

[Statements](../statements/) are some of the driver types you'll use the most. Every request needs
one -- even `session.execute(String)` creates a `SimpleStatement` under the hood.

#### Immutability and builders

Statements are by default implemented with immutable classes: every call to a setter method creates
an intermediary copy. If you have multiple attributes to set, use a builder instead:

```java
SimpleStatement statement = SimpleStatement.builder("SELECT * FROM foo")
    .setPageSize(20)
    .setConsistencyLevel(DefaultConsistencyLevel.QUORUM)
    .setIdempotence(true)
    .build();
```

Also, note that you don't need a driver `Session` to create simple statements: they can be
initialized statically and stored as constants.

#### Prepared statements

[Prepared statements](../statements/prepared) allow Cassandra to cache parsed query strings
server-side, but that's not their only benefit for performance:

* the driver also caches the response metadata, which can then be skipped in subsequent responses.
  This saves bandwidth, as well as the CPU and memory resources required to parse it.
* in some cases, prepared statements set routing information automatically, which allows the driver
  to target the most appropriate replicas.
  
You should use prepared statements for all recurring requests in your application. Simple statements
should only be used for one-off queries, for example some initialization that will be done only once
at startup.

The driver caches prepared statements -- see [CqlSession.prepare(SimpleStatement)] for the fine
print. However, if the query is static, it's still a good practice to cache your `PreparedStatement`
instances to avoid calling `prepare()` every time. One common pattern is to use some sort of DAO
component:

```java
public static class UserDao {

  private final CqlSession session;
  private final PreparedStatement preparedFindById;

  public UserDao(CqlSession session) {
    this.session = session;
    this.preparedFindById = session.prepare("SELECT * FROM user WHERE id = ?");
  }

  public User findById(int id) {
    Row row = session.execute(preparedFindById.bind(id)).one();
    return new User(row.getInt(id), row.getString("first_name"), row.getString("last_name"));
  }
}
```


### Request execution

#### Connection pooling

By default, the driver opens 1 connection per node, and allows 1024 concurrent requests on each
connection. In our experience this is enough for most scenarios. 

If your application generates a very high throughput (hundreds of thousands of requests per second), 
you might want to experiment with different settings. See the [tuning](../pooling/#tuning) section
in the connection pooling page.

#### Compression

Consider [compression](../compression/) if your queries return large payloads; it might help to
reduce network traffic.

#### Timestamp generation

Each query is assigned a [timestamp](../query_timestamps/) to order them relative to each other.

By default, this is done driver-side with
[AtomicTimestampGenerator](../query_timestamps/#atomic-timestamp-generator). This is a very simple
operation so unlikely to be a bottleneck, but note that there are other options, such as a
[thread-local](../query_timestamps/#thread-local-timestamp-generator) variant that creates slightly
less contention, writing your own implementation or letting the server assign timestamps.

#### Tracing

[Tracing](../tracing/) should be used for only a small percentage of your queries. It consumes
additional resources on the server, and fetching each trace requires background requests.

Do not enable tracing for every request; it's a sure way to bring your performance down.

#### Request trackers

[Request trackers](../request_tracker/) are on the hot path (that is, invoked on I/O threads, each
time a request is executed), and users can plug custom implementations.

If you experience throughput issues, check if any trackers are configured, and what they are doing.
They should avoid blocking calls, as well as any CPU-intensive computations.

#### Metrics

Similarly, some of the driver's [metrics](../metrics/) are updated for every request (if the metric
is enabled).

By default, the driver ships with all metrics disabled. Enable them conservatively, and if you're
investigating a performance issue, try disabling them temporarily to check that they are not the
cause.

#### Throttling

[Throttling](../throttling/) can help establish more predictable server performance, by controlling
how much load each driver instance is allowed to put on the cluster. The throttling algorithm itself
incurs a bit of overhead in the driver, but that shouldn't be a problem since the goal is to stay
under reasonable rates in the first place.

If you're debugging an unfamiliar application and experience a throughput plateau, make sure that
it's not caused by a throttler.


### Caching reusable objects

Many driver objects are immutable. If you reuse the same values often, consider caching them in
private fields or constants to alleviate GC pressure.

#### Identifiers

The driver uses [CqlIdentifier] to deal with [case sensitivity](../../case_sensitivity). When you
call methods that take raw strings, the driver generally wraps them under the hood:

```java
session.getMetadata().getKeyspace("inventory"); // shortcut for getKeyspace(CqlIdentifier.fromCql("inventory")

// Caching the identifier:
public static final String INVENTORY_ID = "inventory";

session.getMetadata().getKeyspace(INVENTORY_ID);
```

This also applies to built queries, although it's less important because generally the whole query
can be cached (see below).

Note however that row getters and bound statement setters do **not** wrap their argument: because
those methods are used very often, they handle raw strings with an optimized algorithm that does not
require creating an identifier (the rules are detailed [here][AccessibleByName]).

```java
// No need to extract a CqlIdentifier, raw strings are handled efficiently:
Row row = session.execute("SELECT * FROM user WHERE id = 1").one();
row.getInt("age");

PreparedStatement pst = session.prepare("UPDATE user SET name=:name WHERE id=:id");
pst.bind().setInt("age", 25);
```

#### Type tokens

[GenericType] is used to express complex generic types -- such as
[nested collections](../#collection-types) -- in getters and setters. These objects are immutable
and stateless, so they are good candidates for constants:

```java
public static final GenericType<Set<List<String>>> SET_OF_LIST_OF_STRING = new GenericType<Set<List<String>>>() {};

Set<List<String>> teams = row.get("teams", SET_OF_LIST_OF_STRING);
```

`GenericType` itself already exposes a few of those constants. You can create your own utility class
to store yours.

#### Built queries

Similarly, [built queries](../../query_builder/) are immutable and don't need a reference to a live
driver instance. If you create them statically, they can be stored as constants:

```java
public static final BuildableQuery SELECT_SERVER_VERSION =
    selectFrom("system", "local").column("release_version");
```

Note that you don't necessarily need to extract `CqlIdentifier` constants since the construction
already happens at initialization time.

#### Derived configuration profiles

The configuration API allows you to build [derived profiles](../configuration/#derived-profiles) at
runtime.

```java
DriverExecutionProfile dynamicProfile =
  defaultProfile.withString(
      DefaultDriverOption.REQUEST_CONSISTENCY, DefaultConsistencyLevel.EACH_QUORUM.name());
```

Their use is generally discouraged (you should define profiles statically in the configuration file
as much as possible), but if there's no other way and you reuse them over time, store them instead
of recreating them each time. 

### Metadata

The driver maintains [metadata](../metadata/) about the state of the Cassandra cluster. This work is
done on dedicated "admin" threads (see the [thread pooling](#thread-pooling) section below), so it's
not in direct competition with regular requests. 


#### Filtering

You can disable entire parts of the metadata with those configuration options: 

```
datastax-java-driver.advanced.metadata {
  schema.enabled = true
  token-map.enabled = true
}
```

This will save CPU and memory resources, but you lose some driver features:

* if schema is disabled, `session.getMetadata().getKeyspaces()` will always be empty: your
  application won't be able to inspect the database schema dynamically.
* if the token map is disabled, `session.getMetadata().getTokenMap()` will always be empty, and you
  lose the ability to use [token-aware routing](../load_balancing/#token-aware).

Note that disabling the schema implicitly disables the token map (because computing the token map
requires the keyspace replication settings).

Perhaps more interestingly, metadata can be [filtered](../metadata/schema/#filtering) to a specific
subset of keyspaces. This is handy if you connect to a shared cluster that holds data for multiple
applications:

```
datastax-java-driver.advanced.metadata {
  schema.refreshed-keyspaces = [ "users", "inventory" ]
}
```

To get a sense of the time spent on metadata refreshes, enable [debug logs](../logging/) and look
for entries like this:

```
[s0-io-0] DEBUG c.d.o.d.i.c.m.s.q.CassandraSchemaQueries - [s0] Schema queries took 88 ms
[s0-admin-0] DEBUG c.d.o.d.i.c.m.s.p.CassandraSchemaParser - [s0] Schema parsing took 71 ms
[s0-admin-0] DEBUG c.d.o.d.i.c.metadata.DefaultMetadata - [s0] Refreshing token map (only schema has changed)
[s0-admin-0] DEBUG c.d.o.d.i.c.m.token.DefaultTokenMap - [s0] Computing keyspace-level data for {system_auth={class=org.apache.cassandra.locator.SimpleStrategy, replication_factor=1}, system_schema={class=org.apache.cassandra.locator.LocalStrategy}, system_distributed={class=org.apache.cassandra.locator.SimpleStrategy, replication_factor=3}, system={class=org.apache.cassandra.locator.LocalStrategy}, system_traces={class=org.apache.cassandra.locator.SimpleStrategy, replication_factor=2}}
[s0-admin-0] DEBUG c.d.o.d.i.c.m.token.DefaultTokenMap - [s0] Computing new keyspace-level data for {class=org.apache.cassandra.locator.SimpleStrategy, replication_factor=1}
[s0-admin-0] DEBUG c.d.o.d.i.c.m.token.KeyspaceTokenMap - [s0] Computing keyspace-level data for {class=org.apache.cassandra.locator.SimpleStrategy, replication_factor=1} took 12 ms
[s0-admin-0] DEBUG c.d.o.d.i.c.m.token.DefaultTokenMap - [s0] Computing new keyspace-level data for {class=org.apache.cassandra.locator.LocalStrategy}
[s0-admin-0] DEBUG c.d.o.d.i.c.m.token.KeyspaceTokenMap - [s0] Computing keyspace-level data for {class=org.apache.cassandra.locator.LocalStrategy} took 1 ms
[s0-admin-0] DEBUG c.d.o.d.i.c.m.token.DefaultTokenMap - [s0] Computing new keyspace-level data for {class=org.apache.cassandra.locator.SimpleStrategy, replication_factor=3}
[s0-admin-0] DEBUG c.d.o.d.i.c.m.token.KeyspaceTokenMap - [s0] Computing keyspace-level data for {class=org.apache.cassandra.locator.SimpleStrategy, replication_factor=3} took 54 us
[s0-admin-0] DEBUG c.d.o.d.i.c.m.token.DefaultTokenMap - [s0] Computing new keyspace-level data for {class=org.apache.cassandra.locator.SimpleStrategy, replication_factor=2}
[s0-admin-0] DEBUG c.d.o.d.i.c.m.token.KeyspaceTokenMap - [s0] Computing keyspace-level data for {class=org.apache.cassandra.locator.SimpleStrategy, replication_factor=2} took 98 us
[s0-admin-0] DEBUG c.d.o.d.i.c.metadata.DefaultMetadata - [s0] Rebuilding token map took 32 ms
[s0-admin-0] DEBUG c.d.o.d.i.c.metadata.MetadataManager - [s0] Applying schema refresh took 34 ms
```

#### Debouncing

The driver receives push notifications of schema and topology changes from the Cassandra cluster.
These signals are *debounced*, meaning that rapid series of events will be amortized, for example:

* if multiple schema objects are created or modified, only perform a single schema refresh at the
  end.
* if a node's status oscillates rapidly between UP and DOWN, wait for gossip to stabilize and only
  apply the last state.

Debouncing is controlled by these configuration options (shown here with their defaults):

```
datastax-java-driver.advanced.metadata {
  topology-event-debouncer {
    # How long the driver waits to propagate an event. If another event is received within that
    # time, the window is reset and a batch of accumulated events will be delivered.
    window = 1 second
    
    # The maximum number of events that can accumulate. If this count is reached, the events are
    # delivered immediately and the time window is reset.
    max-events = 20
  }
  schema.debouncer {
    window = 1 second
    max-events = 20
  }
}
```

You may adjust those settings depending on your application's needs: higher values mean less impact
on performance, but the driver will be slower to react to changes.  

#### Schema updates

You should group your schema changes as much as possible.

Every change made from a client will be pushed to all other clients, causing them to refresh their
metadata. If you have multiple client instances, it might be a good idea to
[deactivate the metadata](../metadata/schema/#enabling-disabling) on all clients while you apply the
updates, and reactivate it at the end (reactivating will trigger an immediate refresh, so you might
want to ramp up clients to avoid a "thundering herd" effect).

Schema changes have to replicate to all nodes in the cluster. To minimize the chance of schema
disagreement errors:

* apply your changes serially. The driver handles this automatically by checking for
  [schema agreement](../metadata/schema/#schema-agreement) after each DDL query. Run them from the
  same application thread, and, if you use the asynchronous API, chain the futures properly.
* send all the changes to the same coordinator. This is one of the rare cases where we recommend
  using [Statement.setNode()].  

### Thread pooling

The driver architecture is designed around two code paths:

* the **hot path** is everything directly related to the execution of requests: encoding/decoding
  driver types to/from low-level binary payloads, and network I/O. This is where the driver spends
  most of its cycles in a typical application: when we have to make design tradeoffs, performance is
  always the priority. Hot code runs on 3 categories of threads:
    * your application's thread for the construction of statements;
    * the driver's "I/O" event loop group for encoding/decoding and network I/O. You can configure
      it with the options in `datastax-java-driver.advanced.netty.io-group`.
    * the driver's "timer" thread for request timeouts and speculative executions. See
      `datastax-java-driver.advanced.netty.timer`.
* the **cold path** is for all administrative tasks: managing the
  [control connection](../control_connection), parsing [metadata](../metadata/), reacting to cluster
  events (node going up/down, getting added/removed, etc), and scheduling periodic events
  (reconnections, reloading the configuration). Comparatively, these tasks happen less often, and
  are less critical (for example, stale schema metadata is not a blocker for request execution).
  They are scheduled on a separate "admin" event loop group, controlled by the options in
  `datastax-java-driver.advanced.netty.admin-group`.

By default, the number of I/O threads is set to `Runtime.getRuntime().availableProcessors() * 2`,
and the number of admin threads to 2. It's hard to give one-size-fits-all recommendations because
every case is different, but you might want to try lowering I/O threads, especially if your
application already creates a lot of threads on its side. 

Note that you can gain more fine-grained control over thread pools via the
[internal](../../api_conventions) API (look at the `NettyOptions` interface). In particular, it is
possible to reuse the same event loop group for I/O, admin tasks, and even your application code
(the driver's internal code is fully asynchronous so it will never block any thread). The timer is
the only one that will have to stay on a separate thread.

[AccessibleByName]:                    https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/data/AccessibleByName.html
[CqlIdentifier]:                       https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/CqlIdentifier.html
[CqlSession.prepare(SimpleStatement)]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/CqlSession.html#prepare-com.datastax.oss.driver.api.core.cql.SimpleStatement-
[GenericType]:                         https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/type/reflect/GenericType.html
[Statement.setNode()]:                 https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/Statement.html#setNode-com.datastax.oss.driver.api.core.metadata.Node-
