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

## Upgrade guide

### 4.18.1

#### Keystore reloading in DefaultSslEngineFactory

`DefaultSslEngineFactory` now includes an optional keystore reloading interval, for detecting changes in the local
client keystore file. This is relevant in environments with mTLS enabled and short-lived client certificates, especially
when an application restart might not always happen between a new keystore becoming available and the previous
keystore certificate expiring.

This feature is disabled by default for compatibility. To enable, see `keystore-reload-interval` in `reference.conf`.

### 4.17.0

#### Support for Java17

With the completion of [JAVA-3042](https://datastax-oss.atlassian.net/browse/JAVA-3042) the driver now passes our automated test matrix for Java Driver releases.
If you discover an issue with the Java Driver running on Java 17, please let us know. We will triage and address Java 17 issues.

#### Updated API for vector search

The 4.16.0 release introduced support for the CQL `vector` datatype. This release modifies the `CqlVector`
value type used to represent a CQL vector to make it easier to use.  `CqlVector` now implements the Iterable interface
as well as several methods modelled on the JDK's List interface. For more, see
[JAVA-3060](https://datastax-oss.atlassian.net/browse/JAVA-3060). 

The builder interface was replaced with factory methods that resemble similar methods on `CqlDuration`.
For example, the following code will create a keyspace and table, populate that table with some data, and then execute
a query that will return a `vector` type.  This data is retrieved directly via `Row.getVector()` and the resulting
`CqlVector` value object can be interrogated directly.

```java
try (CqlSession session = new CqlSessionBuilder().withLocalDatacenter("datacenter1").build()) {

    session.execute("DROP KEYSPACE IF EXISTS test");
    session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    session.execute("CREATE TABLE test.foo(i int primary key, j vector<float, 3>)");
    session.execute("CREATE CUSTOM INDEX ann_index ON test.foo(j) USING 'StorageAttachedIndex'");
    session.execute("INSERT INTO test.foo (i, j) VALUES (1, [8, 2.3, 58])");
    session.execute("INSERT INTO test.foo (i, j) VALUES (2, [1.2, 3.4, 5.6])");
    session.execute("INSERT INTO test.foo (i, j) VALUES (5, [23, 18, 3.9])");
    ResultSet rs=session.execute("SELECT j FROM test.foo WHERE j ann of [3.4, 7.8, 9.1] limit 1");
    for (Row row : rs){
        CqlVector<Float> v = row.getVector(0, Float.class);
        System.out.println(v);
        if (Iterables.size(v) != 3) {
            throw new RuntimeException("Expected vector with three dimensions");
        }
    }
}
```

You can also use the `CqlVector` type with prepared statements:

```java
PreparedStatement preparedInsert = session.prepare("INSERT INTO test.foo (i, j) VALUES (?,?)");
CqlVector<Float> vector = CqlVector.newInstance(1.4f, 2.5f, 3.6f);
session.execute(preparedInsert.bind(3, vector));
```

In some cases, it makes sense to access the vector directly as an array of some numerical type. This version
supports such use cases by providing a codec which translates a CQL vector to and from a primitive array. Only float arrays are supported. 
You can find more information about this codec in the manual documentation on [custom codecs](../manual/core/custom_codecs/)

### 4.15.0

#### CodecNotFoundException now extends DriverException

Before [JAVA-2995](https://datastax-oss.atlassian.net/browse/JAVA-2995), `CodecNotFoundException`
was extending `RuntimeException`. This is a discrepancy as all other exceptions extend
`DriverException`, which in turn extends `RuntimeException`.

This was causing integrators to do workarounds in order to react on all exceptions correctly.

The change introduced by JAVA-2995 shouldn't be a problem for most users. But if your code was using
a logic such as below, it won't compile anymore:

```java
try {
    doSomethingWithDriver();
} catch(DriverException e) {
} catch(CodecNotFoundException e) { 
}
```

You need to either reverse the catch order and catch `CodecNotFoundException` first:

```java
try {
    doSomethingWithDriver();
} catch(CodecNotFoundException e) { 
} catch(DriverException e) {
}
```

Or catch only `DriverException`:

```java
try {
    doSomethingWithDriver();
} catch(DriverException e) { 
}
```

### 4.14.0

#### AllNodesFailedException instead of NoNodeAvailableException in certain cases

[JAVA-2959](https://datastax-oss.atlassian.net/browse/JAVA-2959) changed the behavior for when a
request cannot be executed because all nodes tried were busy. Previously you would get back a
`NoNodeAvailableException` but you will now get back an `AllNodesFailedException` where the
`getAllErrors` map contains a `NodeUnavailableException` for that node.

#### Esri Geometry dependency now optional

Previous versions of the Java Driver defined a mandatory dependency on the Esri geometry library.
This library offered support for primitive geometric types supported by DSE.  As of driver 4.14.0
this dependency is now optional.

If you do not use DSE (or if you do but do not use the support for geometric types within DSE) you
should experience no disruption.  If you are using geometric types with DSE you'll now need to
explicitly declare a dependency on the Esri library:

```xml
<dependency>
  <groupId>com.esri.geometry</groupId>
  <artifactId>esri-geometry-api</artifactId>
  <version>${esri.version}</version>
</dependency>
```

See the [integration](../manual/core/integration/#esri) section in the manual for more details.

### 4.13.0

#### Enhanced support for GraalVM native images 

[JAVA-2940](https://datastax-oss.atlassian.net/browse/JAVA-2940) introduced an enhanced support for
building GraalVM native images. 

If you were building a native image for your application, please verify your native image builder
configuration. Most of the extra configuration required until now is likely to not be necessary
anymore.

Refer to this [manual page](../manual/core/graalvm) for details.

#### Registration of multiple listeners and trackers

[JAVA-2951](https://datastax-oss.atlassian.net/browse/JAVA-2951) introduced the ability to register
more than one instance of the following interfaces:

* [RequestTracker](https://docs.datastax.com/en/drivers/java/4.12/com/datastax/oss/driver/api/core/tracker/RequestTracker.html)
* [NodeStateListener](https://docs.datastax.com/en/drivers/java/4.12/com/datastax/oss/driver/api/core/metadata/NodeStateListener.html)
* [SchemaChangeListener](https://docs.datastax.com/en/drivers/java/4.12/com/datastax/oss/driver/api/core/metadata/schema/SchemaChangeListener.html)

Multiple components can now be registered both programmatically and through the configuration. _If
both approaches are used, components will add up and will all be registered_ (whereas previously,
the programmatic approach would take precedence over the configuration one).

When using the programmatic approach to register multiple components, you should use the new
`SessionBuilder` methods `addRequestTracker`, `addNodeStateListener` and  `addSchemaChangeListener`:

```java
CqlSessionBuilder builder = CqlSession.builder();
builder
    .addRequestTracker(tracker1)
    .addRequestTracker(tracker2);
builder
    .addNodeStateListener(nodeStateListener1)
    .addNodeStateListener(nodeStateListener2);
builder
    .addSchemaChangeListener(schemaChangeListener1)
    .addSchemaChangeListener(schemaChangeListener2);
```

To support registration of multiple components through the configuration, the following
configuration options were deprecated because they only allow one component to be declared:

* `advanced.request-tracker.class`
* `advanced.node-state-listener.class`
* `advanced.schema-change-listener.class`

They are still honored, but the driver will log a warning if they are used. They should now be
replaced with the following ones, that accept a list of classes to instantiate, instead of just
one:

* `advanced.request-tracker.classes`
* `advanced.node-state-listener.classes`
* `advanced.schema-change-listener.classes`

Example:

```hocon
datastax-java-driver {
  advanced {
    # RequestLogger is a driver built-in tracker
    request-tracker.classes = [RequestLogger,com.example.app.MyRequestTracker]
    node-state-listener.classes = [com.example.app.MyNodeStateListener1,com.example.app.MyNodeStateListener2]
    schema-change-listener.classes = [com.example.app.MySchemaChangeListener]
  }
}
```

When more than one component of the same type is registered, the driver will distribute received
signals to all components in sequence, by order of their registration, starting with the
programmatically-provided ones. If a component throws an error, the error is intercepted and logged.

### 4.12.0

#### MicroProfile Metrics upgraded to 3.0

The MicroProfile Metrics library has been upgraded from version 2.4 to 3.0. Since this upgrade
involves backwards-incompatible binary changes, users of this library and of the
`java-driver-metrics-microprofile` module are required to take the appropriate action:

* If your application is still using MicroProfile Metrics < 3.0, you can still upgrade the core
  driver to 4.12, but you now must keep `java-driver-metrics-microprofile` in version 4.11 or lower,
  as newer versions will not work.
    
* If your application is using MicroProfile Metrics >= 3.0, then you must upgrade to driver 4.12 or
  higher, as previous versions of `java-driver-metrics-microprofile` will not work.

#### Mapper `@GetEntity` and `@SetEntity` methods can now be lenient

Thanks to [JAVA-2935](https://datastax-oss.atlassian.net/browse/JAVA-2935), `@GetEntity` and
`@SetEntity` methods now have a new `lenient` attribute.

If the attribute is `false` (the default value), then the source row or the target statement must
contain a matching column for every property in the entity definition. If such a column is not
found, an error will be thrown. This corresponds to the mapper's current behavior prior to the
introduction of the new attribute.

If the new attribute is explicitly set to `true` however, the mapper will operate on a best-effort
basis and attempt to read or write all entity properties that have a matching column in the source
row or in the target statement, *leaving unmatched properties untouched*.

This new, lenient behavior allows to achieve the equivalent of driver 3.x 
[lenient mapping](https://docs.datastax.com/en/developer/java-driver/3.10/manual/object_mapper/using/#manual-mapping).

Read the manual pages on [@GetEntity](../manual/mapper/daos/getentity) methods and
[@SetEntity](../manual/mapper/daos/setentity) methods for more details and examples of lenient mode.

### 4.11.0

#### Native protocol V5 is now production-ready

Thanks to [JAVA-2704](https://datastax-oss.atlassian.net/browse/JAVA-2704), 4.11.0 is the first
version in the driver 4.x series to fully support Cassandra's native protocol version 5, which has
been promoted from beta to production-ready in the upcoming Cassandra 4.0 release.

Users should not experience any disruption. When connecting to Cassandra 4.0, V5 will be
transparently selected as the protocol version to use.

#### Customizable metric names, support for metric tags

[JAVA-2872](https://datastax-oss.atlassian.net/browse/JAVA-2872) introduced the ability to configure
how metric identifiers are generated. Metric names can now be configured, but most importantly,
metric tags are now supported. See the [metrics](../manual/core/metrics/) section of the online
manual, or the `advanced.metrics.id-generator` section in the
[reference.conf](../manual/core/configuration/reference/) file for details.

Users should not experience any disruption. However, those using metrics libraries that support tags
are encouraged to try out the new `TaggingMetricIdGenerator`, as it generates metric names and tags
that will look more familiar to users of libraries such as Micrometer or MicroProfile Metrics (and
look nicer when exported to Prometheus or Graphite).

#### New `NodeDistanceEvaluator` API

All driver built-in load-balancing policies now accept a new optional component called
[NodeDistanceEvaluator]. This component gets invoked each time a node is added to the cluster or
comes back up. If the evaluator returns a non-null distance for the node, that distance will be
used, otherwise the driver will use its built-in logic to assign a default distance to it.

[NodeDistanceEvaluator]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/loadbalancing/NodeDistanceEvaluator.html

This component replaces the old "node filter" component. As a consequence, all `withNodeFilter`
methods in `SessionBuilder` are now deprecated and should be replaced by the equivalent
`withNodeDistanceEvaluator` methods.

If you have an existing node filter implementation, it can be converted to a `NodeDistanceEvaluator`
very easily:

```java
Predicate<Node> nodeFilter = ...
NodeDistanceEvaluator nodeEvaluator = 
    (node, dc) -> nodeFilter.test(node) ? null : NodeDistance.IGNORED;
```

The above can also be achieved by an adapter class as shown below:

```java
public class NodeFilterToDistanceEvaluatorAdapter implements NodeDistanceEvaluator {

    private final Predicate<Node> nodeFilter;

    public NodeFilterToDistanceEvaluatorAdapter(@NonNull Predicate<Node> nodeFilter) {
        this.nodeFilter = nodeFilter;
    }

    @Nullable @Override
    public NodeDistance evaluateDistance(@NonNull Node node, @Nullable String localDc) {
        return nodeFilter.test(node) ? null : NodeDistance.IGNORED;
    }
}
```

Finally, the `datastax-java-driver.basic.load-balancing-policy.filter.class` configuration option
has been deprecated; it should be replaced with a node distance evaluator class defined by the
`datastax-java-driver.basic.load-balancing-policy.evaluator.class` option instead.

### 4.10.0

#### Cross-datacenter failover

[JAVA-2899](https://datastax-oss.atlassian.net/browse/JAVA-2899) re-introduced the ability to
perform cross-datacenter failover using the driver's built-in load balancing policies. See [Load
balancing](../manual/core/loadbalancing/) in the manual for details.

Cross-datacenter failover is disabled by default, therefore existing applications should not
experience any disruption.

#### New `RetryVerdict` API

[JAVA-2900](https://datastax-oss.atlassian.net/browse/JAVA-2900) introduced [`RetryVerdict`], a new 
interface that allows custom retry policies to customize the request before it is retried.

For this reason, the following methods in the `RetryPolicy` interface were added; they all return
a `RetryVerdict` instance:

1. [`onReadTimeoutVerdict`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onReadTimeoutVerdict-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-int-int-boolean-int-)
2. [`onWriteTimeoutVerdict`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onWriteTimeoutVerdict-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-com.datastax.oss.driver.api.core.servererrors.WriteType-int-int-int-)
3. [`onUnavailableVerdict`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onUnavailableVerdict-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-int-int-int-)
4. [`onRequestAbortedVerdict`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onRequestAbortedVerdict-com.datastax.oss.driver.api.core.session.Request-java.lang.Throwable-int-)
5. [`onErrorResponseVerdict`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onErrorResponseVerdict-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.servererrors.CoordinatorException-int-)

The following methods were deprecated and will be removed in the next major version:

1. [`onReadTimeout`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onReadTimeout-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-int-int-boolean-int-)
2. [`onWriteTimeout`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onWriteTimeout-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-com.datastax.oss.driver.api.core.servererrors.WriteType-int-int-int-)
3. [`onUnavailable`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onUnavailable-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-int-int-int-)
4. [`onRequestAborted`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onRequestAborted-com.datastax.oss.driver.api.core.session.Request-java.lang.Throwable-int-)
5. [`onErrorResponse`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onErrorResponse-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.servererrors.CoordinatorException-int-)

Driver 4.10.0 also re-introduced a retry policy whose behavior is equivalent to the
`DowngradingConsistencyRetryPolicy` from driver 3.x. See this
[FAQ entry](https://docs.datastax.com/en/developer/java-driver/4.11/faq/#where-is-downgrading-consistency-retry-policy)
for more information.

[`RetryVerdict`]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryVerdict.html

#### Enhancements to the `Uuids` utility class

[JAVA-2449](https://datastax-oss.atlassian.net/browse/JAVA-2449) modified the implementation of
[Uuids.random()]: this method does not delegate anymore to the JDK's `java.util.UUID.randomUUID()`
implementation, but instead re-implements random UUID generation using the non-cryptographic
random number generator `java.util.Random`.

For most users, non-cryptographic strength is enough and this change should translate into better 
performance when generating UUIDs for database insertion. However, in the unlikely case where your
application requires cryptographic strength for UUID generation, you should update your code to
use `java.util.UUID.randomUUID()` instead of `com.datastax.oss.driver.api.core.uuid.Uuids.random()` 
from now on.

This release also introduces two new methods for random UUID generation:

1. [Uuids.random(Random)]: similar to `Uuids.random()` but allows to pass a custom instance of 
   `java.util.Random` and/or re-use the same instance across calls.
2. [Uuids.random(SplittableRandom)]: similar to `Uuids.random()` but uses a 
   `java.util.SplittableRandom` instead.

[Uuids.random()]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/uuid/Uuids.html#random--
[Uuids.random(Random)]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/uuid/Uuids.html#random-java.util.Random-
[Uuids.random(SplittableRandom)]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/uuid/Uuids.html#random-java.util.SplittableRandom-

#### System and DSE keyspaces automatically excluded from metadata and token map computation

[JAVA-2871](https://datastax-oss.atlassian.net/browse/JAVA-2871) now allows for a more fine-grained
control over which keyspaces should qualify for metadata and token map computation, including the 
ability to *exclude* keyspaces based on their names.

From now on, the following keyspaces are automatically excluded:

1. The `system` keyspace;
2. All keyspaces starting with `system_`;
3. DSE-specific keyspaces: 
   1. All keyspaces starting with `dse_`;
   2. The `solr_admin` keyspace;
   3. The `OpsCenter` keyspace.
   
This means that they won't show up anymore in [Metadata.getKeyspaces()], and [TokenMap] will return
empty replicas and token ranges for them. If you need the driver to keep computing metadata and
token map for these keyspaces, you now must modify the following configuration option:
`datastax-java-driver.advanced.metadata.schema.refreshed-keyspaces`.

[Metadata.getKeyspaces()]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/metadata/Metadata.html#getKeyspaces--
[TokenMap]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/metadata/TokenMap.html

#### DSE Graph dependencies are now optional

Until driver 4.9.0, the driver declared a mandatory dependency to Apache TinkerPop, a library
required only when connecting to DSE Graph. The vast majority of Apache Cassandra users did not need
that library, but were paying the price of having that heavy-weight library in their application's
classpath. 

_Starting with driver 4.10.0, TinkerPop is now considered an optional dependency_. 

Regular users of Apache Cassandra that do not use DSE Graph will not notice any disruption.

DSE Graph users, however, will now have to explicitly declare a dependency to Apache TinkerPop. This
can be achieved with Maven by adding the following dependencies to the `<dependencies>` section of
your POM file:

```xml
<dependency>
  <groupId>org.apache.tinkerpop</groupId>
  <artifactId>gremlin-core</artifactId>
  <version>${tinkerpop.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.tinkerpop</groupId>
  <artifactId>tinkergraph-gremlin</artifactId>
  <version>${tinkerpop.version}</version>
</dependency>
```

See the [integration](../manual/core/integration/#tinker-pop) section in the manual for more details
as well as a driver vs. TinkerPop version compatibility matrix.

### 4.5.x - 4.6.0

These versions are subject to [JAVA-2676](https://datastax-oss.atlassian.net/browse/JAVA-2676), a
bug that causes performance degradations in certain scenarios. We strongly recommend upgrading to at
least 4.6.1.

### 4.4.0

DataStax Enterprise support is now available directly in the main driver. There is no longer a
separate DSE driver.

#### For Apache Cassandra® users

The great news is that [reactive execution](../manual/core/reactive/) is now available for everyone.
See the `CqlSession.executeReactive` methods.

Apart from that, the only visible change is that DSE-specific features are now exposed in the API: 

* new execution methods: `CqlSession.executeGraph`, `CqlSession.executeContinuously*`. They all
  have default implementations so this doesn't break binary compatibility. You can just ignore them.
* new driver dependencies: TinkerPop, ESRI, Reactive Streams. If you want to keep your classpath
  lean, you can exclude some dependencies when you don't use the corresponding DSE features; see the 
  [Integration>Driver dependencies](../manual/core/integration/#driver-dependencies) section.

#### For DataStax Enterprise users

Adjust your Maven coordinates to use the unified artifact:

```xml
<!-- Replace: -->
<dependency>
  <groupId>com.datastax.dse</groupId>
  <artifactId>dse-java-driver-core</artifactId>
  <version>2.3.0</version>
</dependency>

<!-- By: -->
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>4.4.0</version>
</dependency>

<!-- Do the same for the other modules: query builder, mapper... -->
```

The new driver is a drop-in replacement for the DSE driver. Note however that we've deprecated a few
DSE-specific types in favor of their OSS equivalents. They still work, so you don't need to make the
changes right away; but you will get deprecation warnings:

* `DseSession`: use `CqlSession` instead, it can now do everything that a DSE session does. This
  also applies to the builder:
  
    ```java
    // Replace:
    DseSession session = DseSession.builder().build()  
  
    // By:
    CqlSession session = CqlSession.builder().build()
    ```
* `DseDriverConfigLoader`: the driver no longer needs DSE-specific config loaders. All the factory
  methods in this class now redirect to `DriverConfigLoader`. On that note, `dse-reference.conf`
  does not exist anymore, all the driver defaults are now in
  [reference.conf](../manual/core/configuration/reference/).
* plain-text authentication: there is now a single implementation that works with both Cassandra and
  DSE. If you used `DseProgrammaticPlainTextAuthProvider`, replace it by
  `PlainTextProgrammaticAuthProvider`. Similarly, if you wrote a custom implementation by
  subclassing `DsePlainTextAuthProviderBase`, extend `PlainTextAuthProviderBase` instead.
* `DseLoadBalancingPolicy`: DSE-specific features (the slow replica avoidance mechanism) have been
  merged into `DefaultLoadBalancingPolicy`. `DseLoadBalancingPolicy` still exists for backward
  compatibility, but it is now identical to the default policy.

#### Class Loader

The default class loader used by the driver when instantiating classes by reflection changed. 
Unless specified by the user, the driver will now use the same class loader that was used to load
the driver classes themselves, in order to ensure that implemented interfaces and implementing 
classes are fully compatible.

This should ensure a more streamlined experience for OSGi users, who do not need anymore to define
a specific class loader to use.

However if you are developing a web application and your setup corresponds to the following 
scenario, then you will now be required to explicitly define another class loader to use: if in your
application the driver jar is loaded by the web server's system class loader (for example, 
because the driver jar was placed in the "/lib" folder of the web server), then the default class
loader will be the server's system class loader. Then if the application tries to load, say, a 
custom load balancing policy declared in the web app's "WEB-INF/lib" folder, then the default class 
loader will not be able to locate that class. Instead, you must use the web app's class loader, that 
you can obtain in most web environments by calling `Thread.getContextClassLoader()`:
 
    CqlSession.builder()
        .addContactEndPoint(...)
        .withClassLoader(Thread.currentThread().getContextClassLoader())
        .build();
 
See the javadocs of [SessionBuilder.withClassLoader] for more information.

[SessionBuilder.withClassLoader]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withClassLoader-java.lang.ClassLoader-

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
* **DataStax Enterprise: 4.7 and above**.

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
