## Upgrade guide

The purpose of this guide is to detail changes made by successive
versions of the Java driver.

### 3.0

This version brings parity with Cassandra 2.2 and 3.0.

It is **not binary compatible** with the driver's 2.1 branch.
The main changes were introduced by the custom codecs feature (see below).
We've also seized the opportunity to remove code that was deprecated in 2.1.

1.  The default consistency level in `QueryOptions` is now `LOCAL_ONE`.
2.  [Custom codecs](../manual/custom_codecs/)
    ([JAVA-721](https://datastax-oss.atlassian.net/browse/JAVA-721))
    introduce several breaking changes and also modify a few runtime behaviors.

    Here is a detailed list of breaking API changes:
    * `TypeCodec` was package-private before and is now public.
    * `DataType` has no more references to `TypeCodec`, so methods that dealt with serialization and deserialization of
        data types have been removed:
        * `ByteBuffer serialize(Object value, ProtocolVersion protocolVersion)`
        * `ByteBuffer serializeValue(Object value, ProtocolVersion protocolVersion)`
        * `Object deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)`
        * `Object deserialize(ByteBuffer bytes, int protocolVersion)`
        * `Object parse(String value)`
        * `String format(Object value)`
        * `Class<?> asJavaClass()`

        These methods must now be invoked on `TypeCodec` directly. To resolve the `TypeCodec` instance for a particular
        data type, use `CodecRegistry#codecFor`.
    * `GettableByIndexData` (affects `Row`, `BoundStatement`, `TupleValue` and `UDTValue`). The following public methods were added:
        * `<T> T get(int i, Class<T> targetClass)`
        * `<T> T get(int i, TypeToken<T> targetType)`
        * `<T> T get(int i, TypeCodec<T> codec)`
    * `GettableByNameData` (affects `Row`, `BoundStatement` and `UDTValue`). The following public methods were added:
        * `<T> T get(String name, Class<T> targetClass)`
        * `<T> T get(String name, TypeToken<T> targetType)`
        * `<T> T get(String name, TypeCodec<T> codec)`
    * `SettableByIndexData` (affects `Row`, `BoundStatement`, `TupleValue` and `UDTValue`). The following public methods were added:
        * `<V> T set(int i, V v, Class<V> targetClass)`
        * `<V> T set(int i, V v, TypeToken<V> targetType)`
        * `<V> T set(int i, V v, TypeCodec<V> codec)`
    * `SettableByNameData` (affects `Row`, `BoundStatement` and `UDTValue`). The following public methods were added:
        * `<V> T set(String name, V v, Class<V> targetClass)`
        * `<V> T set(String name, V v, TypeToken<V> targetType)`
        * `<V> T set(String name, V v, TypeCodec<V> codec)`
    * `Statement`. The following public methods were modified:
        * `getRoutingKey(ProtocolVersion, CodecRegistry)`: both parameters added.
    * `RegularStatement`. The following public methods were modified:
        * `getValues(ProtocolVersion, CodecRegistry)`: second parameter added.
        * `getQueryString(CodecRegistry)` and `hasValues(CodecRegistry)`: parameter added. No-arg versions are still present
           and use the default codec registry; refer to the Javadocs for guidance on which version to use.
    * `PreparedStatement`. The following public method was added:
        * `CodecRegistry getCodecRegistry()`.
    * `TupleType`. The following public method was deleted:
        * `TupleType of(DataType... types)`; users should now use `Metadata.newTupleType(DataType...)`.

    <p>The driver runtime behavior changes in the following situations:</p>
    * By default, the driver now returns _mutable_, _non thread-safe_ instances for CQL collection types;
      This affects methods `getList`, `getSet`, `getMap`, `getObject` and `get` for all instances of
      `GettableByIndexData` and `GettableByNameData` (`Row`, `BoundStatement`, `TupleValue` and `UDTValue`)
    * `RuntimeException`s thrown during serialization or deserialization might not be
      the same ones as before, due to the newly-introduced `CodecNotFoundException`
      and to the dynamic nature of codec search introduced by JAVA-721.
    * `TypeCodec.format(Object)` now returns the CQL keyword `"NULL"` instead of a `null` reference
      for `null` inputs.

3.  The driver now depends on Guava 16.0.1 (instead of 14.0.1).
    This update has been mainly motivated by Guava's [Issue #1635](https://code.google.com/p/guava-libraries/issues/detail?id=1635),
    which affects `TypeToken`, and hence all `TypeCodec` implementations handling parameterized types.

4.  `UDTMapper` (the type previously used to convert `@UDT`-annotated
    classes to their CQL counterpart) was removed, as well as the
    corresponding method `MappingManager#udtMapper`.

    The mapper now uses custom codecs to convert UDTs. See more
    explanations [here](../manual/object_mapper/custom_codecs/#implicit-udt-codecs).

5.  All methods that took the protocol version as an `int` or assumed a
    default version have been removed (they were already deprecated in
    2.1):
    * `AbstractGettableData(int)`
    * `Cluster.Builder#withProtocolVersion(int)`
    * in `ProtocolOptions`:
      * `NEWEST_SUPPORTED_PROTOCOL_VERSION` (replaced by
        `ProtocolVersion#NEWEST_SUPPORTED`)
      * `int getProtocolVersion()`

    There are now variants of these methods using the `ProtocolVersion`
    enum. In addition, `ProtocolOptions#getProtocolVersionEnum` has been
    renamed to `ProtocolOptions#getProtocolVersion`.

6.  All methods related to the "suspected" host state have been removed
    (they had been deprecated in 2.1.6 when the suspicion mechanism was
    removed):
    * `Host.StateListener#onSuspected()` (was inherited by
      `LoadBalancingPolicy`)
    * `Host#getInitialReconnectionAttemptFuture()`

7.  `PoolingOptions#setMinSimultaneousRequestsPerConnectionThreshold(HostDistance,
    int)` has been removed. The new connection pool resizing algorithm introduced by
    [JAVA-419](https://datastax-oss.atlassian.net/browse/JAVA-419) does not need this
    threshold anymore.

8.  `AddressTranslater` has been renamed to `AddressTranslator`. All
    related methods and classes have also been renamed.

    In addition, the `close()` method has been pulled up into
    `AddressTranslator`, and `CloseableAddressTranslator` has been removed.
    Existing third-party `AddressTranslator` implementations only need
    to add an empty `close()` method.

9.  The `close()` method has been pulled up into `LoadBalancingPolicy`,
    and `CloseableLoadBalancingPolicy` has been removed. Existing third-party
    `LoadBalancingPolicy` implementations only need to add an empty
    `close()` method.

10. All pluggable components now have callbacks to detect when they get
    associated with a `Cluster` instance:
    * `ReconnectionPolicy`, `RetryPolicy`, `AddressTranslator`,
      and `TimestampGenerator`:
      * `init(Cluster)`
      * `close()`
    * `Host.StateListener` and `LatencyTracker`:
      * `onRegister(Cluster)`
      * `onUnregister(Cluster)`

    This gives these components the opportunity to perform
    initialization / cleanup tasks. Existing third-party implementations
    only need to add empty methods.

11. `LoadBalancingPolicy` does not extend `Host.StateListener` anymore:
    callback methods (`onUp`, `onDown`, etc.) have been duplicated. This
    is unlikely to affect clients.

12. [Client-side timestamp generation](../manual/query_timestamps/) is
    now the default (provided that [native
    protocol](../manual/native_protocol) v3 or higher is in use). The
    generator used is `AtomicMonotonicTimestampGenerator`.

13. If a DNS name resolves to multiple A-records,
    `Cluster.Builder#addContactPoint(String)` will now use all of these
    addresses as contact points. This gives you the possibility of
    maintaining contact points in DNS configuration, and having a single,
    static contact point in your Java code.

14. The following methods were added for [Custom payloads](../manual/custom_payloads):
    * in `PreparedStatement`: `getIncomingPayload()`,
      `getOutgoingPayload()` and
      `setOutgoingPayload(Map<String,ByteBuffer>)`
    * `AbstractSession#prepareAsync(String, Map<String,ByteBuffer>)`

    Also, note that `AbstractSession#prepareAsync(Statement)` does not
    call `AbstractSession#prepareAsync(String)` anymore, they now both
    delegate to a protected method.

    This breaks binary compatibility for these two classes; if you have
    custom implementations, you will have to adapt them accordingly.

15. Getters and setters have been added to "data-container" classes for
    new CQL types:
    * `getByte`/`setByte` for the `TINYINT` type
    * `getShort`/`setShort` for the `SMALLINT` type
    * `getTime`/`setTime` for the `TIME` type
    * `getDate`/`setDate` for the `DATE` type

    The methods for the `TIMESTAMP` CQL type have been renamed to
    `getTimestamp` and `setTimestamp`.

    This affects `Row`, `BoundStatement`, `TupleValue` and `UDTValue`.

16. New exception types have been added to handle additional server-side
    errors introduced in Cassandra 2.2:
    * `ReadFailureException`
    * `WriteFailureException`
    * `FunctionExecutionException`

    This is not a breaking change since all driver exceptions are
    unchecked; but clients might decide to handle these errors in a specific
    way.

    In addition, `QueryTimeoutException` has been renamed to
    `QueryExecutionException` (this is an intermediary class in our
    exception hierarchy, it now has new child classes that are not
    related to timeouts).

17. `ResultSet#fetchMoreResults()` now returns a `ListenableFuture<ResultSet>`.
    This makes the API more friendly if you chain transformations on an async
    query to process all pages (see `AsyncResultSetTest` in the sources for an
    example).

18. `Frozen` annotations in the mapper are no longer checked at runtime (see
    [JAVA-843](https://datastax-oss.atlassian.net/browse/JAVA-843) for more
    explanations). So they become purely informational at this stage.
    However it is a good idea to keep using these annotations and make sure
    they match the schema, in anticipation for the schema generation features
    that will be added in a future version.

19. `AsyncInitSession` has been removed, `initAsync()` is now part of the
    `Session` interface (the only purpose of the extra interface was to preserve
    binary compatibility on the 2.1 branch).

20. `TableMetadata.Options` has been made a top-level class and renamed to
    `TableOptionsMetadata`. It is now also used by `MaterializedViewMetadata`.

21. The mapper annotation `@Enumerated` has been removed, users should now
    use the newly-introduced `driver-extras` module to get automatic
    enum-to-CQL mappings. Two new codecs provide the same functionality:
    `EnumOrdinalCodec` and `EnumNameCodec`:

    ```java
    enum Foo {...}
    enum Bar {...}

    // register the appropriate codecs
    CodecRegistry.DEFAULT_INSTANCE
        .register(new EnumOrdinalCodec<Foo>(Foo.class))
        .register(new EnumNameCodec<Bar>(Bar.class))

    // the following mappings are handled out-of-the-box
    @Table
    public class MyPojo {
        private Foo foo;
        private List<Bar> bars;
        ...
    }
    ```

22. The interface `IdempotenceAwarePreparedStatement` has been removed
    and now the `PreparedStatement` interface exposes 2 new methods,
    `setIdempotent(Boolean)` and `isIdempotent()`.

23. `RetryPolicy` and `ExtendedRetryPolicy` (introduced in 2.1.10)
    were merged together; as a consequence, `RetryPolicy` now has one
    more method: `onRequestError`; see
    [JAVA-819](https://datastax-oss.atlassian.net/browse/JAVA-819) for
    more information. Furthermore, `FallthroughRetryPolicy` now returns
    `RetryDecision.rethrow()` when `onRequestError` is called.

24. `DseAuthProvider` has been deprecated and is now replaced by
    `DseGSSAPIAuthProvider` for Kerberos authentication. `DsePlainTextAuthProvider`
    has been introduced to handle plain text authentication with the
    `DseAuthenticator`.

25. The constructor of `DCAwareRoundRobinPolicy` is not accessible anymore. You
    should use `DCAwareRoundRobinPolicy#builder()` to create new instances.
    
26. `ColumnMetadata.getTable()` has been renamed to `ColumnMetadata.getParent()`.
    Also, its return type is now `AbstractTableMetadata` which can be either
    a `TableMetadata` object or a `MaterializedViewMetadata` object.
    This change is motivated by the fact that a column can now belong to a 
    table or a materialized view.
    
27. `ColumnMetadata.getIndex()` has been removed. 
    This is due to the fact that secondary indexes have been completely redesigned 
    in Cassandra 3.0, and the former one-to-one relationship between a column and its index
    has been replaced with a one-to-many relationship between a table and its indexes.
    This is reflected in the driver's API by the new methods
    `TableMetadata.getIndexes()` and `TableMetadata.getIndex(String name)`.
    See [CASSANDRA-9459](https://issues.apache.org/jira/browse/CASSANDRA-9459) and
    and [JAVA-1008](https://datastax-oss.atlassian.net/browse/JAVA-1008) for
    more details.
    Unfortunately, there is no easy way to recover the functionality provided
    by the deleted method, _even for Cassandra versions <= 3.0_.

28. `IndexMetadata` is now a top-level class and its structure has been deeply modified. 
    Again, this is due to the fact that secondary indexes have been completely redesigned 
    in Cassandra 3.0.


### 2.1.8

2.1.8 is binary-compatible with 2.1.7 but introduces a small change in the
driver's behavior:

1. The list of contact points provided at startup is now shuffled before trying
   to open the control connection, so that multiple clients with the same contact
   points don't all pick the same control host. As a result, you can't assume that
   the driver will try contact points in a deterministic order. In particular, if
   you use the `DCAwareRoundRobinPolicy` without specifying a primary datacenter
   name, make sure that you only provide local hosts as contact points.


### 2.1.7

This version brings a few changes in the driver's behavior; none of them break
binary compatibility.

1. The `DefaultRetryPolicy`'s behaviour has changed in the case of an Unavailable
   exception received from a request. The new behaviour will cause the driver to
   process a Retry on a different node at most once, otherwise an exception will
   be thrown. This change makes sense in the case where the node tried initially
   for the request happens to be isolated from the rest of the cluster (e.g.
   because of a network partition) but can still answer to the client normally.
   In this case, trying another node has a chance of success.
   The previous behaviour was to always throw an exception.

2. The following properties in `PoolingOptions` were renamed:
    * `MaxSimultaneousRequestsPerConnectionThreshold` to `NewConnectionThreshold`
    * `MaxSimultaneousRequestsPerHostThreshold` to `MaxRequestsPerConnection`

    The old getters/setters were deprecated, but they delegate to the new
    ones.

    Also, note that the connection pool for protocol v3 can now be configured to
    use multiple connections. See [this page](../manual/pooling) for more
    information.

3. `MappingManager(Session)` will now force the initialization of the `Session`
   if needed. This is a change from 2.1.6, where if you gave it an uninitialized
   session (created with `Cluster#newSession()` instead of `Cluster#connect()`),
   it would only get initialized on the first request.

    If this is a problem for you, `MappingManager(Session, ProtocolVersion)`
    preserves the previous behavior (see the API docs for more details).

4. A `BuiltStatement` is now considered non-idempotent whenever a `fcall()`
   or `raw()` is used to build a value to be inserted in the database.
   If you know that the CQL functions or expressions are safe, use
   `setIdempotent(true)` on the statement.

### 2.1.6

See [2.0.10](#2-0-x-to-2-0-10).


### 2.1.2

2.1.2 brings important internal changes with native protocol v3 support, but
the impact on the public API has been kept as low as possible.

#### User API Changes

1. The native protocol version is now modelled as an enum: `ProtocolVersion`.
   Most public methods that take it as an argument have a backward-compatible
   version that takes an `int` (the exception being `RegularStatement`,
   described below). For new code, prefer the enum version.

#### Internal API Changes

1. `RegularStatement.getValues` now takes the protocol version as a
   `ProtocolVersion` instead of an `int`. This is transparent for callers
   since there is a backward-compatible alternative, but if you happened to
   extend the class you'll need to update your implementation.

2. `BatchStatement.setSerialConsistencyLevel` now returns `BatchStatement`
   instead of `Statement`. Again, this only matters if you extended this
   class (if so, it might be a good idea to also have a covariant return in
   your child class).

3. The constructor of `UnsupportedFeatureException` now takes a
   `ProtocolVersion` as a parameter. This should impact few users, as there's
   hardly any reason to build instances of that class from client code.

#### New features

These features are only active when the native protocol v3 is in use.

1. The driver now uses a single connection per host (as opposed to a pool in
   2.1.1). Most options in `PoolingOptions` are ignored, except for a new one
   called `maxSimultaneousRequestsPerHostThreshold`. See the class's Javadocs
   for detailed explanations.

2. You can now provide a default timestamp with each query (but it will be
   ignored if the CQL query string already contains a `USING TIMESTAMP`
   clause). This can be done on a per-statement basis with
   `Statement.setDefaultTimestamp`, or automatically with a
   `TimestampGenerator` specified with
   `Cluster.Builder.withTimestampGenerator` (two implementations are
   provided: `ThreadLocalMonotonicTimestampGenerator` and
   `AtomicMonotonicTimestampGenerator`). If you specify both, the statement's
   timestamp takes precedence over the generator. By default, the driver has
   the same behavior as 2.1.1 (no generator, timestamps are assigned by
   Cassandra unless `USING TIMESTAMP` was specified).

3. `BatchStatement.setSerialConsistencyLevel` no longer throws an exception,
   it will honor the serial consistency level for the batch.


### 2.1.1

#### Internal API Changes

1. The `ResultSet` interface has a new `wasApplied()` method. This will
   only affect clients that provide their own implementation of this interface.


### 2.1.0

#### User API Changes

1. The `getCaching` method of `TableMetadata#Options` now returns a
   `Map` to account for changes to Cassandra 2.1. Also, the
   `getIndexInterval` method now returns an `Integer` instead of an `int`
   which will be `null` when connected to Cassandra 2.1 nodes.

2. `BoundStatement` variables that have not been set explicitly will no
   longer default to `null`. Instead, all variables must be bound explicitly,
   otherwise the execution of the statement will fail (this also applies to
   statements inside of a `BatchStatement`). For variables that map to a
   primitive Java type, a new `setToNull` method has been added.
   We made this change because the driver might soon distinguish between unset
   and null variables, so we don't want clients relying on the "leave unset to
   set to `null`" behavior.


#### Internal API Changes

The changes listed in this section should normally not impact end users of the
driver, but rather third-party frameworks and tools.

1. The `serialize` and `deserialize` methods in `DataType` now take an
   additional parameter: the protocol version. As explained in the javadoc,
   if unsure, the proper value to use for this parameter is the protocol version
   in use by the driver, i.e. the value returned by
   `cluster.getConfiguration().getProtocolOptions().getProtocolVersion()`.

2. The `parse` method in `DataType` now returns a Java object, not a
   `ByteBuffer`. The previous behavior can be obtained by calling the
   `serialize` method on the returned object.

3. The `getValues` method of `RegularStatement` now takes the protocol
   version as a parameter. As above, the proper value if unsure is almost surely
   the protocol version in use
   (`cluster.getConfiguration().getProtocolOptions().getProtocolVersion()`).


### 2.0.11

2.0.11 preserves binary compatibility with previous versions. There are a few
changes in the driver's behavior:

1. The `DefaultRetryPolicy`'s behaviour has changed in the case of an Unavailable
   exception received from a request. The new behaviour will cause the driver to
   process a Retry on a different node at most once, otherwise an exception will
   be thrown. This change makes sense in the case where the node tried initially
   for the request happens to be isolated from the rest of the cluster (e.g.
   because of a network partition) but can still answer to the client normally.
   In this case, trying another node has a chance of success.
   The previous behaviour was to always throw an exception.

2. A `BuiltStatement` is now considered non-idempotent whenever a `fcall()`
   or `raw()` is used to build a value to be inserted in the database.
   If you know that the CQL functions or expressions are safe, use
   `setIdempotent(true)` on the statement.

3. The list of contact points provided at startup is now shuffled before trying
   to open the control connection, so that multiple clients with the same contact
   points don't all pick the same control host. As a result, you can't assume that
   the driver will try contact points in a deterministic order. In particular, if
   you use the `DCAwareRoundRobinPolicy` without specifying a primary datacenter
   name, make sure that you only provide local hosts as contact points.


### <a name="2-0-x-to-2-0-10"></a>2.0.x to 2.0.10

We try to avoid breaking changes within a branch (2.0.x to 2.0.y), but
2.0.10 saw a lot of new features and internal improvements. There is one
breaking change:

1. `LatencyTracker#update` now has a different signature and takes two new
   parameters: the statement that has been executed (never null), and the exception
   thrown while executing the query (or null, if the query executed successfully).
   Existing implementations of this interface, once upgraded to the new method
   signature, should continue to work as before.

The following might also be of interest:

2. `SocketOptions#getTcpNoDelay()` is now TRUE by default (it was previously undefined).
   This reflects the new behavior of Netty (which was upgraded from version 3.9.0 to
   4.0.27): `TCP_NODELAY` is now turned on by default, instead of depending on the OS
   default like in previous versions.

3. Netty is not shaded anymore in the default Maven artifact. However we publish a
   [shaded artifact](../manual/shaded_jar/) under a different classifier.

4. The internal initialization sequence of the Cluster object has been slightly changed:
   some fields that were previously initialized in the constructor are now set when
   the `init()` method is called. In particular, `Cluster#getMetrics()` will return
   `null` until the cluster is initialized.

### 1.0 to 2.0

We used the opportunity of a major version bump to incorporate your feedback
and improve the API, to fix a number of inconsistencies and remove cruft.
Unfortunately this means there are some breaking changes, but the new API should
be both simpler and more complete.

The following describes the changes for 2.0 that are breaking changes of the
1.0 API. For ease of use, we distinguish two categories of API changes: the "main"
ones and the "other" ones.

The "main" API changes are the ones that are either
likely to affect most upgraded apps or are incompatible changes that, even if minor,
will not be detected at compile time. Upgraders are highly encouraged to check
this list of "main" changes while upgrading their application to 2.0 (even
though most applications are likely to be affected by only a handful of
changes).

The "other" list is, well, other changes: those that are likely to
affect a minor number of applications and will be detected by compile time
errors anyway. It is ok to skip those initially and only come back to them if
you have trouble compiling your application after an upgrade.

#### Main API changes

1. The `Query` class has been renamed into `Statement` (it was confusing
  to some that the `BoundStatement` was not a `Statement`). To allow
  this, the old `Statement` class has been renamed to `RegularStatement`.

2. The `Cluster` and `Session` shutdown API has changed. There is now a
  `closeAsync` that is asynchronous but returns a `Future` on the
  completion of the shutdown process.  There is also a `close` shortcut
  that does the same but blocks.  Also, `close` now waits for ongoing
  queries to complete by default (but you can force the closing of all
  connections if you want to).

3. `NoHostAvailableException#getErrors` now returns the full exception objects for
   each node instead of just a message. In other words, it returns a
   `Map<InetAddress, Throwable>` instead of a `Map<InetAddress, String>`.

4. `Statement#getConsistencyLevel` (previously `Query#getConsistencyLevel`, see
   first point) will now return `null` by default (instead of `CL.ONE`), with the
   meaning of "use the default consistency level".
   The default consistency level can now be configured through the new `QueryOptions`
   object in the cluster `Configuration`.

5. The `Metrics` class now uses the Codahale metrics library version 3 (version 2 was
   used previously). This new major version of the library has many API changes
   compared to its version 2 (see the [release notes](https://dropwizard.github.io/metrics/3.1.0/about/release-notes/) for details),
   which can thus impact consumers of the Metrics class.
   Furthermore, the default `JmxReporter` now includes a name specific to the
   cluster instance (to avoid conflicts when multiple Cluster instances are created
   in the same JVM). As a result, tools that were polling JMX info will
   have to be updated accordingly.

6. The `QueryBuilder#in` method now has the following special case: using
   `QueryBuilder.in(QueryBuilder.bindMarker())` will generate the string `IN ?`,
   not `IN (?)` as was the case in 1.0. The reasoning being that the former
   syntax, made valid by [CASSANDRA-4210](https://issues.apache.org/jira/browse/CASSANDRA-4210)
   is a lot more useful than `IN (?)`, as the latter can more simply use an
   equality.
   Note that if you really want to output `IN (?)` with the query
   builder, you can use `QueryBuilder.in(QueryBuilder.raw("?"))`.

7. When binding values by name in `BoundStatement` (i.e. using the
   `setX(String, X)` methods), if more than one variable have the same name,
   then all values corresponding to that variable
   name are set instead of just the first occurrence.

8. The `QueryBuilder#raw` method does not automatically add quotes anymore, but
   rather output its result without any change (as the raw name implies).
   This means for instance that `eq("x", raw(foo))` will output `x = foo`,
   not `x = 'foo'` (you don't need the raw method to output the latter string).

9. The `QueryBuilder` will now sometimes use the new ability to send value as
   bytes instead of serializing everything to string. In general the QueryBuilder
   will do the right thing, but if you were calling the `getQueryString()` method
   on a Statement created with a QueryBuilder (for other reasons than to prepare a query)
   then the returned string may contain bind markers in place of some of the values
   provided (and in that case, `getValues()` will contain the values corresponding
   to those markers). If need be, it is possible to force the old behavior by
   using the new `setForceNoValues()` method.

#### Other API Changes

1. Creating a Cluster instance (through `Cluster#buildFrom` or the
   `Cluster.Builder#build` method) **does not create any connection right away
   anymore** (and thus cannot throw a `NoHostAvailableException` or an
   `AuthenticationException`). Instead, the initial contact points are checked
   the first time a call to `Cluster#connect` is done. If for some reason you
   want to emulate the previous behavior, you can use the new method
   `Cluster#init`: `Cluster.builder().build()` in 1.0 is equivalent to
   `Cluster.builder().build().init()` in 2.0.

2. Methods from `Metadata`, `KeyspaceMetadata` and `TableMetadata` now use by default
   case insensitive identifiers (for keyspace, table and column names in
   parameter). You can double-quote an identifier if you want it to be a
   case sensitive one (as you would do in CQL) and there is a `Metadata.quote`
   helper method for that.

3. The `TableMetadata#getClusteringKey` method has been renamed
   `TableMetadata#getClusteringColumns` to match the "official" vocabulary.

4. The `UnavailableException#getConsistency` method has been renamed to
   `UnavailableException#getConsistencyLevel` for consistency with the method of
   `QueryTimeoutException`.

5. The `RegularStatement` class (ex-`Statement` class, see above) must now
   implement two additional methods: `RegularStatement#getKeyspace` and
   `RegularStatement#getValues`. If you had extended this class, you will have to
   implement those new methods, but both can return null if they are not useful
   in your case.

6. The `Cluster.Initializer` interface should now implement 2 new methods:
   `Cluster.Initializer#getInitialListeners` (which can return an empty
   collection) and `Cluster.Initializer#getClusterName` (which can return null).

7. The `Metadata#getReplicas` method now takes 2 arguments. On top of the
   partition key, you must now provide the keyspace too. The previous behavior
   was buggy: it's impossible to properly return the full list of replica for a
   partition key without knowing the keyspace since replication may depend on
   the keyspace).

8. The method `LoadBalancingPolicy#newQueryPlan()` method now takes the currently
   logged keyspace as 2nd argument. This information is necessary to do proper
   token aware balancing (see preceding point).

9. The `ResultSetFuture#set` and `ResultSetFuture#setException` methods have been
   removed (from the public API at least). They were never meant to be exposed
   publicly: a `resultSetFuture` is always set by the driver itself and should
   not be set manually.

10. The deprecated since 1.0.2 `Host.HealthMonitor` class has been removed. You
    will now need to use `Host#isUp` and `Cluster#register` if you were using that
    class.

#### Features available only with Cassandra 2.0

This section details the biggest additions to 2.0 API wise. It is not an
exhaustive list of new features in 2.0.

1. The new `BatchStatement` class allows to group any type of insert Statements
   (`BoundStatement` or `RegularStatement`) for execution as a batch. For instance,
   you can do something like:

    ```java
    List<String> values = ...;
    PreparedStatement ps = session.prepare("INSERT INTO myTable(value) VALUES (?)");
    BatchStatement bs = new BatchStatement();
    for (String value : values)
       bs.add(ps.bind(value));
    session.execute(bs);
    ```

2. `SimpleStatement` can now take a list of values in addition to the query. This
   allows to do the equivalent of a prepare+execute but with only one round-trip
   to the server and without keeping the prepared statement after the
   execution.

    This is typically useful if a given query should be executed only
    once (i.e. you don't want to prepare it) but you also don't want to
    serialize all values into strings. Shortcut `Session#execute()` and
    `Session#executeAsync()` methods are also provided so you that you can do:

    ```java
    String imgName = ...;
    ByteBuffer imgBytes = ...;
    session.execute("INSERT INTO images(name, bytes) VALUES (?, ?)", imgName, imgBytes);
    ```

3. SELECT queries are now "paged" under the hood. In other words, if a query
   yields a very large result, only the beginning of the `ResultSet` will be fetched
   initially, the rest being fetched "on-demand". In practice, this means that:

    ```java
    for (Row r : session.execute("SELECT * FROM mytable"))
       ... process r ...
    ```

    should not timeout or OOM the server anymore even if "mytable" contains a lot
    of data. In general paging should be transparent for the application (as in
    the example above), but the implementation provides a number of knobs to
    fine tune the behavior of that paging:
    * the size of each "page" can be set per-query (`Statement#setFetchSize()`)
    * the `ResultSet` object provides 2 methods to check the state of paging
      (`ResultSet#getAvailableWithoutFetching` and `ResultSet#isFullyFetched`)
      as well as a mean to force the pre-fetching of the next page (`ResultSet#fetchMoreResults`).
