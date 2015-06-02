## Upgrade guide

The purpose of this guide is to detail changes made by successive
versions of the Java driver.

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


### 2.0.x to 2.0.10

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
   [shaded artifact](../features/shaded_jar/) under a different classifier.

4. The internal initialization sequence of the Cluster object has been slightly changed:
   some fields that were previously initialized in the constructor are now set when
   the `init()` method is called. This is unlikely to affect regular driver users.

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
   yields a very large result, only the beginning of the `ResultSet` will be fetch
   initially, the rest being fetch "on-demand". In practice, this means that:

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
