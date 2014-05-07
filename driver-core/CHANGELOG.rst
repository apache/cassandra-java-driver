CHANGELOG
=========

1.0.7:
------

- [new] Add method to check if a Cluster/Session instance has been closed already (JAVA-114)
- [new] Expose the name of the partitioner in use in the cluster metadata (JAVA-179)
- [new] Add new WhiteListPolicy to limit the nodes connected to a particular list
- [improvement] Do not hop DC for LOCAL_* CL in DCAwareRoundRobinPolicy (JAVA-289)
- [bug] Revert back to longs for dates in the query builder (JAVA-313)
- [bug] Don't reconnect to nodes ignored by the load balancing policy (JAVA-314)


1.0.6:
------

- [new] Added Session#prepareAsync calls (JAVA-224)
- [new] Added Cluster#getLoggedKeyspace (JAVA-249)
- [improvement] Make most main objects interface to facilitate testing/mocking (JAVA-195)
- [improvement] Avoid preparing a statement multiple time per host with multiple sessions
- [bug] Fix potential thread leaks when shutting down Metrics (JAVA-232)
- [bug] Fix potential NPE in HostConnectionPool (JAVA-231)
- [bug] Make sure connections are returned to the right pools (JAVA-255)
- [bug] Use date string in query build to work-around CASSANDRA-6718 (JAVA-264)
- [bug] Always return the PreparedStatement object that is cache internally (JAVA-263)
- [bug] Fix race when multiple connect are done in parallel (JAVA-261)
- [bug] Don't connect at all to nodes that are ignored by the load balancing
  policy (JAVA-270)
- [bug] LatencyAwarePolicy.Builder#withScale doesn't set the scale (JAVA-275)


1.0.5:
------

- [new] OSGi bundle (JAVA-142)
- [new] Add support for ConsistencyLevel.LOCAL_ONE; note that this
  require Cassandra 1.2.12+ (JAVA-207)
- [improvement] Make collections returned by Row immutable (JAVA-205)
- [improvement] Limit internal thread pool size (JAVA-203)
- [improvement] New getter to obtain a snapshot of the scores maintained by
  LatencyAwarePolicy.
- [improvement] Avoid synchronization when getting codec for collection
  types (JAVA-222)
- [bug] Don't retain unused PreparedStatement in memory (JAVA-201, JAVA-213)
- [bug] Add missing clustering order info in TableMetadata
- [bug] Allow bind markers for collections in the query builder (JAVA-196)


1.0.4:
------

- [api] The Cluster.Builder#poolingOptions and Cluster.Builder#socketOptions
  are now deprecated. They are replaced by the new withPoolingOptions and
  withSocketOptions methods (JAVA-163).
- [new] A new LatencyAwarePolicy wrapping policy has been added, allowing to
  add latency awareness to a wrapped load balancing policy (JAVA-129).
- [new] Allow defering cluster initialization (Cluster.Builder#deferInitialization)
  (JAVA-161)
- [new] Add truncate statement in query builder (JAVA-117).
- [new] Support empty IN in the query builder (JAVA-106).
- [bug] Fix spurious "No current pool set; this should not happen" error
  message (JAVA-166)
- [bug] Fix potential overflow in RoundRobinPolicy and correctly errors if
  a balancing policy throws (JAVA-184)
- [bug] Don't release Stream ID for timeouted queries (unless we do get back
  the response)
- [bug] Correctly escape identifiers and use fully qualified table names when
  exporting schema as string.


1.0.3:
------

- [api] The query builder now correctly throw an exception when given a value
  of a type it doesn't know about.
- [new] SocketOptions#setReadTimeout allows to set a timeout on how long we
  wait for the answer of one node. See the javadoc for more details.
- [new] New Session#prepare method that takes a Statement.
- [bug] Always take per-query CL, tracing, etc. into account for QueryBuilder
  statements (JAVA-143).
- [bug] Temporary fixup for TimestampType when talking to C* 2.0 nodes.


1.0.2:
------

- [api] Host#getMonitor and all Host.HealthMonitor methods have been
  deprecated. The new Host#isUp method is now prefered to the method
  in the monitor and you should now register Host.StateListener against
  the Cluster object directly (registering against a host HealthMonitor
  was much more limited anyway).
- [new] New serialize/deserialize methods in DataType to serialize/deserialize
  values to/from bytes (JAVA-92).
- [new] New getIndexOf() method in ColumnDefinitions to find the index of
  a given column name (JAVA-128).
- [bug] Fix a bug when thread could get blocked while setting the current
  keyspace (JAVA-131).
- [bug] Quote inet addresses in the query builder since CQL3 requires it
  (JAVA-136)


1.0.1:
------

- [api] Function call handling in the query builder has been modified in a
  backward incompatible way. Function calls are not parsed from string values
  anymore as this wasn't safe. Instead the new 'fcall' method should be used
  (JAVA-100).
- [api] Some typos in method names in PoolingOptions have been fixed in a
  backward incompatible way before the API get widespread.
- [bug] Don't destroy composite partition key with BoundStatement and
  TokenAwarePolicy (JAVA-123).
- [new] null values support in the query builder.
- [new] SSL support (requires C* >= 1.2.1) (JAVA-5).
- [new] Allow generating unlogged batch in the query builder (JAVA-113).
- [improvement] Better error message when no host are available.
- [improvement] Improves performance of the stress example application been.


1.0.0:
------

- [api] The AuthInfoProvider has be (temporarily) removed. Instead, the
  Cluster builder has a new withCredentials() method to provide a username
  and password for use with Cassandra's PasswordAuthenticator. Custom
  authenticator will be re-introduced in a future version but are not
  supported at the moment.
- [api] The isMetricsEnabled() method in Configuration has been replaced by
  getMetricsOptions(). An option to disabled JMX reporting (on by default)
  has been added.
- [bug] Don't make default load balancing policy a static singleton since it
  is stateful (JAVA-91).


1.0.0-RC1:
----------

- [new] Null values are now supported in BoundStatement (but you will need at
  least Cassandra 1.2.3 for it to work). The API of BoundStatement has been
  slightly changed so that not binding a variable is not an error anymore,
  the variable is simply considered null by default. The isReady() method has
  been removed (JAVA-79).
- [improvement] The Cluster/Session shutdown methods now properly block until
  the shutdown is complete. A version with at timeout has been added (JAVA-75).
- [bug] Fix use of CQL3 functions in the query builder (JAVA-44).
- [bug] Fix case where multiple schema changes too quickly wouldn't work
  (only triggered when 0.0.0.0 was use for the rpc_address on the Cassandra
  nodes) (JAVA-77).
- [bug] Fix IllegalStateException thrown due to a reconnection made on an I/O
  thread (JAVA-72).
- [bug] Correctly reports errors during authentication phase (JAVA-82).


1.0.0-beta2:
------------

- [new] Support blob constants, BigInteger, BigDecimal and counter batches in
  the query builder (JAVA-51, JAVA-60, JAVA-58)
- [new] Basic support for custom CQL3 types (JAVA-61)
- [new] Add "execution infos" for a result set (this also move the query
  trace in the new ExecutionInfos object, so users of beta1 will have to
  update) (JAVA-65)
- [bug] Fix failover bug in DCAwareRoundRobinPolicy (JAVA-62)
- [bug] Fix use of bind markers for routing keys in the query builder
  (JAVA-66)


1.0.0-beta1:
------------

- initial release
