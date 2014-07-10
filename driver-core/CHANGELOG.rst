CHANGELOG
=========

2.0.4:
------

- [improvement] Better handling of dead connections (JAVA-204)
- [bug] Fix potential NPE in ControlConnection (JAVA-373)
- [bug] Throws NPE when passed null for a contact point (JAVA-291)
- [bug] Avoid LoadBalancingPolicy onDown+onUp at startup (JAVA-315)
- [bug] Avoid classloader leak in Tomcat (JAVA-343)
- [bug] Avoid deadlock in onAdd/onUp (JAVA-387)
- [bug] Make metadata parsing more lenient (JAVA-377, JAVA-391)
- [bug] Ensure defunct connections are completely closed (JAVA-394)
- [bug] Fix memory and resource leak on closed Sessions (JAVA-342, JAVA-390)


2.0.3:
------

- [new] The new AbsractSession makes mocking of Session easier.
- [new] Allow to trigger a refresh of connected hosts (JAVA-309)
- [new] New Session#getState method allows to grab information on
  which nodes a session is connected to (JAVA-265)
- [new] Add QueryBuilder syntax for tuples in where clauses (syntax
  introduced in Cassandra 2.0.6) (JAVA-327)
- [improvement] Properly validate arguments of PoolingOptions methods
  (JAVA-359)
- [bug] Fix bogus rejection of BigInteger in 'execute with values'
  (JAVA-368)
- [bug] Signal connection failure sooner to avoid missing them
  (JAVA-367)

Merged from 1.0 branch:

- [bug] Fix periodic reconnection to down hosts (JAVA-325)


2.0.2:
------

- [api] The type of the map key returned by NoHostAvailable#getErrors has changed from
  InetAddress to InetSocketAddress. Same for Initializer#getContactPoints return and
  for AuthProvider#newAuthenticator.
- [api] The default load balacing policy is now DCAwareRoundRobinPolicy, and the local
  datacenter is automatically picked based on the first connected node. Furthermore,
  the TokenAwarePolicy is also used by default (JAVA-296)
- [new] New optional AddressTranslater (JAVA-145)
- [bug] Don't remove quotes on keyspace in the query builder (JAVA-321)
- [bug] Fix potential NPE while cluster undergo schema changes (JAVA-320)
- [bug] Fix thread-safety of page fetching (JAVA-319)
- [bug] Fix potential NPE using fetchMoreResults (JAVA-318)

Merged from 1.0 branch:

- [new] Expose the name of the partitioner in use in the cluster metadata (JAVA-179)
- [new] Add new WhiteListPolicy to limit the nodes connected to a particular list
- [improvement] Do not hop DC for LOCAL_* CL in DCAwareRoundRobinPolicy (JAVA-289)
- [bug] Revert back to longs for dates in the query builder (JAVA-313)
- [bug] Don't reconnect to nodes ignored by the load balancing policy (JAVA-314)


2.0.1:
------

- [improvement] Handle the static columns introduced in Cassandra 2.0.6 (JAVA-278)
- [improvement] Add Cluster#newSession method to create Session without connecting
  right away (JAVA-208)
- [bug] Add missing iso8601 patterns for parsing dates (JAVA-279)
- [bug] Properly parse BytesType as the blob type
- [bug] Potential NPE when parsing schema of pre-CQL tables of C* 1.2 nodes (JAVA-280)

Merged from 1.0 branch:

- [bug] LatencyAwarePolicy.Builder#withScale doesn't set the scale (JAVA-275)
- [new] Add methods to check if a Cluster/Session instance has been closed already (JAVA-114)


2.0.0:
------

- [api] Case sensitive identifier by default in Metadata (JAVA-269)
- [bug] Fix potential NPE in Cluster#connect (JAVA-274)

Merged from 1.0 branch:

- [bug] Always return the PreparedStatement object that is cache internally (JAVA-263)
- [bug] Fix race when multiple connect are done in parallel (JAVA-261)
- [bug] Don't connect at all to nodes that are ignored by the load balancing
  policy (JAVA-270)


2.0.0-rc3:
----------

- [improvement] The protocol version 1 is now supported (features only supported by the
  version 2 of the protocol throw UnsupportedFeatureException).
- [improvement] Make most main objects interface to facilitate testing/mocking (JAVA-195)
- [improvement] Adds new getStatements and clear methods to BatchStatement.
- [api] Renamed shutdown to closeAsync and ShutdownFuture to CloseFuture. Clustering
  and Session also now implement Closeable (JAVA-247).
- [bug] Fix potential thread leaks when shutting down Metrics (JAVA-232)
- [bug] Fix potential NPE in HostConnectionPool (JAVA-231)
- [bug] Avoid NPE when node is in an unconfigured DC (JAVA-244)
- [bug] Don't block for scheduled reconnections on Cluster#close (JAVA-258)

Merged from 1.0 branch:

- [new] Added Session#prepareAsync calls (JAVA-224)
- [new] Added Cluster#getLoggedKeyspace (JAVA-249)
- [improvement] Avoid preparing a statement multiple time per host with multiple sessions
- [bug] Make sure connections are returned to the right pools (JAVA-255)
- [bug] Use date string in query build to work-around CASSANDRA-6718 (JAVA-264)


2.0.0-rc2:
----------

- [new] Add LOCAL_ONE consistency level support (requires using C* 2.0.2+) (JAVA-207)
- [bug] Fix parsing of counter types (JAVA-219)
- [bug] Fix missing whitespace for IN clause in the query builder (JAVA-218)
- [bug] Fix replicas computation for token aware balancing (JAVA-221)

Merged from 1.0 branch:

- [bug] Fix regression from JAVA-201 (JAVA-213)
- [improvement] New getter to obtain a snapshot of the scores maintained by
  LatencyAwarePolicy.


2.0.0-rc1:
----------

- [new] Mark compression dependencies optional in maven (JAVA-199).
- [api] Renamed TableMetadata#getClusteringKey to TableMetadata#getClusteringColumns.

Merged from 1.0 branch:

- [new] OSGi bundle (JAVA-142)
- [improvement] Make collections returned by Row immutable (JAVA-205)
- [improvement] Limit internal thread pool size (JAVA-203)
- [bug] Don't retain unused PreparedStatement in memory (JAVA-201)
- [bug] Add missing clustering order info in TableMetadata
- [bug] Allow bind markers for collections in the query builder (JAVA-196)


2.0.0-beta2:
------------

- [api] BoundStatement#setX(String, X) methods now set all values (if there is
  more than one) having the provided name, not just the first occurence.
- [api] The Authenticator interface now has a onAuthenticationSuccess method that
  allows to handle the potential last token sent by the server.
- [new] The query builder don't serialize large values to strings anymore by
  default by making use the new ability to send values alongside the query string.
- [new] The query builder has been updated for new CQL features (JAVA-140).
- [bug] Fix exception when a conditional write timeout C* side.
- [bug] Ensure connection is created when Cluster metadata are asked for
  (JAVA-182).
- [bug] Fix potential NPE during authentication (JAVA-187)


2.0.0-beta1:
-----------

- [api] The 2.0 version is an API-breaking upgrade of the driver. While most
  of the breaking changes are minor, there are too numerous to be listed here
  and you are encouraged to look at the Upgrade_guide_to_2.0 file that describe
  those changes in details.
- [new] LZ4 compression is supported for the protocol.
- [new] The driver does not depend on cassandra-all anymore (JAVA-39)
- [new] New BatchStatement class allows to execute batch other statements.
- [new] Large ResultSet are now paged (incrementally fetched) by default.
- [new] SimpleStatement support values for bind-variables, to allow
  prepare+execute behavior with one roundtrip.
- [new] Query parameters defaults (Consistency level, page size, ...) can be
  configured globally.
- [new] New Cassandra 2.0 SERIAL and LOCAL_SERIAL consistency levels are
  supported.
- [new] Cluster#shutdown now waits for ongoing queries to complete by default
  (JAVA-116).
- [new] Generic authentication through SASL is now exposed.
- [bug] TokenAwarePolicy now takes all replica into account, instead of only the
  first one (JAVA-88).


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
