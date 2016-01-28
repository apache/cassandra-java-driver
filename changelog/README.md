## Changelog

### 3.0.0

- [bug] JAVA-1034: fix metadata parser for collections of custom types.
- [improvement] JAVA-1035: Expose host broadcast_address and listen_address if available.
- [new feature] JAVA-1037: Allow named parameters in simple statements.
- [improvement] JAVA-1033: Allow per-statement read timeout.
- [improvement] JAVA-1042: Include DSE version and workload in Host data.

Merged from 2.1 branch:

- [improvement] JAVA-1030: Log token to replica map computation times.
- [bug] JAVA-1039: Minor bugs in Event Debouncer.


### 3.0.0-rc1

- [bug] JAVA-890: fix mapper for case-sensitive UDT.


### 3.0.0-beta1

- [bug] JAVA-993: Support for "custom" types after CASSANDRA-10365.
- [bug] JAVA-999: Handle unset parameters in QueryLogger.
- [bug] JAVA-998: SchemaChangeListener not invoked for Functions or Aggregates having UDT arguments.
- [bug] JAVA-1009: use CL ONE to compute query plan when reconnecting
  control connection.
- [improvement] JAVA-1003: Change default consistency level to LOCAL_ONE (amends JAVA-926).
- [improvement] JAVA-863: Idempotence propagation in prepared statements.
- [improvement] JAVA-996: Make CodecRegistry available to ProtocolDecoder.
- [bug] JAVA-819: Driver shouldn't retry on client timeout if statement is not idempotent.
- [improvement] JAVA-1007: Make SimpleStatement and QueryBuilder "detached" again.

Merged from 2.1 branch:

- [improvement] JAVA-989: Include keyspace name when invalid replication found when generating token map.
- [improvement] JAVA-664: Reduce heap consumption for TokenMap.
- [bug] JAVA-994: Don't call on(Up|Down|Add|Remove) methods if Cluster is closed/closing.


### 3.0.0-alpha5

- [improvement] JAVA-958: Make TableOrView.Order visible.
- [improvement] JAVA-968: Update metrics to the latest version.
- [improvement] JAVA-965: Improve error handling for when a non-type 1 UUID is given to bind() on a timeuuid column.
- [improvement] JAVA-885: Pass the authenticator name from the server to the auth provider.
- [improvement] JAVA-961: Raise an exception when an older version of guava (<16.01) is found.
- [bug] JAVA-972: TypeCodec.parse() implementations should be case insensitive when checking for keyword NULL.
- [bug] JAVA-971: Make type codecs invariant.
- [bug] JAVA-986: Update documentation links to reference 3.0.
- [improvement] JAVA-841: Refactor SSLOptions API.
- [improvement] JAVA-948: Don't limit cipher suites by default.
- [improvement] JAVA-917: Document SSL configuration.
- [improvement] JAVA-936: Adapt schema metadata parsing logic to new storage format of CQL types in C* 3.0.
- [new feature] JAVA-846: Provide custom codecs library as an extra module.
- [new feature] JAVA-742: Codec Support for JSON.
- [new feature] JAVA-606: Codec support for Java 8.
- [new feature] JAVA-565: Codec support for Java arrays.
- [new feature] JAVA-605: Codec support for Java enums.
- [bug] JAVA-884: Fix UDT mapper to process fields in the correct order.

Merged from 2.1 branch:

- [bug] JAVA-854: avoid early return in Cluster.init when a node doesn't support the protocol version.
- [bug] JAVA-978: Fix quoting issue that caused Mapper.getTableMetadata() to return null.
- [improvement] JAVA-920: Downgrade "error creating pool" message to WARN.
- [bug] JAVA-954: Don't trigger reconnection before initialization complete.
- [improvement] JAVA-914: Avoid rejected tasks at shutdown.
- [improvement] JAVA-921: Add SimpleStatement.getValuesCount().
- [bug] JAVA-901: Move call to connection.release() out of cancelHandler.
- [bug] JAVA-960: Avoid race in control connection shutdown.
- [bug] JAVA-656: Fix NPE in ControlConnection.updateLocationInfo.
- [bug] JAVA-966: Count uninitialized connections in conviction policy.
- [improvement] JAVA-917: Document SSL configuration.
- [improvement] JAVA-652: Add DCAwareRoundRobinPolicy builder.
- [improvement] JAVA-808: Add generic filtering policy that can be used to exclude specific DCs.
- [bug] JAVA-988: Metadata.handleId should handle escaped double quotes.
- [bug] JAVA-983: QueryBuilder cannot handle collections containing function calls.


### 3.0.0-alpha4

- [improvement] JAVA-926: Change default consistency level to LOCAL_QUORUM.
- [bug] JAVA-942: Fix implementation of UserType.hashCode().
- [improvement] JAVA-877: Don't delay UP/ADDED notifications if protocol version = V4.
- [improvement] JAVA-938: Parse 'extensions' column in table metadata.
- [bug] JAVA-900: Fix Configuration builder to allow disabled metrics.
- [new feature] JAVA-902: Prepare API for async query trace.
- [new feature] JAVA-930: Add BoundStatement#unset.
- [bug] JAVA-946: Make table metadata options class visible.
- [bug] JAVA-939: Add crcCheckChance to TableOptionsMetadata#equals/hashCode.
- [bug] JAVA-922: Make TypeCodec return mutable collections.
- [improvement] JAVA-932: Limit visibility of codec internals.
- [improvement] JAVA-934: Warn if a custom codec collides with an existing one.
- [improvement] JAVA-940: Allow typed getters/setters to target any CQL type.
- [bug] JAVA-950: Fix Cluster.connect with a case-sensitive keyspace.
- [bug] JAVA-953: Fix MaterializedViewMetadata when base table name is case sensitive.


### 3.0.0-alpha3

- [new feature] JAVA-571: Support new system tables in C* 3.0.
- [improvement] JAVA-919: Move crc_check_chance out of compressions options.

Merged from 2.0 branch:

- [improvement] JAVA-718: Log streamid at the trace level on sending request and receiving response.
- [bug] JAVA-796: Fix SpeculativeExecutionPolicy.init() and close() are never called.
- [improvement] JAVA-710: Suppress unnecessary warning at shutdown.
- [improvement] #340: Allow DNS name with multiple A-records as contact point.
- [bug] JAVA-794: Allow tracing across multiple result pages.
- [bug] JAVA-737: DowngradingConsistencyRetryPolicy ignores write timeouts.
- [bug] JAVA-736: Forbid bind marker in QueryBuilder add/append/prepend.
- [bug] JAVA-712: Prevent QueryBuilder.quote() from applying duplicate double quotes.
- [bug] JAVA-688: Prevent QueryBuilder from trying to serialize raw string.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-702: Warn when ReplicationStrategy encounters invalid
  replication factors.
- [improvement] JAVA-662: Add PoolingOptions method to set both core and max
  connections.
- [improvement] JAVA-766: Do not include epoll JAR in binary distribution.
- [improvement] JAVA-726: Optimize internal copies of Request objects.
- [bug] JAVA-815: Preserve tracing across retries.
- [improvement] JAVA-709: New RetryDecision.tryNextHost().
- [bug] JAVA-733: Handle function calls and raw strings as non-idempotent in QueryBuilder.
- [improvement] JAVA-765: Provide API to retrieve values of a Parameterized SimpleStatement.
- [improvement] JAVA-827: implement UPDATE .. IF EXISTS in QueryBuilder.
- [improvement] JAVA-618: Randomize contact points list to prevent hotspots.
- [improvement] JAVA-720: Surface the coordinator used on query failure.
- [bug] JAVA-792: Handle contact points removed during init.
- [improvement] JAVA-719: Allow PlainTextAuthProvider to change its credentials at runtime.
- [new feature] JAVA-151: Make it possible to register for SchemaChange Events.
- [improvement] JAVA-861: Downgrade "Asked to rebuild table" log from ERROR to INFO level.
- [improvement] JAVA-797: Provide an option to prepare statements only on one node.
- [improvement] JAVA-658: Provide an option to not re-prepare all statements in onUp.
- [improvement] JAVA-853: Customizable creation of netty timer.
- [bug] JAVA-859: Avoid quadratic ring processing with invalid replication factors.
- [improvement] JAVA-657: Debounce control connection queries.
- [bug] JAVA-784: LoadBalancingPolicy.distance() called before init().
- [new feature] JAVA-828: Make driver-side metadata optional.
- [improvement] JAVA-544: Allow hosts to remain partially up.
- [improvement] JAVA-821, JAVA-822: Remove internal blocking calls and expose async session
  creation.
- [improvement] JAVA-725: Use parallel calls when re-preparing statement on other
  hosts.
- [bug] JAVA-629: Don't use connection timeout for unrelated internal queries.
- [bug] JAVA-892: Fix NPE in speculative executions when metrics disabled.


### 3.0.0-alpha2

- [new feature] JAVA-875, JAVA-882: Move secondary index metadata out of column definitions.

Merged from 2.2 branch:

- [bug] JAVA-847: Propagate CodecRegistry to nested UDTs.
- [improvement] JAVA-848: Ability to store a default, shareable CodecRegistry
  instance.
- [bug] JAVA-880: Treat empty ByteBuffers as empty values in TupleCodec and
  UDTCodec.


### 3.0.0-alpha1

- [new feature] JAVA-876: Support new system tables in C* 3.0.0-alpha1.

Merged from 2.2 branch:

- [improvement] JAVA-810: Rename DateWithoutTime to LocalDate.
- [bug] JAVA-816: DateCodec does not format values correctly.
- [bug] JAVA-817: TimeCodec does not format values correctly.
- [bug] JAVA-818: TypeCodec.getDataTypeFor() does not handle LocalDate instances.
- [improvement] JAVA-836: Make ResultSet#fetchMoreResult return a
  ListenableFuture<ResultSet>.
- [improvement] JAVA-843: Disable frozen checks in mapper.
- [improvement] JAVA-721: Allow user to register custom type codecs.
- [improvement] JAVA-722: Support custom type codecs in mapper.


### 2.2.0-rc3

- [bug] JAVA-847: Propagate CodecRegistry to nested UDTs.
- [improvement] JAVA-848: Ability to store a default, shareable CodecRegistry
  instance.
- [bug] JAVA-880: Treat empty ByteBuffers as empty values in TupleCodec and
  UDTCodec.


### 2.2.0-rc2

- [improvement] JAVA-810: Rename DateWithoutTime to LocalDate.
- [bug] JAVA-816: DateCodec does not format values correctly.
- [bug] JAVA-817: TimeCodec does not format values correctly.
- [bug] JAVA-818: TypeCodec.getDataTypeFor() does not handle LocalDate instances.
- [improvement] JAVA-836: Make ResultSet#fetchMoreResult return a
  ListenableFuture<ResultSet>.
- [improvement] JAVA-843: Disable frozen checks in mapper.
- [improvement] JAVA-721: Allow user to register custom type codecs.
- [improvement] JAVA-722: Support custom type codecs in mapper.

Merged from 2.1 branch:

- [bug] JAVA-834: Special case check for 'null' string in index_options column.
- [improvement] JAVA-835: Allow accessor methods with less parameters in case
  named bind markers are repeated.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-715: Make NativeColumnType a top-level class.
- [improvement] JAVA-700: Expose ProtocolVersion#toInt.
- [bug] JAVA-542: Handle void return types in accessors.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-713: HashMap throws an OOM Exception when logging level is set to TRACE.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-732: Expose KEYS and FULL indexing options in IndexMetadata.
- [improvement] JAVA-589: Allow @Enumerated in Accessor method parameters.
- [improvement] JAVA-554: Allow access to table metadata from Mapper.
- [improvement] JAVA-661: Provide a way to map computed fields.
- [improvement] JAVA-824: Ignore missing columns in mapper.
- [bug] JAVA-724: Preserve default timestamp for retries and speculative executions.
- [improvement] JAVA-738: Use same pool implementation for protocol v2 and v3.
- [improvement] JAVA-677: Support CONTAINS / CONTAINS KEY in QueryBuilder.
- [improvement] JAVA-477/JAVA-540: Add USING options in mapper for delete and save
  operations.
- [improvement] JAVA-473: Add mapper option to configure whether to save null fields.

Merged from 2.0 branch:

- [bug] JAVA-737: DowngradingConsistencyRetryPolicy ignores write timeouts.
- [bug] JAVA-736: Forbid bind marker in QueryBuilder add/append/prepend.
- [bug] JAVA-712: Prevent QueryBuilder.quote() from applying duplicate double quotes.
- [bug] JAVA-688: Prevent QueryBuilder from trying to serialize raw string.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-702: Warn when ReplicationStrategy encounters invalid
  replication factors.
- [improvement] JAVA-662: Add PoolingOptions method to set both core and max
  connections.
- [improvement] JAVA-766: Do not include epoll JAR in binary distribution.
- [improvement] JAVA-726: Optimize internal copies of Request objects.
- [bug] JAVA-815: Preserve tracing across retries.
- [improvement] JAVA-709: New RetryDecision.tryNextHost().
- [bug] JAVA-733: Handle function calls and raw strings as non-idempotent in QueryBuilder.


### 2.2.0-rc1

- [new feature] JAVA-783: Protocol V4 enum support.
- [new feature] JAVA-776: Use PK columns in protocol v4 PREPARED response.
- [new feature] JAVA-777: Distinguish NULL and UNSET values.
- [new feature] JAVA-779: Add k/v payload for 3rd party usage.
- [new feature] JAVA-780: Expose server-side warnings on ExecutionInfo.
- [new feature] JAVA-749: Expose new read/write failure exceptions.
- [new feature] JAVA-747: Expose function and aggregate metadata.
- [new feature] JAVA-778: Add new client exception for CQL function failure.
- [improvement] JAVA-700: Expose ProtocolVersion#toInt.
- [new feature] JAVA-404: Support new C* 2.2 CQL date and time types.

Merged from 2.1 branch:

- [improvement] JAVA-782: Unify "Target" enum for schema elements.


### 2.1.10 (in progress)

- [bug] JAVA-988: Metadata.handleId should handle escaped double quotes.
- [improvement] JAVA-1030: Log token to replica map computation times.
- [bug] JAVA-1039: Minor bugs in Event Debouncer.

Merged from 2.0 branch:

- [bug] JAVA-994: Don't call on(Up|Down|Add|Remove) methods if Cluster is closed/closing.
- [improvement] JAVA-805: Document that metrics are null until Cluster is initialized.


### 2.1.9

- [bug] JAVA-942: Fix implementation of UserType.hashCode().
- [bug] JAVA-854: avoid early return in Cluster.init when a node doesn't support the protocol version.
- [bug] JAVA-978: Fix quoting issue that caused Mapper.getTableMetadata() to return null.

Merged from 2.0 branch:

- [bug] JAVA-950: Fix Cluster.connect with a case-sensitive keyspace.
- [improvement] JAVA-920: Downgrade "error creating pool" message to WARN.
- [bug] JAVA-954: Don't trigger reconnection before initialization complete.
- [improvement] JAVA-914: Avoid rejected tasks at shutdown.
- [improvement] JAVA-921: Add SimpleStatement.getValuesCount().
- [bug] JAVA-901: Move call to connection.release() out of cancelHandler.
- [bug] JAVA-960: Avoid race in control connection shutdown.
- [bug] JAVA-656: Fix NPE in ControlConnection.updateLocationInfo.
- [bug] JAVA-966: Count uninitialized connections in conviction policy.
- [improvement] JAVA-917: Document SSL configuration.
- [improvement] JAVA-652: Add DCAwareRoundRobinPolicy builder.
- [improvement] JAVA-808: Add generic filtering policy that can be used to exclude specific DCs.


### 2.1.8

Merged from 2.0 branch:

- [improvement] JAVA-718: Log streamid at the trace level on sending request and receiving response.

- [bug] JAVA-796: Fix SpeculativeExecutionPolicy.init() and close() are never called.
- [improvement] JAVA-710: Suppress unnecessary warning at shutdown.
- [improvement] #340: Allow DNS name with multiple A-records as contact point.
- [bug] JAVA-794: Allow tracing across multiple result pages.
- [bug] JAVA-737: DowngradingConsistencyRetryPolicy ignores write timeouts.
- [bug] JAVA-736: Forbid bind marker in QueryBuilder add/append/prepend.
- [bug] JAVA-712: Prevent QueryBuilder.quote() from applying duplicate double quotes.
- [bug] JAVA-688: Prevent QueryBuilder from trying to serialize raw string.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-702: Warn when ReplicationStrategy encounters invalid
  replication factors.
- [improvement] JAVA-662: Add PoolingOptions method to set both core and max
  connections.
- [improvement] JAVA-766: Do not include epoll JAR in binary distribution.
- [improvement] JAVA-726: Optimize internal copies of Request objects.
- [bug] JAVA-815: Preserve tracing across retries.
- [improvement] JAVA-709: New RetryDecision.tryNextHost().
- [bug] JAVA-733: Handle function calls and raw strings as non-idempotent in QueryBuilder.
- [improvement] JAVA-765: Provide API to retrieve values of a Parameterized SimpleStatement.
- [improvement] JAVA-827: implement UPDATE .. IF EXISTS in QueryBuilder.
- [improvement] JAVA-618: Randomize contact points list to prevent hotspots.
- [improvement] JAVA-720: Surface the coordinator used on query failure.
- [bug] JAVA-792: Handle contact points removed during init.
- [improvement] JAVA-719: Allow PlainTextAuthProvider to change its credentials at runtime.
- [new feature] JAVA-151: Make it possible to register for SchemaChange Events.
- [improvement] JAVA-861: Downgrade "Asked to rebuild table" log from ERROR to INFO level.
- [improvement] JAVA-797: Provide an option to prepare statements only on one node.
- [improvement] JAVA-658: Provide an option to not re-prepare all statements in onUp.
- [improvement] JAVA-853: Customizable creation of netty timer.
- [bug] JAVA-859: Avoid quadratic ring processing with invalid replication factors.
- [improvement] JAVA-657: Debounce control connection queries.
- [bug] JAVA-784: LoadBalancingPolicy.distance() called before init().
- [new feature] JAVA-828: Make driver-side metadata optional.
- [improvement] JAVA-544: Allow hosts to remain partially up.
- [improvement] JAVA-821, JAVA-822: Remove internal blocking calls and expose async session
  creation.
- [improvement] JAVA-725: Use parallel calls when re-preparing statement on other
  hosts.
- [bug] JAVA-629: Don't use connection timeout for unrelated internal queries.
- [bug] JAVA-892: Fix NPE in speculative executions when metrics disabled.


### 2.1.7.1

- [bug] JAVA-834: Special case check for 'null' string in index_options column.
- [improvement] JAVA-835: Allow accessor methods with less parameters in case
  named bind markers are repeated.


### 2.1.7

- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-715: Make NativeColumnType a top-level class.
- [improvement] JAVA-782: Unify "Target" enum for schema elements.
- [improvement] JAVA-700: Expose ProtocolVersion#toInt.
- [bug] JAVA-542: Handle void return types in accessors.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-713: HashMap throws an OOM Exception when logging level is set to TRACE.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-732: Expose KEYS and FULL indexing options in IndexMetadata.
- [improvement] JAVA-589: Allow @Enumerated in Accessor method parameters.
- [improvement] JAVA-554: Allow access to table metadata from Mapper.
- [improvement] JAVA-661: Provide a way to map computed fields.
- [improvement] JAVA-824: Ignore missing columns in mapper.
- [bug] JAVA-724: Preserve default timestamp for retries and speculative executions.
- [improvement] JAVA-738: Use same pool implementation for protocol v2 and v3.
- [improvement] JAVA-677: Support CONTAINS / CONTAINS KEY in QueryBuilder.
- [improvement] JAVA-477/JAVA-540: Add USING options in mapper for delete and save
  operations.
- [improvement] JAVA-473: Add mapper option to configure whether to save null fields.

Merged from 2.0 branch:

- [bug] JAVA-737: DowngradingConsistencyRetryPolicy ignores write timeouts.
- [bug] JAVA-736: Forbid bind marker in QueryBuilder add/append/prepend.
- [bug] JAVA-712: Prevent QueryBuilder.quote() from applying duplicate double quotes.
- [bug] JAVA-688: Prevent QueryBuilder from trying to serialize raw string.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-702: Warn when ReplicationStrategy encounters invalid
  replication factors.
- [improvement] JAVA-662: Add PoolingOptions method to set both core and max
  connections.
- [improvement] JAVA-766: Do not include epoll JAR in binary distribution.
- [improvement] JAVA-726: Optimize internal copies of Request objects.
- [bug] JAVA-815: Preserve tracing across retries.
- [improvement] JAVA-709: New RetryDecision.tryNextHost().
- [bug] JAVA-733: Handle function calls and raw strings as non-idempotent in QueryBuilder.


### 2.1.6

Merged from 2.0 branch:

- [new feature] JAVA-584: Add getObject to BoundStatement and Row.
- [improvement] JAVA-419: Improve connection pool resizing algorithm.
- [bug] JAVA-599: Fix race condition between pool expansion and shutdown.
- [improvement] JAVA-622: Upgrade Netty to 4.0.27.
- [improvement] JAVA-562: Coalesce frames before flushing them to the connection.
- [improvement] JAVA-583: Rename threads to indicate that they are for the driver.
- [new feature] JAVA-550: Expose paging state.
- [new feature] JAVA-646: Slow Query Logger.
- [improvement] JAVA-698: Exclude some errors from measurements in LatencyAwarePolicy.
- [bug] JAVA-641: Fix issue when executing a PreparedStatement from another cluster.
- [improvement] JAVA-534: Log keyspace xxx does not exist at WARN level.
- [improvement] JAVA-619: Allow Cluster subclasses to delegate to another instance.
- [new feature] JAVA-669: Expose an API to check for schema agreement after a
  schema-altering statement.
- [improvement] JAVA-692: Make connection and pool creation fully async.
- [improvement] JAVA-505: Optimize connection use after reconnection.
- [improvement] JAVA-617: Remove "suspected" mechanism.
- [improvement] reverts JAVA-425: Don't mark connection defunct on client timeout.
- [new feature] JAVA-561: Speculative query executions.
- [bug] JAVA-666: Release connection before completing the ResultSetFuture.
- [new feature BETA] JAVA-723: Percentile-based variant of query logger and speculative
  executions.
- [bug] JAVA-734: Fix buffer leaks when compression is enabled.
- [improvement] JAVA-756: Use Netty's pooled ByteBufAllocator by default.
- [improvement] JAVA-759: Expose "unsafe" paging state API.
- [bug] JAVA-768: Prevent race during pool initialization.


### 2.1.5

- [bug] JAVA-575: Authorize Null parameter in Accessor method.
- [improvement] JAVA-570: Support C* 2.1.3's nested collections.
- [bug] JAVA-612: Fix checks on mapped collection types.
- [bug] JAVA-672: Fix QueryBuilder.putAll() when the collection contains UDTs.

Merged from 2.0 branch:

- [new feature] JAVA-518: Add AddressTranslater for EC2 multi-region deployment.
- [improvement] JAVA-533: Add connection heartbeat.
- [improvement] JAVA-568: Reduce level of logs on missing rpc_address.
- [improvement] JAVA-312, JAVA-681: Expose node token and range information.
- [bug] JAVA-595: Fix cluster name mismatch check at startup.
- [bug] JAVA-620: Fix guava dependency when using OSGI.
- [bug] JAVA-678: Fix handling of DROP events when ks name is case-sensitive.
- [improvement] JAVA-631: Use List<?> instead of List<Object> in QueryBuilder API.
- [improvement] JAVA-654: Exclude Netty POM from META-INF in shaded JAR.
- [bug] JAVA-655: Quote single quotes contained in table comments in asCQLQuery method.
- [bug] JAVA-684: Empty TokenRange returned in a one token cluster.
- [improvement] JAVA-687: Expose TokenRange#contains.
- [bug] JAVA-614: Prevent race between cancellation and query completion.
- [bug] JAVA-632: Prevent cancel and timeout from cancelling unrelated ResponseHandler if
  streamId was already released and reused.
- [bug] JAVA-642: Fix issue when newly opened pool fails before we could mark the node UP.
- [bug] JAVA-613: Fix unwanted LBP notifications when a contact host is down.
- [bug] JAVA-651: Fix edge cases where a connection was released twice.
- [bug] JAVA-653: Fix edge cases in query cancellation.


### 2.1.4

Merged from 2.0 branch:

- [improvement] JAVA-538: Shade Netty dependency.
- [improvement] JAVA-543: Target schema refreshes more precisely.
- [bug] JAVA-546: Don't check rpc_address for control host.
- [improvement] JAVA-409: Improve message of NoHostAvailableException.
- [bug] JAVA-556: Rework connection reaper to avoid deadlock.
- [bug] JAVA-557: Avoid deadlock when multiple connections to the same host get write
  errors.
- [improvement] JAVA-504: Make shuffle=true the default for TokenAwarePolicy.
- [bug] JAVA-577: Fix bug when SUSPECT reconnection succeeds, but one of the pooled
  connections fails while bringing the node back up.
- [bug] JAVA-419: JAVA-587: Prevent faulty control connection from ignoring reconnecting hosts.
- temporarily revert "Add idle timeout to the connection pool".
- [bug] JAVA-593: Ensure updateCreatedPools does not add pools for suspected hosts.
- [bug] JAVA-594: Ensure state change notifications for a given host are handled serially.
- [bug] JAVA-597: Ensure control connection reconnects when control host is removed.


### 2.1.3

- [bug] JAVA-510: Ignore static fields in mapper.
- [bug] JAVA-509: Fix UDT parsing at init when using the default protocol version.
- [bug] JAVA-495: Fix toString, equals and hashCode on accessor proxies.
- [bug] JAVA-528: Allow empty name on Column and Field annotations.

Merged from 2.0 branch:

- [bug] JAVA-497: Ensure control connection does not trigger concurrent reconnects.
- [improvement] JAVA-472: Keep trying to reconnect on authentication errors.
- [improvement] JAVA-463: Expose close method on load balancing policy.
- [improvement] JAVA-459: Allow load balancing policy to trigger refresh for a single host.
- [bug] JAVA-493: Expose an API to cancel reconnection attempts.
- [bug] JAVA-503: Fix NPE when a connection fails during pool construction.
- [improvement] JAVA-423: Log datacenter name in DCAware policy's init when it is explicitly provided.
- [improvement] JAVA-504: Shuffle the replicas in TokenAwarePolicy.newQueryPlan.
- [improvement] JAVA-507: Make schema agreement wait tuneable.
- [improvement] JAVA-494: Document how to inject the driver metrics into another registry.
- [improvement] JAVA-419: Add idle timeout to the connection pool.
- [bug] JAVA-516: LatencyAwarePolicy does not shutdown executor on invocation of close.
- [improvement] JAVA-451: Throw an exception when DCAwareRoundRobinPolicy is built with
  an explicit but null or empty local datacenter.
- [bug] JAVA-511: Fix check for local contact points in DCAware policy's init.
- [improvement] JAVA-457: Make timeout on saturated pool customizable.
- [improvement] JAVA-521: Downgrade Guava to 14.0.1.
- [bug] JAVA-526: Fix token awareness for case-sensitive keyspaces and tables.
- [bug] JAVA-515: Check maximum number of values passed to SimpleStatement.
- [improvement] JAVA-532: Expose the driver version through the API.
- [improvement] JAVA-522: Optimize session initialization when some hosts are not
  responsive.


### 2.1.2

- [improvement] JAVA-361, JAVA-364, JAVA-467: Support for native protocol v3.
- [bug] JAVA-454: Fix UDT fields of type inet in QueryBuilder.
- [bug] JAVA-455: Exclude transient fields from Frozen checks.
- [bug] JAVA-453: Fix handling of null collections in mapper.
- [improvement] JAVA-452: Make implicit column names case-insensitive in mapper.
- [bug] JAVA-433: Fix named bind markers in QueryBuilder.
- [bug] JAVA-458: Fix handling of BigInteger in object mapper.
- [bug] JAVA-465: Ignore synthetic fields in mapper.
- [improvement] JAVA-451: Throw an exception when DCAwareRoundRobinPolicy is built with
  an explicit but null or empty local datacenter.
- [improvement] JAVA-469: Add backwards-compatible DataType.serialize methods.
- [bug] JAVA-487: Handle null enum fields in object mapper.
- [bug] JAVA-499: Handle null UDT fields in object mapper.

Merged from 2.0 branch:

- [bug] JAVA-449: Handle null pool in PooledConnection.release.
- [improvement] JAVA-425: Defunct connection on request timeout.
- [improvement] JAVA-426: Try next host when we get a SERVER_ERROR.
- [bug] JAVA-449, JAVA-460, JAVA-471: Handle race between query timeout and completion.
- [bug] JAVA-496: Fix DCAwareRoundRobinPolicy datacenter auto-discovery.


### 2.1.1

- [new] JAVA-441: Support for new "frozen" keyword.

Merged from 2.0 branch:

- [bug] JAVA-397: Check cluster name when connecting to a new node.
- [bug] JAVA-326: Add missing CAS delete support in QueryBuilder.
- [bug] JAVA-363: Add collection and data length checks during serialization.
- [improvement] JAVA-329: Surface number of retries in metrics.
- [bug] JAVA-428: Do not use a host when no rpc_address found for it.
- [improvement] JAVA-358: Add ResultSet.wasApplied() for conditional queries.
- [bug] JAVA-349: Fix negative HostConnectionPool open count.
- [improvement] JAVA-436: Log more connection details at trace and debug levels.
- [bug] JAVA-445: Fix cluster shutdown.


### 2.1.0

- [bug] JAVA-408: ClusteringColumn annotation not working with specified ordering.
- [improvement] JAVA-410: Fail BoundStatement if null values are not set explicitly.
- [bug] JAVA-416: Handle UDT and tuples in BuiltStatement.toString.

Merged from 2.0 branch:

- [bug] JAVA-407: Release connections on ResultSetFuture#cancel.
- [bug] JAVA-393: Fix handling of SimpleStatement with values in query builder
  batches.
- [bug] JAVA-417: Ensure pool is properly closed in onDown.
- [bug] JAVA-415: Fix tokenMap initialization at startup.
- [bug] JAVA-418: Avoid deadlock on close.


### 2.1.0-rc1

Merged from 2.0 branch:

- [bug] JAVA-394: Ensure defunct connections are completely closed.
- [bug] JAVA-342, JAVA-390: Fix memory and resource leak on closed Sessions.


### 2.1.0-beta1

- [new] Support for User Defined Types and tuples
- [new] Simple object mapper

Merged from 2.0 branch: everything up to 2.0.3 (included), and the following.

- [improvement] JAVA-204: Better handling of dead connections.
- [bug] JAVA-373: Fix potential NPE in ControlConnection.
- [bug] JAVA-291: Throws NPE when passed null for a contact point.
- [bug] JAVA-315: Avoid LoadBalancingPolicy onDown+onUp at startup.
- [bug] JAVA-343: Avoid classloader leak in Tomcat.
- [bug] JAVA-387: Avoid deadlock in onAdd/onUp.
- [bug] JAVA-377, JAVA-391: Make metadata parsing more lenient.


### 2.0.13 (in progress)

- [bug] JAVA-994: Don't call on(Up|Down|Add|Remove) methods if Cluster is closed/closing.
- [improvement] JAVA-805: Document that metrics are null until Cluster is initialized.

### 2.0.12

- [bug] JAVA-950: Fix Cluster.connect with a case-sensitive keyspace.
- [improvement] JAVA-920: Downgrade "error creating pool" message to WARN.
- [bug] JAVA-954: Don't trigger reconnection before initialization complete.
- [improvement] JAVA-914: Avoid rejected tasks at shutdown.
- [improvement] JAVA-921: Add SimpleStatement.getValuesCount().
- [bug] JAVA-901: Move call to connection.release() out of cancelHandler.
- [bug] JAVA-960: Avoid race in control connection shutdown.
- [bug] JAVA-656: Fix NPE in ControlConnection.updateLocationInfo.
- [bug] JAVA-966: Count uninitialized connections in conviction policy.
- [improvement] JAVA-917: Document SSL configuration.
- [improvement] JAVA-652: Add DCAwareRoundRobinPolicy builder.
- [improvement] JAVA-808: Add generic filtering policy that can be used to exclude specific DCs.


### 2.0.11

- [improvement] JAVA-718: Log streamid at the trace level on sending request and receiving response.
- [bug] JAVA-796: Fix SpeculativeExecutionPolicy.init() and close() are never called.
- [improvement] JAVA-710: Suppress unnecessary warning at shutdown.
- [improvement] #340: Allow DNS name with multiple A-records as contact point.
- [bug] JAVA-794: Allow tracing across multiple result pages.
- [bug] JAVA-737: DowngradingConsistencyRetryPolicy ignores write timeouts.
- [bug] JAVA-736: Forbid bind marker in QueryBuilder add/append/prepend.
- [bug] JAVA-712: Prevent QueryBuilder.quote() from applying duplicate double quotes.
- [bug] JAVA-688: Prevent QueryBuilder from trying to serialize raw string.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-702: Warn when ReplicationStrategy encounters invalid
  replication factors.
- [improvement] JAVA-662: Add PoolingOptions method to set both core and max
  connections.
- [improvement] JAVA-766: Do not include epoll JAR in binary distribution.
- [improvement] JAVA-726: Optimize internal copies of Request objects.
- [bug] JAVA-815: Preserve tracing across retries.
- [improvement] JAVA-709: New RetryDecision.tryNextHost().
- [bug] JAVA-733: Handle function calls and raw strings as non-idempotent in QueryBuilder.
- [improvement] JAVA-765: Provide API to retrieve values of a Parameterized SimpleStatement.
- [improvement] JAVA-827: implement UPDATE .. IF EXISTS in QueryBuilder.
- [improvement] JAVA-618: Randomize contact points list to prevent hotspots.
- [improvement] JAVA-720: Surface the coordinator used on query failure.
- [bug] JAVA-792: Handle contact points removed during init.
- [improvement] JAVA-719: Allow PlainTextAuthProvider to change its credentials at runtime.
- [new feature] JAVA-151: Make it possible to register for SchemaChange Events.
- [improvement] JAVA-861: Downgrade "Asked to rebuild table" log from ERROR to INFO level.
- [improvement] JAVA-797: Provide an option to prepare statements only on one node.
- [improvement] JAVA-658: Provide an option to not re-prepare all statements in onUp.
- [improvement] JAVA-853: Customizable creation of netty timer.
- [bug] JAVA-859: Avoid quadratic ring processing with invalid replication factors.
- [improvement] JAVA-657: Debounce control connection queries.
- [bug] JAVA-784: LoadBalancingPolicy.distance() called before init().
- [new feature] JAVA-828: Make driver-side metadata optional.
- [improvement] JAVA-544: Allow hosts to remain partially up.
- [improvement] JAVA-821, JAVA-822: Remove internal blocking calls and expose async session
  creation.
- [improvement] JAVA-725: Use parallel calls when re-preparing statement on other
  hosts.
- [bug] JAVA-629: Don't use connection timeout for unrelated internal queries.
- [bug] JAVA-892: Fix NPE in speculative executions when metrics disabled.

Merged from 2.0.10_fixes branch:

- [improvement] JAVA-756: Use Netty's pooled ByteBufAllocator by default.
- [improvement] JAVA-759: Expose "unsafe" paging state API.
- [bug] JAVA-767: Fix getObject by name.
- [bug] JAVA-768: Prevent race during pool initialization.


### 2.0.10.1

- [improvement] JAVA-756: Use Netty's pooled ByteBufAllocator by default.
- [improvement] JAVA-759: Expose "unsafe" paging state API.
- [bug] JAVA-767: Fix getObject by name.
- [bug] JAVA-768: Prevent race during pool initialization.


### 2.0.10

- [new feature] JAVA-518: Add AddressTranslater for EC2 multi-region deployment.
- [improvement] JAVA-533: Add connection heartbeat.
- [improvement] JAVA-568: Reduce level of logs on missing rpc_address.
- [improvement] JAVA-312, JAVA-681: Expose node token and range information.
- [bug] JAVA-595: Fix cluster name mismatch check at startup.
- [bug] JAVA-620: Fix guava dependency when using OSGI.
- [bug] JAVA-678: Fix handling of DROP events when ks name is case-sensitive.
- [improvement] JAVA-631: Use List<?> instead of List<Object> in QueryBuilder API.
- [improvement] JAVA-654: Exclude Netty POM from META-INF in shaded JAR.
- [bug] JAVA-655: Quote single quotes contained in table comments in asCQLQuery method.
- [bug] JAVA-684: Empty TokenRange returned in a one token cluster.
- [improvement] JAVA-687: Expose TokenRange#contains.
- [new feature] JAVA-547: Expose values of BoundStatement.
- [new feature] JAVA-584: Add getObject to BoundStatement and Row.
- [improvement] JAVA-419: Improve connection pool resizing algorithm.
- [bug] JAVA-599: Fix race condition between pool expansion and shutdown.
- [improvement] JAVA-622: Upgrade Netty to 4.0.27.
- [improvement] JAVA-562: Coalesce frames before flushing them to the connection.
- [improvement] JAVA-583: Rename threads to indicate that they are for the driver.
- [new feature] JAVA-550: Expose paging state.
- [new feature] JAVA-646: Slow Query Logger.
- [improvement] JAVA-698: Exclude some errors from measurements in LatencyAwarePolicy.
- [bug] JAVA-641: Fix issue when executing a PreparedStatement from another cluster.
- [improvement] JAVA-534: Log keyspace xxx does not exist at WARN level.
- [improvement] JAVA-619: Allow Cluster subclasses to delegate to another instance.
- [new feature] JAVA-669: Expose an API to check for schema agreement after a
  schema-altering statement.
- [improvement] JAVA-692: Make connection and pool creation fully async.
- [improvement] JAVA-505: Optimize connection use after reconnection.
- [improvement] JAVA-617: Remove "suspected" mechanism.
- [improvement] reverts JAVA-425: Don't mark connection defunct on client timeout.
- [new feature] JAVA-561: Speculative query executions.
- [bug] JAVA-666: Release connection before completing the ResultSetFuture.
- [new feature BETA] JAVA-723: Percentile-based variant of query logger and speculative
  executions.
- [bug] JAVA-734: Fix buffer leaks when compression is enabled.

Merged from 2.0.9_fixes branch:

- [bug] JAVA-614: Prevent race between cancellation and query completion.
- [bug] JAVA-632: Prevent cancel and timeout from cancelling unrelated ResponseHandler if
  streamId was already released and reused.
- [bug] JAVA-642: Fix issue when newly opened pool fails before we could mark the node UP.
- [bug] JAVA-613: Fix unwanted LBP notifications when a contact host is down.
- [bug] JAVA-651: Fix edge cases where a connection was released twice.
- [bug] JAVA-653: Fix edge cases in query cancellation.


### 2.0.9.2

- [bug] JAVA-651: Fix edge cases where a connection was released twice.
- [bug] JAVA-653: Fix edge cases in query cancellation.


### 2.0.9.1

- [bug] JAVA-614: Prevent race between cancellation and query completion.
- [bug] JAVA-632: Prevent cancel and timeout from cancelling unrelated ResponseHandler if
  streamId was already released and reused.
- [bug] JAVA-642: Fix issue when newly opened pool fails before we could mark the node UP.
- [bug] JAVA-613: Fix unwanted LBP notifications when a contact host is down.


### 2.0.9

- [improvement] JAVA-538: Shade Netty dependency.
- [improvement] JAVA-543: Target schema refreshes more precisely.
- [bug] JAVA-546: Don't check rpc_address for control host.
- [improvement] JAVA-409: Improve message of NoHostAvailableException.
- [bug] JAVA-556: Rework connection reaper to avoid deadlock.
- [bug] JAVA-557: Avoid deadlock when multiple connections to the same host get write
  errors.
- [improvement] JAVA-504: Make shuffle=true the default for TokenAwarePolicy.
- [bug] JAVA-577: Fix bug when SUSPECT reconnection succeeds, but one of the pooled
  connections fails while bringing the node back up.
- [bug] JAVA-419: JAVA-587: Prevent faulty control connection from ignoring reconnecting hosts.
- temporarily revert "Add idle timeout to the connection pool".
- [bug] JAVA-593: Ensure updateCreatedPools does not add pools for suspected hosts.
- [bug] JAVA-594: Ensure state change notifications for a given host are handled serially.
- [bug] JAVA-597: Ensure control connection reconnects when control host is removed.


### 2.0.8

- [bug] JAVA-526: Fix token awareness for case-sensitive keyspaces and tables.
- [bug] JAVA-515: Check maximum number of values passed to SimpleStatement.
- [improvement] JAVA-532: Expose the driver version through the API.
- [improvement] JAVA-522: Optimize session initialization when some hosts are not
  responsive.


### 2.0.7

- [bug] JAVA-449: Handle null pool in PooledConnection.release.
- [improvement] JAVA-425: Defunct connection on request timeout.
- [improvement] JAVA-426: Try next host when we get a SERVER_ERROR.
- [bug] JAVA-449, JAVA-460, JAVA-471: Handle race between query timeout and completion.
- [bug] JAVA-496: Fix DCAwareRoundRobinPolicy datacenter auto-discovery.
- [bug] JAVA-497: Ensure control connection does not trigger concurrent reconnects.
- [improvement] JAVA-472: Keep trying to reconnect on authentication errors.
- [improvement] JAVA-463: Expose close method on load balancing policy.
- [improvement] JAVA-459: Allow load balancing policy to trigger refresh for a single host.
- [bug] JAVA-493: Expose an API to cancel reconnection attempts.
- [bug] JAVA-503: Fix NPE when a connection fails during pool construction.
- [improvement] JAVA-423: Log datacenter name in DCAware policy's init when it is explicitly provided.
- [improvement] JAVA-504: Shuffle the replicas in TokenAwarePolicy.newQueryPlan.
- [improvement] JAVA-507: Make schema agreement wait tuneable.
- [improvement] JAVA-494: Document how to inject the driver metrics into another registry.
- [improvement] JAVA-419: Add idle timeout to the connection pool.
- [bug] JAVA-516: LatencyAwarePolicy does not shutdown executor on invocation of close.
- [improvement] JAVA-451: Throw an exception when DCAwareRoundRobinPolicy is built with
  an explicit but null or empty local datacenter.
- [bug] JAVA-511: Fix check for local contact points in DCAware policy's init.
- [improvement] JAVA-457: Make timeout on saturated pool customizable.
- [improvement] JAVA-521: Downgrade Guava to 14.0.1.


### 2.0.6

- [bug] JAVA-397: Check cluster name when connecting to a new node.
- [bug] JAVA-326: Add missing CAS delete support in QueryBuilder.
- [bug] JAVA-363: Add collection and data length checks during serialization.
- [improvement] JAVA-329: Surface number of retries in metrics.
- [bug] JAVA-428: Do not use a host when no rpc_address found for it.
- [improvement] JAVA-358: Add ResultSet.wasApplied() for conditional queries.
- [bug] JAVA-349: Fix negative HostConnectionPool open count.
- [improvement] JAVA-436: Log more connection details at trace and debug levels.
- [bug] JAVA-445: Fix cluster shutdown.
- [improvement] JAVA-439: Expose child policy in chainable load balancing policies.


### 2.0.5

- [bug] JAVA-407: Release connections on ResultSetFuture#cancel.
- [bug] JAVA-393: Fix handling of SimpleStatement with values in query builder
  batches.
- [bug] JAVA-417: Ensure pool is properly closed in onDown.
- [bug] JAVA-415: Fix tokenMap initialization at startup.
- [bug] JAVA-418: Avoid deadlock on close.


### 2.0.4

- [improvement] JAVA-204: Better handling of dead connections.
- [bug] JAVA-373: Fix potential NPE in ControlConnection.
- [bug] JAVA-291: Throws NPE when passed null for a contact point.
- [bug] JAVA-315: Avoid LoadBalancingPolicy onDown+onUp at startup.
- [bug] JAVA-343: Avoid classloader leak in Tomcat.
- [bug] JAVA-387: Avoid deadlock in onAdd/onUp.
- [bug] JAVA-377, JAVA-391: Make metadata parsing more lenient.
- [bug] JAVA-394: Ensure defunct connections are completely closed.
- [bug] JAVA-342, JAVA-390: Fix memory and resource leak on closed Sessions.


### 2.0.3

- [new] The new AbsractSession makes mocking of Session easier.
- [new] JAVA-309: Allow to trigger a refresh of connected hosts.
- [new] JAVA-265: New Session#getState method allows to grab information on
  which nodes a session is connected to.
- [new] JAVA-327: Add QueryBuilder syntax for tuples in where clauses (syntax
  introduced in Cassandra 2.0.6).
- [improvement] JAVA-359: Properly validate arguments of PoolingOptions methods.
- [bug] JAVA-368: Fix bogus rejection of BigInteger in 'execute with values'.
- [bug] JAVA-367: Signal connection failure sooner to avoid missing them.
- [bug] JAVA-337: Throw UnsupportedOperationException for protocol batch
  setSerialCL.

Merged from 1.0 branch:

- [bug] JAVA-325: Fix periodic reconnection to down hosts.


### 2.0.2

- [api] The type of the map key returned by NoHostAvailable#getErrors has changed from
  InetAddress to InetSocketAddress. Same for Initializer#getContactPoints return and
  for AuthProvider#newAuthenticator.
- [api] JAVA-296: The default load balacing policy is now DCAwareRoundRobinPolicy, and the local
  datacenter is automatically picked based on the first connected node. Furthermore,
  the TokenAwarePolicy is also used by default.
- [new] JAVA-145: New optional AddressTranslater.
- [bug] JAVA-321: Don't remove quotes on keyspace in the query builder.
- [bug] JAVA-320: Fix potential NPE while cluster undergo schema changes.
- [bug] JAVA-319: Fix thread-safety of page fetching.
- [bug] JAVA-318: Fix potential NPE using fetchMoreResults.

Merged from 1.0 branch:

- [new] JAVA-179: Expose the name of the partitioner in use in the cluster metadata.
- [new] Add new WhiteListPolicy to limit the nodes connected to a particular list.
- [improvement] JAVA-289: Do not hop DC for LOCAL_* CL in DCAwareRoundRobinPolicy.
- [bug] JAVA-313: Revert back to longs for dates in the query builder.
- [bug] JAVA-314: Don't reconnect to nodes ignored by the load balancing policy.


### 2.0.1

- [improvement] JAVA-278: Handle the static columns introduced in Cassandra 2.0.6.
- [improvement] JAVA-208: Add Cluster#newSession method to create Session without connecting
  right away.
- [bug] JAVA-279: Add missing iso8601 patterns for parsing dates.
- [bug] Properly parse BytesType as the blob type.
- [bug] JAVA-280: Potential NPE when parsing schema of pre-CQL tables of C* 1.2 nodes.

Merged from 1.0 branch:

- [bug] JAVA-275: LatencyAwarePolicy.Builder#withScale doesn't set the scale.
- [new] JAVA-114: Add methods to check if a Cluster/Session instance has been closed already.


### 2.0.0

- [api] JAVA-269: Case sensitive identifier by default in Metadata.
- [bug] JAVA-274: Fix potential NPE in Cluster#connect.

Merged from 1.0 branch:

- [bug] JAVA-263: Always return the PreparedStatement object that is cache internally.
- [bug] JAVA-261: Fix race when multiple connect are done in parallel.
- [bug] JAVA-270: Don't connect at all to nodes that are ignored by the load balancing
  policy.


### 2.0.0-rc3

- [improvement] The protocol version 1 is now supported (features only supported by the
  version 2 of the protocol throw UnsupportedFeatureException).
- [improvement] JAVA-195: Make most main objects interface to facilitate testing/mocking.
- [improvement] Adds new getStatements and clear methods to BatchStatement.
- [api] JAVA-247: Renamed shutdown to closeAsync and ShutdownFuture to CloseFuture. Clustering
  and Session also now implement Closeable.
- [bug] JAVA-232: Fix potential thread leaks when shutting down Metrics.
- [bug] JAVA-231: Fix potential NPE in HostConnectionPool.
- [bug] JAVA-244: Avoid NPE when node is in an unconfigured DC.
- [bug] JAVA-258: Don't block for scheduled reconnections on Cluster#close.

Merged from 1.0 branch:

- [new] JAVA-224: Added Session#prepareAsync calls.
- [new] JAVA-249: Added Cluster#getLoggedKeyspace.
- [improvement] Avoid preparing a statement multiple time per host with multiple sessions.
- [bug] JAVA-255: Make sure connections are returned to the right pools.
- [bug] JAVA-264: Use date string in query build to work-around CASSANDRA-6718.


### 2.0.0-rc2

- [new] JAVA-207: Add LOCAL_ONE consistency level support (requires using C* 2.0.2+).
- [bug] JAVA-219: Fix parsing of counter types.
- [bug] JAVA-218: Fix missing whitespace for IN clause in the query builder.
- [bug] JAVA-221: Fix replicas computation for token aware balancing.

Merged from 1.0 branch:

- [bug] JAVA-213: Fix regression from JAVA-201.
- [improvement] New getter to obtain a snapshot of the scores maintained by
  LatencyAwarePolicy.


### 2.0.0-rc1

- [new] JAVA-199: Mark compression dependencies optional in maven.
- [api] Renamed TableMetadata#getClusteringKey to TableMetadata#getClusteringColumns.

Merged from 1.0 branch:

- [new] JAVA-142: OSGi bundle.
- [improvement] JAVA-205: Make collections returned by Row immutable.
- [improvement] JAVA-203: Limit internal thread pool size.
- [bug] JAVA-201: Don't retain unused PreparedStatement in memory.
- [bug] Add missing clustering order info in TableMetadata
- [bug] JAVA-196: Allow bind markers for collections in the query builder.


### 2.0.0-beta2

- [api] BoundStatement#setX(String, X) methods now set all values (if there is
  more than one) having the provided name, not just the first occurence.
- [api] The Authenticator interface now has a onAuthenticationSuccess method that
  allows to handle the potential last token sent by the server.
- [new] The query builder don't serialize large values to strings anymore by
  default by making use the new ability to send values alongside the query string.
- [new] JAVA-140: The query builder has been updated for new CQL features.
- [bug] Fix exception when a conditional write timeout C* side.
- [bug] JAVA-182: Ensure connection is created when Cluster metadata are asked for.
- [bug] JAVA-187: Fix potential NPE during authentication.


### 2.0.0-beta1

- [api] The 2.0 version is an API-breaking upgrade of the driver. While most
  of the breaking changes are minor, there are too numerous to be listed here
  and you are encouraged to look at the Upgrade_guide_to_2.0 file that describe
  those changes in details.
- [new] LZ4 compression is supported for the protocol.
- [new] JAVA-39: The driver does not depend on cassandra-all anymore.
- [new] New BatchStatement class allows to execute batch other statements.
- [new] Large ResultSet are now paged (incrementally fetched) by default.
- [new] SimpleStatement support values for bind-variables, to allow
  prepare+execute behavior with one roundtrip.
- [new] Query parameters defaults (Consistency level, page size, ...) can be
  configured globally.
- [new] New Cassandra 2.0 SERIAL and LOCAL_SERIAL consistency levels are
  supported.
- [new] JAVA-116: Cluster#shutdown now waits for ongoing queries to complete by default.
- [new] Generic authentication through SASL is now exposed.
- [bug] JAVA-88: TokenAwarePolicy now takes all replica into account, instead of only the
  first one.


### 1.0.5

- [new] JAVA-142: OSGi bundle.
- [new] JAVA-207: Add support for ConsistencyLevel.LOCAL_ONE; note that this
  require Cassandra 1.2.12+.
- [improvement] JAVA-205: Make collections returned by Row immutable.
- [improvement] JAVA-203: Limit internal thread pool size.
- [improvement] New getter to obtain a snapshot of the scores maintained by
  LatencyAwarePolicy.
- [improvement] JAVA-222: Avoid synchronization when getting codec for collection
  types.
- [bug] JAVA-201, JAVA-213: Don't retain unused PreparedStatement in memory.
- [bug] Add missing clustering order info in TableMetadata
- [bug] JAVA-196: Allow bind markers for collections in the query builder.


### 1.0.4

- [api] JAVA-163: The Cluster.Builder#poolingOptions and Cluster.Builder#socketOptions
  are now deprecated. They are replaced by the new withPoolingOptions and
  withSocketOptions methods.
- [new] JAVA-129: A new LatencyAwarePolicy wrapping policy has been added, allowing to
  add latency awareness to a wrapped load balancing policy.
- [new] JAVA-161: Cluster.Builder#deferInitialization: Allow defering cluster initialization.
- [new] JAVA-117: Add truncate statement in query builder.
- [new] JAVA-106: Support empty IN in the query builder.
- [bug] JAVA-166: Fix spurious "No current pool set; this should not happen" error
  message.
- [bug] JAVA-184: Fix potential overflow in RoundRobinPolicy and correctly errors if
  a balancing policy throws.
- [bug] Don't release Stream ID for timeouted queries (unless we do get back
  the response)
- [bug] Correctly escape identifiers and use fully qualified table names when
  exporting schema as string.


### 1.0.3

- [api] The query builder now correctly throw an exception when given a value
  of a type it doesn't know about.
- [new] SocketOptions#setReadTimeout allows to set a timeout on how long we
  wait for the answer of one node. See the javadoc for more details.
- [new] New Session#prepare method that takes a Statement.
- [bug] JAVA-143: Always take per-query CL, tracing, etc. into account for QueryBuilder
  statements.
- [bug] Temporary fixup for TimestampType when talking to C* 2.0 nodes.


### 1.0.2

- [api] Host#getMonitor and all Host.HealthMonitor methods have been
  deprecated. The new Host#isUp method is now prefered to the method
  in the monitor and you should now register Host.StateListener against
  the Cluster object directly (registering against a host HealthMonitor
  was much more limited anyway).
- [new] JAVA-92: New serialize/deserialize methods in DataType to serialize/deserialize
  values to/from bytes.
- [new] JAVA-128: New getIndexOf() method in ColumnDefinitions to find the index of
  a given column name.
- [bug] JAVA-131: Fix a bug when thread could get blocked while setting the current
  keyspace.
- [bug] JAVA-136: Quote inet addresses in the query builder since CQL3 requires it.


### 1.0.1

- [api] JAVA-100: Function call handling in the query builder has been modified in a
  backward incompatible way. Function calls are not parsed from string values
  anymore as this wasn't safe. Instead the new 'fcall' method should be used.
- [api] Some typos in method names in PoolingOptions have been fixed in a
  backward incompatible way before the API get widespread.
- [bug] JAVA-123: Don't destroy composite partition key with BoundStatement and
  TokenAwarePolicy.
- [new] null values support in the query builder.
- [new] JAVA-5: SSL support (requires C* >= 1.2.1).
- [new] JAVA-113: Allow generating unlogged batch in the query builder.
- [improvement] Better error message when no host are available.
- [improvement] Improves performance of the stress example application been.


### 1.0.0

- [api] The AuthInfoProvider has be (temporarily) removed. Instead, the
  Cluster builder has a new withCredentials() method to provide a username
  and password for use with Cassandra's PasswordAuthenticator. Custom
  authenticator will be re-introduced in a future version but are not
  supported at the moment.
- [api] The isMetricsEnabled() method in Configuration has been replaced by
  getMetricsOptions(). An option to disabled JMX reporting (on by default)
  has been added.
- [bug] JAVA-91: Don't make default load balancing policy a static singleton since it
  is stateful.


### 1.0.0-RC1

- [new] JAVA-79: Null values are now supported in BoundStatement (but you will need at
  least Cassandra 1.2.3 for it to work). The API of BoundStatement has been
  slightly changed so that not binding a variable is not an error anymore,
  the variable is simply considered null by default. The isReady() method has
  been removed.
- [improvement] JAVA-75: The Cluster/Session shutdown methods now properly block until
  the shutdown is complete. A version with at timeout has been added.
- [bug] JAVA-44: Fix use of CQL3 functions in the query builder.
- [bug] JAVA-77: Fix case where multiple schema changes too quickly wouldn't work
  (only triggered when 0.0.0.0 was used for the rpc_address on the Cassandra
  nodes).
- [bug] JAVA-72: Fix IllegalStateException thrown due to a reconnection made on an I/O
  thread.
- [bug] JAVA-82: Correctly reports errors during authentication phase.


### 1.0.0-beta2

- [new] JAVA-51, JAVA-60, JAVA-58: Support blob constants, BigInteger, BigDecimal and counter batches in
  the query builder.
- [new] JAVA-61: Basic support for custom CQL3 types.
- [new] JAVA-65: Add "execution infos" for a result set (this also move the query
  trace in the new ExecutionInfos object, so users of beta1 will have to
  update).
- [bug] JAVA-62: Fix failover bug in DCAwareRoundRobinPolicy.
- [bug] JAVA-66: Fix use of bind markers for routing keys in the query builder.


### 1.0.0-beta1

- initial release
