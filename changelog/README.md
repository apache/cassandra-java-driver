## Changelog

<!-- Note: contrary to 3.x, insert new entries *first* in their section -->

### 4.4.0 (in progress)

- [improvement] JAVA-2528: Store suppressed exceptions in AllNodesFailedException
- [new feature] JAVA-2581: Add query builder support for indexed list assignments
- [improvement] JAVA-2596: Consider collection removals as idempotent in query builder
- [bug] JAVA-2555: Generate append/prepend constructs compatible with legacy C* versions
- [bug] JAVA-2584: Ensure codec registry is able to create codecs for collections of UDTs and tuples
- [bug] JAVA-2583: IS NOT NULL clause should be idempotent
- [improvement] JAVA-2442: Don't check for schema agreement twice when completing a DDL query
- [improvement] JAVA-2473: Don't reconnect control connection if protocol is downgraded
- [bug] JAVA-2556: Make ExecutionInfo compatible with any Request type
- [new feature] JAVA-2532: Add BoundStatement ReturnType for insert, update, and delete DAO methods
- [improvement] JAVA-2107: Add XML formatting plugin
- [bug] JAVA-2527: Allow AllNodesFailedException to accept more than one error per node
- [improvement] JAVA-2546: Abort schema refresh if a query fails

### 4.3.1

- [bug] JAVA-2557: Accept any negative length when decoding elements of tuples and UDTs

### 4.3.0

- [improvement] JAVA-2497: Ensure nodes and exceptions are serializable
- [bug] JAVA-2464: Fix initial schema refresh when reconnect-on-init is enabled
- [improvement] JAVA-2516: Enable hostname validation with Cloud
- [documentation]: JAVA-2460: Document how to determine the local DC
- [improvement] JAVA-2476: Improve error message when codec registry inspects a collection with a
  null element
- [documentation] JAVA-2509: Mention file-based approach for Cloud configuration in the manual
- [improvement] JAVA-2447: Mention programmatic local DC method in Default LBP error message
- [improvement] JAVA-2459: Improve extensibility of existing load balancing policies
- [documentation] JAVA-2428: Add developer docs
- [documentation] JAVA-2503: Migrate Cloud "getting started" page to driver manual
- [improvement] JAVA-2484: Add errors for cloud misconfiguration
- [improvement] JAVA-2490: Allow to read the secure bundle from an InputStream
- [new feature] JAVA-2478: Allow to provide the secure bundle via URL
- [new feature] JAVA-2356: Support for DataStax Cloud API
- [improvement] JAVA-2407: Improve handling of logback configuration files in IDEs
- [improvement] JAVA-2434: Add support for custom cipher suites and host name validation to ProgrammaticSslEngineFactory
- [improvement] JAVA-2480: Upgrade Jackson to 2.10.0
- [documentation] JAVA-2505: Annotate Node.getHostId() as nullable
- [improvement] JAVA-1708: Support DSE "everywhere" replication strategy
- [improvement] JAVA-2471: Consider DSE version when parsing the schema
- [improvement] JAVA-2444: Add method setRoutingKey(ByteBuffer...) to StatementBuilder
- [improvement] JAVA-2398: Improve support for optional dependencies in OSGi
- [improvement] JAVA-2452: Allow "none" as a compression option
- [improvement] JAVA-2419: Allow registration of user codecs at runtime
- [documentation] JAVA-2384: Add quick overview section to each manual page
- [documentation] JAVA-2412: Cover DDL query debouncing in FAQ and upgrade guide
- [documentation] JAVA-2416: Update paging section in the manual
- [improvement] JAVA-2402: Add setTracing(boolean) to StatementBuilder
- [bug] JAVA-2466: Set idempotence to null in BatchStatement.newInstance

### 4.2.2

- [bug] JAVA-2475: Fix message size when query string contains Unicode surrogates
- [bug] JAVA-2470: Fix Session.OSS_DRIVER_COORDINATES for shaded JAR

### 4.2.1

- [bug] JAVA-2454: Handle "empty" CQL type while parsing schema
- [improvement] JAVA-2455: Improve logging of schema refresh errors
- [documentation] JAVA-2429: Document expected types on DefaultDriverOption
- [documentation] JAVA-2426: Fix month pattern in CqlDuration documentation
- [bug] JAVA-2451: Make zero a valid estimated size for PagingIterableSpliterator
- [bug] JAVA-2443: Compute prepared statement PK indices for protocol v3
- [bug] JAVA-2430: Use variable metadata to infer the routing keyspace on bound statements

### 4.2.0

- [improvement] JAVA-2390: Add methods to set the SSL engine factory programmatically
- [improvement] JAVA-2379: Fail fast if prepared id doesn't match when repreparing on the fly
- [bug] JAVA-2375: Use per-request keyspace when repreparing on the fly
- [improvement] JAVA-2370: Remove auto-service plugin from mapper processor
- [improvement] JAVA-2377: Add a config option to make driver threads daemon
- [improvement] JAVA-2371: Handle null elements in collections on the decode path
- [improvement] JAVA-2351: Add a driver example for the object mapper
- [bug] JAVA-2323: Handle restart of a node with same host_id but a different address
- [improvement] JAVA-2303: Ignore peer rows matching the control host's RPC address
- [improvement] JAVA-2236: Add methods to set the auth provider programmatically
- [improvement] JAVA-2369: Change mapper annotations retention to runtime
- [improvement] JAVA-2365: Redeclare default constants when an enum is abstracted behind an
  interface
- [improvement] JAVA-2302: Better target mapper errors and warnings for inherited methods
- [improvement] JAVA-2336: Expose byte utility methods in the public API 
- [improvement] JAVA-2338: Revisit toString() for data container types
- [bug] JAVA-2367: Fix column names in EntityHelper.updateByPrimaryKey
- [bug] JAVA-2358: Fix list of reserved CQL keywords
- [improvement] JAVA-2359: Allow default keyspace at the mapper level
- [improvement] JAVA-2306: Clear security tokens from memory immediately after use
- [improvement] JAVA-2320: Expose more attributes on mapper Select for individual query clauses
- [bug] JAVA-2332: Destroy connection pool when a node gets removed
- [bug] JAVA-2324: Add support for primitive shorts in mapper
- [bug] JAVA-2325: Allow "is" prefix for boolean getters in mapped entities
- [improvement] JAVA-2308: Add customWhereClause to `@Delete`
- [improvement] JAVA-2247: PagingIterable implementations should implement spliterator()
- [bug] JAVA-2312: Handle UDTs with names that clash with collection types
- [improvement] JAVA-2307: Improve `@Select` and `@Delete` by not requiring full primary key
- [improvement] JAVA-2315: Improve extensibility of session builder
- [bug] JAVA-2394: BaseCcmRule DseRequirement max should use DseVersion, not Cassandra version

### 4.1.0

- [documentation] JAVA-2294: Fix wrong examples in manual page on batch statements
- [bug] JAVA-2304: Avoid direct calls to ByteBuffer.array()
- [new feature] JAVA-2078: Add object mapper
- [improvement] JAVA-2297: Add a NettyOptions method to set socket options
- [bug] JAVA-2280: Ignore peer rows with missing host id or RPC address
- [bug] JAVA-2264: Adjust HashedWheelTimer tick duration from 1 to 100 ms
- [bug] JAVA-2260: Handle empty collections in PreparedStatement.bind(...)
- [improvement] JAVA-2278: Pass the request's log prefix to RequestTracker
- [bug] JAVA-2253: Don't strip trailing zeros in ByteOrderedToken
- [improvement] JAVA-2207: Add bulk value assignment to QueryBuilder Insert
- [bug] JAVA-2234: Handle terminated executor when the session is closed twice
- [documentation] JAVA-2220: Emphasize that query builder is now a separate artifact in root README
- [documentation] JAVA-2217: Cover contact points and local datacenter earlier in the manual
- [improvement] JAVA-2242: Allow skipping all integration tests with -DskipITs
- [improvement] JAVA-2241: Make DefaultDriverContext.cycleDetector protected
- [bug] JAVA-2226: Support IPv6 contact points in the configuration

### 4.0.1

- [new feature] JAVA-2201: Expose a public API for programmatic config
- [new feature] JAVA-2205: Expose public factory methods for alternative config loaders
- [bug] JAVA-2214: Fix flaky RequestLoggerIT test
- [bug] JAVA-2203: Handle unresolved addresses in DefaultEndPoint
- [bug] JAVA-2210: Add ability to set TTL for modification queries
- [improvement] JAVA-2212: Add truncate to QueryBuilder 
- [improvement] JAVA-2211: Upgrade Jersey examples to fix security issue sid-3606
- [bug] JAVA-2193: Fix flaky tests in ExecutionInfoWarningsIT
- [improvement] JAVA-2197: Skip deployment of examples and integration tests to Maven central

### 4.0.0

- [improvement] JAVA-2192: Don't return generic types with wildcards
- [improvement] JAVA-2148: Add examples
- [bug] JAVA-2189: Exclude virtual keyspaces from token map computation
- [improvement] JAVA-2183: Enable materialized views when testing against Cassandra 4
- [improvement] JAVA-2182: Add insertInto().json() variant that takes an object in QueryBuilder
- [improvement] JAVA-2161: Annotate mutating methods with `@CheckReturnValue`
- [bug] JAVA-2177: Don't exclude down nodes when initializing LBPs
- [improvement] JAVA-2143: Rename Statement.setTimestamp() to setQueryTimestamp()
- [improvement] JAVA-2165: Abstract node connection information
- [improvement] JAVA-2090: Add support for additional_write_policy and read_repair table options
- [improvement] JAVA-2164: Rename statement builder methods to setXxx
- [bug] JAVA-2178: QueryBuilder: Alias after function column is not included in a query
- [improvement] JAVA-2158: Allow BuildableQuery to build statement with values
- [improvement] JAVA-2150: Improve query builder error message on unsupported literal type
- [documentation] JAVA-2149: Improve Term javadocs in the query builder

### 4.0.0-rc1

- [improvement] JAVA-2106: Log server side warnings returned from a query
- [improvement] JAVA-2151: Drop "Dsl" suffix from query builder main classes
- [new feature] JAVA-2144: Expose internal API to hook into the session lifecycle
- [improvement] JAVA-2119: Add PagingIterable abstraction as a supertype of ResultSet
- [bug] JAVA-2063: Normalize authentication logging
- [documentation] JAVA-2034: Add performance recommendations in the manual
- [improvement] JAVA-2077: Allow reconnection policy to detect first connection attempt
- [improvement] JAVA-2067: Publish javadocs JAR for the shaded module
- [improvement] JAVA-2103: Expose partitioner name in TokenMap API
- [documentation] JAVA-2075: Document preference for LZ4 over Snappy

### 4.0.0-beta3

- [bug] JAVA-2066: Array index range error when fetching routing keys on bound statements
- [documentation] JAVA-2061: Add section to upgrade guide about updated type mappings
- [improvement] JAVA-2038: Add jitter to delays between reconnection attempts
- [improvement] JAVA-2053: Cache results of session.prepare()
- [improvement] JAVA-2058: Make programmatic config reloading part of the public API
- [improvement] JAVA-1943: Fail fast in execute() when the session is closed
- [improvement] JAVA-2056: Reduce HashedWheelTimer tick duration
- [bug] JAVA-2057: Do not create pool when SUGGEST\_UP topology event received
- [improvement] JAVA-2049: Add shorthand method to SessionBuilder to specify local DC
- [bug] JAVA-2037: Fix NPE when preparing statement with no bound variables
- [improvement] JAVA-2014: Schedule timeouts on a separate Timer
- [bug] JAVA-2029: Handle schema refresh failure after a DDL query
- [bug] JAVA-1947: Make schema parsing more lenient and allow missing system_virtual_schema
- [bug] JAVA-2028: Use CQL form when parsing UDT types in system tables
- [improvement] JAVA-1918: Document temporal types
- [improvement] JAVA-1914: Optimize use of System.nanoTime in CqlRequestHandlerBase
- [improvement] JAVA-1945: Document corner cases around UDT and tuple attachment
- [improvement] JAVA-2026: Make CqlDuration implement TemporalAmount
- [improvement] JAVA-2017: Slightly optimize conversion methods on the hot path
- [improvement] JAVA-2010: Make dependencies to annotations required again
- [improvement] JAVA-1978: Add a config option to keep contact points unresolved
- [bug] JAVA-2000: Fix ConcurrentModificationException during channel shutdown
- [improvement] JAVA-2002: Reimplement TypeCodec.accepts to improve performance
- [improvement] JAVA-2011: Re-add ResultSet.getAvailableWithoutFetching() and isFullyFetched()
- [improvement] JAVA-2007: Make driver threads extend FastThreadLocalThread
- [bug] JAVA-2001: Handle zero timeout in admin requests

### 4.0.0-beta2

- [new feature] JAVA-1919: Provide a timestamp <=> ZonedDateTime codec
- [improvement] JAVA-1989: Add BatchStatement.newInstance(BatchType, Iterable<BatchableStatement>)
- [improvement] JAVA-1988: Remove pre-fetching from ResultSet API
- [bug] JAVA-1948: Close session properly when LBP fails to initialize
- [improvement] JAVA-1949: Improve error message when contact points are wrong
- [improvement] JAVA-1956: Add statementsCount accessor to BatchStatementBuilder
- [bug] JAVA-1946: Ignore protocol version in equals comparison for UdtValue/TupleValue
- [new feature] JAVA-1932: Send Driver Name and Version in Startup message
- [new feature] JAVA-1917: Add ability to set node on statement
- [improvement] JAVA-1916: Base TimestampCodec.parse on java.util.Date.
- [improvement] JAVA-1940: Clean up test resources when CCM integration tests finish
- [bug] JAVA-1938: Make CassandraSchemaQueries classes public
- [improvement] JAVA-1925: Rename context getters
- [improvement] JAVA-1544: Check API compatibility with Revapi
- [new feature] JAVA-1900: Add support for virtual tables

### 4.0.0-beta1

- [new feature] JAVA-1869: Add DefaultDriverConfigLoaderBuilder
- [improvement] JAVA-1913: Expose additional counters on Node
- [improvement] JAVA-1880: Rename "config profile" to "execution profile"
- [improvement] JAVA-1889: Upgrade dependencies to the latest minor versions
- [improvement] JAVA-1819: Propagate more attributes to bound statements
- [improvement] JAVA-1897: Improve extensibility of schema metadata classes
- [improvement] JAVA-1437: Enable SSL hostname validation by default
- [improvement] JAVA-1879: Duplicate basic.request options as Request/Statement attributes
- [improvement] JAVA-1870: Use sensible defaults in RequestLogger if config options are missing
- [improvement] JAVA-1877: Use a separate reconnection schedule for the control connection
- [improvement] JAVA-1763: Generate a binary tarball as part of the build process
- [improvement] JAVA-1884: Add additional methods from TypeToken to GenericType
- [improvement] JAVA-1883: Use custom queue implementation for LBP's query plan
- [improvement] JAVA-1890: Add more configuration options to DefaultSslEngineFactory
- [bug] JAVA-1895: Rename PreparedStatement.getPrimaryKeyIndices to getPartitionKeyIndices
- [bug] JAVA-1891: Allow null items when setting values in bulk
- [improvement] JAVA-1767: Improve message when column not in result set
- [improvement] JAVA-1624: Expose ExecutionInfo on exceptions where applicable
- [improvement] JAVA-1766: Revisit nullability
- [new feature] JAVA-1860: Allow reconnection at startup if no contact point is available
- [improvement] JAVA-1866: Make all public policies implement AutoCloseable
- [new feature] JAVA-1762: Build alternate core artifact with Netty shaded
- [new feature] JAVA-1761: Add OSGi descriptors
- [bug] JAVA-1560: Correctly propagate policy initialization errors
- [improvement] JAVA-1865: Add RelationMetadata.getPrimaryKey()
- [improvement] JAVA-1862: Add ConsistencyLevel.isDcLocal and isSerial
- [improvement] JAVA-1858: Implement Serializable in implementations, not interfaces
- [improvement] JAVA-1830: Surface response frame size in ExecutionInfo
- [improvement] JAVA-1853: Add newValue(Object...) to TupleType and UserDefinedType
- [improvement] JAVA-1815: Reorganize configuration into basic/advanced categories
- [improvement] JAVA-1848: Add logs to DefaultRetryPolicy
- [new feature] JAVA-1832: Add Ec2MultiRegionAddressTranslator
- [improvement] JAVA-1825: Add remaining Typesafe config primitive types to DriverConfigProfile
- [new feature] JAVA-1846: Add ConstantReconnectionPolicy
- [improvement] JAVA-1824: Make policies overridable in profiles
- [bug] JAVA-1569: Allow null to be used in positional and named values in statements
- [new feature] JAVA-1592: Expose request's total Frame size through API
- [new feature] JAVA-1829: Add metrics for bytes-sent and bytes-received 
- [improvement] JAVA-1755: Normalize usage of DEBUG/TRACE log levels
- [improvement] JAVA-1803: Log driver version on first use
- [improvement] JAVA-1792: Add AuthProvider callback to handle missing challenge from server
- [improvement] JAVA-1775: Assume default packages for built-in policies
- [improvement] JAVA-1774: Standardize policy locations
- [improvement] JAVA-1798: Allow passing the default LBP filter as a session builder argument
- [new feature] JAVA-1523: Add query logger
- [improvement] JAVA-1801: Revisit NodeStateListener and SchemaChangeListener APIs
- [improvement] JAVA-1759: Revisit metrics API
- [improvement] JAVA-1776: Use concurrency annotations
- [improvement] JAVA-1799: Use CqlIdentifier for simple statement named values
- [new feature] JAVA-1515: Add query builder
- [improvement] JAVA-1773: Make DriverConfigProfile enumerable
- [improvement] JAVA-1787: Use standalone shaded Guava artifact
- [improvement] JAVA-1769: Allocate exact buffer size for outgoing requests
- [documentation] JAVA-1780: Add manual section about case sensitivity
- [new feature] JAVA-1536: Add request throttling
- [improvement] JAVA-1772: Revisit multi-response callbacks
- [new feature] JAVA-1537: Add remaining socket options
- [bug] JAVA-1756: Propagate custom payload when preparing a statement
- [improvement] JAVA-1847: Add per-node request tracking

### 4.0.0-alpha3

- [new feature] JAVA-1518: Expose metrics
- [improvement] JAVA-1739: Add host_id and schema_version to node metadata
- [improvement] JAVA-1738: Convert enums to allow extensibility
- [bug] JAVA-1727: Override DefaultUdtValue.equals
- [bug] JAVA-1729: Override DefaultTupleValue.equals
- [improvement] JAVA-1720: Merge Cluster and Session into a single interface
- [improvement] JAVA-1713: Use less nodes in DefaultLoadBalancingPolicyIT
- [improvement] JAVA-1707: Add test infrastructure for running DSE clusters with CCM
- [bug] JAVA-1715: Propagate unchecked exceptions to CompletableFuture in SyncAuthenticator methods
- [improvement] JAVA-1714: Make replication strategies pluggable
- [new feature] JAVA-1647: Handle metadata_changed flag in protocol v5
- [new feature] JAVA-1633: Handle per-request keyspace in protocol v5
- [improvement] JAVA-1678: Warn if auth is configured on the client but not the server
- [improvement] JAVA-1673: Remove schema agreement check when repreparing on up
- [new feature] JAVA-1526: Provide a single load balancing policy implementation
- [improvement] JAVA-1680: Improve error message on batch log write timeout
- [improvement] JAVA-1675: Remove dates from copyright headers
- [improvement] JAVA-1645: Don't log stack traces at WARN level
- [new feature] JAVA-1524: Add query trace API
- [improvement] JAVA-1646: Provide a more readable error when connecting to Cassandra 2.0 or lower
- [improvement] JAVA-1662: Raise default request timeout
- [improvement] JAVA-1566: Enforce API rules automatically
- [bug] JAVA-1584: Validate that no bound values are unset in protocol v3

### 4.0.0-alpha2

- [new feature] JAVA-1525: Handle token metadata
- [new feature] JAVA-1638: Check schema agreement
- [new feature] JAVA-1494: Implement Snappy and LZ4 compression
- [new feature] JAVA-1514: Port Uuids utility class
- [new feature] JAVA-1520: Add node state listeners
- [new feature] JAVA-1493: Handle schema metadata
- [improvement] JAVA-1605: Refactor request execution model
- [improvement] JAVA-1597: Fix raw usages of Statement
- [improvement] JAVA-1542: Enable JaCoCo code coverage
- [improvement] JAVA-1295: Auto-detect best protocol version in mixed cluster
- [bug] JAVA-1565: Mark node down when it loses its last connection and was already reconnecting
- [bug] JAVA-1594: Don't create pool if node comes back up but is ignored
- [bug] JAVA-1593: Reconnect control connection if current node is removed, forced down or ignored
- [bug] JAVA-1595: Don't use system.local.rpc_address when refreshing node list
- [bug] JAVA-1568: Handle Reconnection#reconnectNow/stop while the current attempt is still in 
  progress
- [improvement] JAVA-1585: Add GenericType#where
- [improvement] JAVA-1590: Properly skip deployment of integration-tests module
- [improvement] JAVA-1576: Expose AsyncResultSet's iterator through a currentPage() method
- [improvement] JAVA-1591: Add programmatic way to get driver version

### 4.0.0-alpha1

- [improvement] JAVA-1586: Throw underlying exception when codec not found in cache
- [bug] JAVA-1583: Handle write failure in ChannelHandlerRequest
- [improvement] JAVA-1541: Reorganize configuration
- [improvement] JAVA-1577: Set default consistency level to LOCAL_ONE
- [bug] JAVA-1548: Retry idempotent statements on READ_TIMEOUT and UNAVAILABLE
- [bug] JAVA-1562: Fix various issues around heart beats
- [improvement] JAVA-1546: Make all statement implementations immutable
- [bug] JAVA-1554: Include VIEW and CDC in WriteType
- [improvement] JAVA-1498: Add a cache above Typesafe config
- [bug] JAVA-1547: Abort pending requests when connection dropped
- [new feature] JAVA-1497: Port timestamp generators from 3.x
- [improvement] JAVA-1539: Configure for deployment to Maven central
- [new feature] JAVA-1519: Close channel if number of orphan stream ids exceeds a configurable 
  threshold
- [new feature] JAVA-1529: Make configuration reloadable
- [new feature] JAVA-1502: Reprepare statements on newly added/up nodes
- [new feature] JAVA-1530: Add ResultSet.wasApplied
- [improvement] JAVA-1531: Merge CqlSession and Session
- [new feature] JAVA-1513: Handle batch statements
- [improvement] JAVA-1496: Improve log messages
- [new feature] JAVA-1501: Reprepare on the fly when we get an UNPREPARED response
- [bug] JAVA-1499: Wait for load balancing policy at cluster initialization
- [new feature] JAVA-1495: Add prepared statements
