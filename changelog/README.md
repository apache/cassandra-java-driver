## Changelog

<!-- Note: contrary to 3.x, insert new entries *first* in their section -->

### 4.0.0 (in progress)

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
