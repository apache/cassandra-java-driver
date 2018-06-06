## Changelog

<!-- Note: contrary to 3.x, insert new entries *first* in their section -->

### 4.0.0-alpha4 (in progress)

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
