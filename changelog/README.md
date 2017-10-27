## Changelog

<!-- Note: contrary to 3.x, insert new entries *first* in their section -->

### 4.0.0-alpha3 (in progress)

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
