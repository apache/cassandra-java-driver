## Changelog

<!-- Note: contrary to 3.x, insert new entries *first* in their section -->

### 4.0.0-alpha2 (in progress)


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
