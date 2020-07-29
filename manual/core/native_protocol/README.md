## Native protocol

### Quick overview

Low-level binary format. Mostly irrelevant for everyday use, only governs whether certain features
are available.

* setting the version:
  * automatically negotiated during the connection (improved algorithm in driver 4, no longer an
    issue in mixed clusters).
  * or force with `advanced.protocol.version` in the configuration.
* reading the version:
  [session.getContext().getProtocolVersion()][AttachmentPoint.getProtocolVersion].

-----

The native protocol defines the format of the binary messages exchanged between the driver and
Cassandra over TCP. As a driver user, you don't need to know the fine details (although the
[protocol spec] is available if you're curious); the most visible aspect is that some features are
only available with specific protocol versions.

### Compatibility matrix

Java driver 4 supports protocol versions 3 to 5. By default, the version is negotiated with the
first node the driver connects to:

| Cassandra version   | Negotiated protocol version with driver 4 ¹     |
|---------------------|-------------------------------------------------|
| 2.1.x               | v3                                              |
| 2.2.x               | v4                                              |
| 3.x                 | v4                                              |
| 4.x ²               | v5                                              |

*(1) for previous driver versions, see the [3.x documentation][driver3]*

*(2) at the time of writing, Cassandra 4 is not released yet. Protocol v5 support is still in beta,
and must be enabled explicitly (negotiation will yield v4).*

Since version 4.5.0, the driver can also use DSE protocols when all nodes are running a version of
DSE. The table below shows the protocol matrix for these cases:

| DSE version         | Negotiated protocol version with driver 4       |
|---------------------|-------------------------------------------------|
| 4.7/4.8             | v3                                              |
| 5.0                 | v4                                              |
| 5.1                 | DSE_V1 ³                                        |
| 6.0/6.7/6.8         | DSE_V2 ³                                        |

*(3) DSE Protocols are chosen before other Cassandra native protocols.

### Controlling the protocol version

To find out which version you're currently using, use the following:

```java
ProtocolVersion currentVersion = session.getContext().getProtocolVersion();
```

The protocol version cannot be changed at runtime. However, you can force a particular version in
the [configuration](../configuration/):

```
datastax-java-driver {
  advanced.protocol {
    version = v3
  }
}
```

If you force a version that is too high for the server, you'll get an error:

```
Exception in thread "main" com.datastax.oss.driver.api.core.AllNodesFailedException:
  All 1 node tried for the query failed (showing first 1 nodes, use getAllErrors() for more:
    /127.0.0.1:9042: com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException:
                     [/127.0.0.1:9042] Host does not support protocol version V5)
```

### Protocol version with mixed clusters

It's possible to have heterogeneous Cassandra versions in your cluster, in particular during a
rolling upgrade. This used to be a problem with previous driver versions, which would negotiate a
version with the first contacted node that might not work with others.

Starting with driver 4, protocol negotiation uses an improved strategy to prevent those issues:

* the driver negotiates with the first node for the initial connection (for example, v4 for
  Cassandra 3);
* right after connecting, it queries the `system.peers` table to find out the Cassandra version of
  the other nodes (for example, node2 → Cassandra 3, node3 → Cassandra 2.1);
* it infers the highest supported protocol version for each node (node2 → v4, node3 → v3);
* it selects the minimum of those protocol versions (v3). If that is lower than the initially
  negotiated version, the first connection is closed and reopened; 
* the connection to the rest of the nodes proceeds with the possibly downgraded protocol version.

Thanks to this approach, automatic negotiation works even with mixed clusters, you don't need to
force the protocol version manually anymore. 

### Debugging protocol negotiation

You can observe the negotiation process in the [logs](../logging/).
 
The versions tried while negotiating with the first node are logged at level `DEBUG` in the category
`com.datastax.oss.driver.internal.core.channel.ChannelFactory`:

```
DEBUG ChannelFactory - Failed to connect with protocol v4, retrying with v3
```

If a mixed cluster renegotiation happens, it is logged at level `INFO` in the category
`com.datastax.oss.driver.internal.core.session.DefaultSession`:

```
INFO DefaultSession - Negotiated protocol version v4 for the initial contact point, but other nodes
                      only support v3, downgrading
```

If you want to see the details of mixed cluster negotiation, enable `DEBUG` level for the category
`com.datastax.oss.driver.internal.core.CassandraProtocolVersionRegistry`.

### New features by protocol version

#### v3 to v4

* [query warnings][ExecutionInfo.getWarnings]
* [unset values in bound statements](../statements/prepared/#unset-values)
* [custom payloads][Request.getCustomPayload]

#### v4 to v5

* [per-query keyspace](../statements/per_query_keyspace)
* [improved prepared statement resilience](../statements/prepared/#prepared-statements-and-schema-changes)
  in the face of schema changes

[protocol spec]: https://github.com/datastax/native-protocol/tree/1.x/src/main/resources
[driver3]: https://docs.datastax.com/en/developer/java-driver/3.10/manual/native_protocol/

[ExecutionInfo.getWarnings]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/cql/ExecutionInfo.html#getWarnings--
[Request.getCustomPayload]:  https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/session/Request.html#getCustomPayload--
[AttachmentPoint.getProtocolVersion]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/detach/AttachmentPoint.html#getProtocolVersion--