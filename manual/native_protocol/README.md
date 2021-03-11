## Native protocol

The native protocol defines the format of the binary messages exchanged
between the driver and Cassandra over TCP. As a driver user, you don't
need to know the fine details (although the protocol spec is [in the
Cassandra codebase][native_spec] if you're curious); the most visible
aspect is that some features are only available with specific protocol
versions.

[native_spec]: https://github.com/apache/cassandra/tree/trunk/doc

### Compatibility matrix

By default, the protocol version is negotiated between the driver and
Cassandra when the first connection is established. Both sides are
backward-compatible with older versions:

<table border="1" style="text-align:center; width:100%;margin-bottom:1em;">
<tr><th>Driver Version</th><th>Cassandra: 1.2.x<br/>(DSE 3.2)</th><th>2.0.x<br/>(DSE 4.0 to 4.6)
</th><th>2.1.x<br/>(DSE 4.7)</th><th>2.2.x</th><th>3.0.x &amp; 3.x<br/>(DSE 5.0+)</th></tr>
<tr><th>1.0.x</th> <td>v1</td> <td>v1</td>  <td>v1</td> <td>v1</td>  <td>Unsupported <i>(1)</i></td> </tr>
<tr><th>2.0.x to 2.1.1</th> <td>v1</td> <td>v2</td>  <td>v2</td> <td>v2</td> <td>Unsupported <i>(1)</i></td> </tr>
<tr><th>2.1.2 to 2.1.x</th> <td>v1</td> <td>v2</td>  <td>v3</td> <td>v3</td> <td>Unsupported <i>(2)</i></td> </tr>
<tr><th>3.x</th> <td>v1</td> <td>v2</td>  <td>v3</td> <td>v4</td> <td>v4</td> </tr>
</table>

*(1) Cassandra 3.0 does not support protocol versions v1 and v2*

*(2) There is a matching protocol version (v3), but the driver 2.1.x can't read the new system table format of Cassandra 3.0*

For example, if you use version 2.1.5 of the driver to connect to
Cassandra 2.0.9, the maximum version you can use (and the one you'll get
by default) is protocol v2 (third row, second column). If you use the
same version to connect to Cassandra 2.1.4, you can use protocol v3.

### Controlling the protocol version

To find out which version you're currently using, use
[ProtocolOptions#getProtocolVersion()][gpv]:

```java
ProtocolVersion myCurrentVersion = cluster.getConfiguration()
    .getProtocolOptions()
    .getProtocolVersion();
```

The protocol version can not be changed at runtime. However, you can
force a given version at initialization:

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withProtocolVersion(ProtocolVersion.V2)
    .build();
```

If you specify a version that is not compatible with your current
driver/Cassandra combination, you'll get an error:

```
Exception in thread "main" com.datastax.driver.core.exceptions.NoHostAvailableException:
All host(s) tried for query failed
(tried: /127.0.0.1:9042 (com.datastax.driver.core.UnsupportedProtocolVersionException:
  [/127.0.0.1:9042] Host /127.0.0.1:9042 does not support protocol version V3 but V2))
```

[gpv]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/ProtocolOptions.html#getProtocolVersion--

#### Protocol version with mixed clusters

If you have a cluster with mixed versions (for example, while doing a
rolling upgrade of Cassandra), note that **the protocol version will be
negotiated with the first host the driver connects to**.

This could lead to the following situation (assuming you use driver
2.1.2+):

* the first contact point is a 2.1 host, so the driver negotiates
  protocol v3;
* while connecting to the rest of the cluster, the driver contacts a 2.0
  host using protocol v3, which fails; an error is logged and this host
  will be permanently ignored.

To avoid this issue, you can use one the following workarounds:

* always force a protocol version at startup. You keep it at v2 while
  the rolling upgrade is happening, and only switch to v3 when the whole
  cluster has switched to Cassandra 2.1;
* ensure that the list of initial contact points only contains hosts
  with the oldest version (2.0 in this example).


### New features by protocol version

#### v1 to v2

* bound variables in simple statements
  ([Session#execute(String, Object...)](https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/Session.html#execute-java.lang.String-java.lang.Object...-))
* [batch statements](https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/BatchStatement.html)
* [query paging](../paging/)

#### v2 to v3

* the number of stream ids per connection goes from 128 to 32768 (see
  [Connection pooling](../pooling/))
* [serial consistency on batch statements](https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/BatchStatement.html#setSerialConsistencyLevel-com.datastax.driver.core.ConsistencyLevel-)
* [client-side timestamps](../query_timestamps/)

#### v3 to v4

* [query warnings](https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/ExecutionInfo.html#getWarnings--)
* allowed unset values in bound statements
* [Custom payloads](../custom_payloads/)
