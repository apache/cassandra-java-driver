## Native protocol

The native protocol defines the format of the binary messages exchanged
between the driver and Cassandra over TCP. As a driver user, you don't
need to know the fine details (although the protocol spec is [in the
Cassandra codebase][native_spec] if you're curious); the most visible
aspect is that some features are only available with specific protocol
versions.

[native_spec]: https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=tree;f=doc;hb=HEAD

### Compatibility matrix

By default, the protocol version is negotiated between the driver and
Cassandra when the first connection is established. Both sides are
backward-compatible with older versions:

<table border="1" style="text-align:center; width:100%;margin-bottom:1em;">
<tr><td>&nbsp;</td><td>Cassandra: 1.2.x<br/>(DSE 3.2)</td><td>2.0.x<br/>(DSE 4.0 to 4.6)</td></tr>
<tr><td>Driver: 1.0.x</td> <td>v1</td> <td>v1</td></tr>
<tr><td>2.0.x or more</td> <td>v1</td> <td>v2</td></tr>
</table>

For example, if you use version 2.0.10 of the driver to connect to
Cassandra 1.2.19, the maximum version you can use (and the one you'll get
by default) is protocol v1 (bottom line, left column). If you use the
same version to connect to Cassandra 2.0.9, you can use protocol v2.

### Controlling the protocol version

To find out which version you're currently using, use
[ProtocolOptions#getProtocolVersion()][gpv]:

```java
int myCurrentVersion = cluster.getConfiguration()
    .getProtocolOptions()
    .getProtocolVersion();
```

The protocol version can not be changed at runtime. However, you can
force a given version at initialization:

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withProtocolVersion(1)
    .build();
```

If you specify a version that is not compatible with your current
driver/Cassandra combination, you'll get an error:

```
Exception in thread "main" com.datastax.driver.core.exceptions.NoHostAvailableException:
All host(s) tried for query failed
(tried: /127.0.0.1:9042 (com.datastax.driver.core.UnsupportedProtocolVersionException:
  [/127.0.0.1:9042] Host /127.0.0.1:9042 does not support protocol version 2))
```

[gpv]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/ProtocolOptions.html#getProtocolVersion()

#### Protocol version with mixed clusters

If you have a cluster with mixed versions (for example, while doing a
rolling upgrade of Cassandra), note that **the protocol version will be
negotiated with the first host the driver connects to**.

This could lead to the following situation (assuming you use driver
2.x):

* the first contact point is a 2.0 host, so the driver negotiates
  protocol v2;
* while connecting to the rest of the cluster, the driver contacts a 1.2
  host using protocol v2, which fails; an error is logged and this host
  will be permanently ignored.

To avoid this issue, you can use one the following workarounds:

* always force a protocol version at startup. You keep it at v1 while
  the rolling upgrade is happening, and only switch to v2 when the whole
  cluster has switched to Cassandra 2.0;
* ensure that the list of initial contact points only contains hosts
  with the oldest version (1.2 in this example).


### New features by protocol version

#### v1 to v2

* bound variables in simple statements
  ([Session#execute(String, Object...)](http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Session.html#execute(java.lang.String,%20java.lang.Object...)))
* [batch statements](http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/BatchStatement.html)
* [query paging](../paging/)
