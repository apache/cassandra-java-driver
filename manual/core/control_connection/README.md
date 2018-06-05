## Control connection

The control connection is a dedicated connection used for administrative tasks:

* querying system tables to learn about the cluster's [topology](../metadata/node/) and
  [schema](../metadata/schema/);
* checking [schema agreement](../metadata/schema/#schema-agreement);
* reacting to server events, which are used to notify the driver of external topology or schema
  changes.

When the driver starts, the control connection is established to the first contacted node. If that
node goes down, a [reconnection](../reconnection/) is started to find another node; it is governed
by the same policy as regular connections (`advanced.reconnection-policy` options in the
[configuration](../configuration/)), and tries the nodes according to a query plan from the
[load balancing policy](../load_balancing/). 

The control connection is managed independently from [regular pooled connections](../pooling/), and
used exclusively for administrative requests. It shows up in [Node.getOpenConnections], as well as
the `pool.open-connections` [metric](../metrics); for example, if you've configured a pool size of
2, the control node will show 3 connections.

There are a few options to fine tune the control connection behavior in the
`advanced.control-connection` and `advanced.metadata` sections; see the [metadata](../metadata/)
pages and the [reference configuration](../configuration/reference/) for all the details.

[Node.getOpenConnections]: https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/metadata/Node.html#getOpenConnections--