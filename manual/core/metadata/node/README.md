## Node metadata

### Quick overview

[session.getMetadata().getNodes()][Metadata#getNodes]: all nodes known to the driver (even if not
actively connected).

* [Node] instances are mutable, the fields will update in real time.
* getting notifications:
  [CqlSession.builder().withNodeStateListener][SessionBuilder.withNodeStateListener].

-----

[Metadata#getNodes] returns all the nodes known to the driver when the metadata was retrieved; this
includes down and ignored nodes (see below), so the fact that a node is in this list does not
necessarily mean that the driver is connected to it.

```java
Map<InetSocketAddress, Node> nodes = session.getMetadata().getNodes();
System.out.println("Nodes in the cluster:");
for (Node node : nodes.values()) {
  System.out.printf(
      "  %s is %s and %s (%d connections)%n",
      node.getConnectAddress().getAddress(),
      node.getState(),
      node.getDistance(),
      node.getOpenConnections());
}
```

The returned map is immutable: it does not reflect additions or removals since the metadata was
retrieved. On the other hand, the [Node] object is mutable; you can hold onto an instance across
metadata refreshes and see updates to the fields.

A few notable fields are explained below; for the full details, refer to the Javadocs.

[Node#getState()] indicates how the driver sees the node (see the Javadocs of [NodeState] for the
list of possible states with detailed explanations). In general, the driver tries to be resilient to
spurious DOWN notifications, and will try to use a node as long as it seems up, even if some events
seem to indicate otherwise: for example, if the Cassandra gossip detects a node as down because of
cross-node connectivity issues, but the driver still has active connections to that node, the node
will stay up. Two related properties are [Node#getOpenConnections()] and [Node#isReconnecting()].

[Node#getDatacenter()] and [Node#getRack()] represent the location of the node. This information is
used by some load balancing policies to prioritize coordinators that are physically close to the
client.

[Node#getDistance()] is set by the load balancing policy. The driver does not connect to `IGNORED`
nodes. The exact definition of `LOCAL` and `REMOTE` is left to the interpretation of each policy,
but in general it represents the proximity to the client, and `LOCAL` nodes will be prioritized as
coordinators. They also influence pooling options.

If you need to follow node state changes, you don't need to poll the metadata manually; instead,
you can register a listener to get notified when changes occur:

```java
NodeStateListener listener =
    new NodeStateListenerBase() {
      @Override
      public void onUp(@NonNull Node node) {
        System.out.printf("%s went UP%n", node);
      }
    };
CqlSession session = CqlSession.builder()
    .withNodeStateListener(listener)
    .build();
``` 

See [NodeStateListener] for the list of available methods. [NodeStateListenerBase] is a
convenience implementation with empty methods, for when you only need to override a few of them.

### Advanced topics

#### Forcing a node down

It is possible to temporarily or permanently close all connections to a node and disable
reconnection. The driver does that internally for certain unrecoverable errors (such as a protocol
version mismatch), but this could also be useful for maintenance, or for a custom component (load
balancing policy, etc). 

```java
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;

InternalDriverContext context = (InternalDriverContext) session.getContext();
context.getEventBus().fire(TopologyEvent.forceDown(node1.getConnectAddress()));
context.getEventBus().fire(TopologyEvent.forceUp(node1.getConnectAddress()));
```

As shown by the imports above, forcing a node down requires the *internal* driver API, which is 
reserved for expert usage and subject to the disclaimers in
[API conventions](../../../api_conventions/).

#### Using a custom topology monitor

By default, the driver relies on Cassandra's gossip protocol to receive notifications about the
node states. It opens a control connection to one of the nodes, and registers for server-sent state
events.

Some organizations have their own way of monitoring Cassandra nodes, and prefer to use it instead.
It is possible to completely override the default behavior to bypass gossip. The full details are
beyond the scope of this document; if you're interested, study the `TopologyMonitor` interface in
the source code.


[Metadata#getNodes]:         https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/metadata/Metadata.html#getNodes--
[Node]:                      https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/metadata/Node.html
[Node#getState()]:           https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/metadata/Node.html#getState--
[Node#getDatacenter()]:      https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/metadata/Node.html#getDatacenter--
[Node#getRack()]:            https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/metadata/Node.html#getRack--
[Node#getDistance()]:        https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/metadata/Node.html#getDistance--
[Node#getOpenConnections()]: https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/metadata/Node.html#getOpenConnections--
[Node#isReconnecting()]:     https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/metadata/Node.html#isReconnecting--
[NodeState]:                 https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/metadata/NodeState.html
[NodeStateListener]:         https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/metadata/NodeStateListener.html
[NodeStateListenerBase]:     https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/metadata/NodeStateListenerBase.html
[SessionBuilder.withNodeStateListener]: https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withNodeStateListener-com.datastax.oss.driver.api.core.metadata.NodeStateListener-