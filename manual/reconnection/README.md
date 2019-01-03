## Reconnection

If the driver loses a connection to a node, it tries to re-establish it according to a configurable
policy. This is used in two places:

* [connection pools](../pooling/): for each node, a session has a fixed-size pool of connections to
  execute user requests. If one or more connections drop, a reconnection gets started for the pool;
  each attempt tries to reopen the missing number of connections. This goes on until the pool is
  back to its expected size;
* [control connection](../control_connection/): a session uses a single connection to an arbitrary
  node for administrative requests. If that connection goes down, a reconnection gets started; each
  attempt iterates through all active nodes until one of them accepts a connection. This goes on
  until we have a control node again.

[ReconnectionPolicy] controls the interval between each attempt. The policy to use may be
provided using [Cluster.Builder.withReconnectionPolicy].  For example, the following configures
an [ExponentialReconnectionPolicy] with a base delay of 1 second, and a max delay of 10 minutes
(this is the default behavior).

```java
Cluster.builder()
  .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 10 * 60 * 1000))
  .build();
```

[ConstantReconnectionPolicy] uses the same delay every time, regardless of the
previous number of attempts.

You can also write your own policy; it must implement [ReconnectionPolicy].

For best results, use reasonable values: very low values (for example a constant delay of 10
milliseconds) will quickly saturate your system.

The policy works by creating a *schedule* each time a reconnection starts. These schedules are
independent across reconnection attempts, meaning that each pool will start with a fresh delay even
if other pools are already reconnecting. For example, assuming that the pool size is 3, the policy
is the exponential one with the default values, and the control connection is initially on node1:

* [t = 0] 2 connections to node2 go down. A reconnection starts for node2's pool, with the next
  attempt in 1 second;
* [t = 1] node2's pool tries to open the 2 missing connections. One succeeds but the other fails.
  Another attempt is scheduled in 2 seconds;
* [t = 1.2] 1 connection to node3 goes down. A reconnection starts for node3's pool, with the next
  attempt in 1 second;
* [t = 1.5] the control connection to node1 goes down. A reconnection starts for the control
  connection, with the next attempt in 1 second;
* [t = 2.2], node3's pool tries to open its missing connection, which succeeds. The pool is back to
  its expected size, node3's reconnection stops;
* [t = 2.5] the control connection tries to find a new node. It invokes the
  [load balancing policy](../load_balancing/) to get a query plan, which happens to start with
  node4. The connection succeeds, node4 is now the control node and the reconnection stops;
* [t = 3] node2's pool tries to open the last missing connection, which succeeds. The pool is back
  to its expected size, node2's reconnection stops.

[ReconnectionPolicy]: https://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/ReconnectionPolicy.html
[Cluster.Builder.withReconnectionPolicy]: https://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/Cluster.Builder.html#withReconnectionPolicy-com.datastax.driver.core.policies.ReconnectionPolicy-
[ExponentialReconnectionPolicy]: https://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/ExponentialReconnectionPolicy.html
[ConstantReconnectionPolicy]:    https://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/ConstantReconnectionPolicy.html
