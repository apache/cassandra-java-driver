## Reconnection

### Quick overview

When a connection is lost, try to reestablish it at configured intervals.

* `advanced.reconnection-policy` in the configuration; defaults to exponential backoff, also
  available: constant delay, write your own.
* applies to connection pools and the control connection.
* `advanced.reconnect-on-init` (false by default) controls whether the session tries to reconnect
  when it is first created

-----

### At runtime

If a running session loses a connection to a node, it tries to re-establish it according to a
configurable policy. This is used in two places:

* [connection pools](../pooling/): for each node, a session has a fixed-size pool of connections to
  execute user requests. If one or more connections drop, a reconnection gets started for the pool;
  each attempt tries to reopen the missing number of connections. This goes on until the pool is
  back to its expected size;
* [control connection](../control_connection/): a session uses a single connection to an arbitrary
  node for administrative requests. If that connection goes down, a reconnection gets started; each
  attempt iterates through all active nodes until one of them accepts a connection. This goes on
  until we have a control node again.

The reconnection policy controls the interval between each attempt. It is defined in the
[configuration](../configuration/):

```
datastax-java-driver {
  advanced.reconnection-policy {
    class = ExponentialReconnectionPolicy
    base-delay = 1 second
    max-delay = 60 seconds
  }
}
```

[ExponentialReconnectionPolicy] is the default; it starts with a base delay, and then doubles it
after each attempt. [ConstantReconnectionPolicy] uses the same delay every time, regardless of the
previous number of attempts. 

You can also write your own policy; it must implement [ReconnectionPolicy] and declare a public
constructor with a [DriverContext] argument. 

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

### At init time

If a session fails to connect when it is first created, the default behavior is to abort and throw
an error immediately.

If you prefer to retry, you can set the configuration option `advanced.reconnect-on-init` to true.
Instead of failing, the driver will keep attempting to initialize the session at regular intervals,
according to the reconnection policy, until at least one contact point replies. This can be useful
when dealing with containers and microservices.

Note that the session is not accessible until it is fully ready: the `CqlSessionBuilder.build()`
call &mdash; or the future returned by `buildAsync()` &mdash; will not complete until the connection
was established.

[ConstantReconnectionPolicy]:    https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/internal/core/connection/ConstantReconnectionPolicy.html
[DriverContext]:                 https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/context/DriverContext.html
[ExponentialReconnectionPolicy]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/internal/core/connection/ExponentialReconnectionPolicy.html
[ReconnectionPolicy]:            https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/connection/ReconnectionPolicy.html