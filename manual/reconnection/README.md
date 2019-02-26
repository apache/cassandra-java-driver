## Reconnection

If the driver loses a connection to a node, it tries to re-establish it according to a configurable
policy. This is used in two places:

* [connection pools](../pooling/): for each node, a session has a fixed-size pool of connections to
  execute user requests. If a node is detected as down, a reconnection is started.
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

[ReconnectionPolicy]: https://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/core/policies/ReconnectionPolicy.html
[Cluster.Builder.withReconnectionPolicy]: https://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/core/Cluster.Builder.html#withReconnectionPolicy-com.datastax.driver.core.policies.ReconnectionPolicy-
[ExponentialReconnectionPolicy]: https://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/core/policies/ExponentialReconnectionPolicy.html
[ConstantReconnectionPolicy]:    https://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/core/policies/ConstantReconnectionPolicy.html
