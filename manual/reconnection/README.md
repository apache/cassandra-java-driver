<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

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

[ReconnectionPolicy]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/policies/ReconnectionPolicy.html
[Cluster.Builder.withReconnectionPolicy]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/Cluster.Builder.html#withReconnectionPolicy-com.datastax.driver.core.policies.ReconnectionPolicy-
[ExponentialReconnectionPolicy]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/policies/ExponentialReconnectionPolicy.html
[ConstantReconnectionPolicy]:    https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/policies/ConstantReconnectionPolicy.html
