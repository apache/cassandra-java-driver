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

## Retries

### Quick overview

What to do when a request failed on a node: retry (same or other node), rethrow, or ignore.

* `advanced.retry-policy` in the configuration. Default policy retries at most once, in cases that
  have a high chance of success; you can also write your own.
* can have per-profile policies. 
* only kicks in if the query is [idempotent](../idempotence).

-----

When a query fails, it sometimes makes sense to retry it: the error might be temporary, or the query
might work on a different node. The driver uses a *retry policy* to determine when and how to retry.

### Built-in retry policies

The driver ships with two retry policies: `DefaultRetryPolicy` –– the default ––  and 
`ConsistencyDowngradingRetryPolicy`. 

The default retry policy should be preferred in most cases as it only retries when *it is perfectly 
safe to do so*, and when *the chances of success are high enough* to warrant a retry.

`ConsistencyDowngradingRetryPolicy` is provided for cases where the application can tolerate a 
temporary degradation of its consistency guarantees. Its general behavior is as follows: if, based 
on the information the coordinator returns, retrying the operation with the initially requested 
consistency has a chance to succeed, do it. Otherwise, if based on this information, we know that 
the initially requested consistency level *cannot be achieved currently*, then:

* For writes, ignore the exception *if we know the write has been persisted on at least one 
  replica*.
* For reads, try reading again at a weaker consistency level.

Keep in mind that this may break invariants! For example, if your application relies on immediate 
write visibility by writing and reading at QUORUM only, downgrading a write to ONE could cause that 
write to go unnoticed by subsequent reads at QUORUM. Furthermore, this policy doesn't always respect 
datacenter locality; for example, it may downgrade LOCAL_QUORUM to ONE, and thus could accidentally 
send a write that was intended for the local datacenter to another datacenter. In summary: **only 
use this retry policy if you understand the consequences.**

Since `DefaultRetryPolicy` is already the driver's default retry policy, no special configuration
is required to activate it. To use `ConsistencyDowngradingRetryPolicy` instead, the following 
option must be declared in the driver [configuration](../configuration/):
                 
```
datastax-java-driver.advanced.retry-policy.class = ConsistencyDowngradingRetryPolicy
```

You can also use your own policy by specifying for the above option the fully-qualified name of a 
class that implements [RetryPolicy].

### Behavior

The behavior of both policies will be detailed in the sections below. 

The policy has several methods that cover different error cases. Each method returns a 
[RetryVerdict]. A retry verdict essentially provides the driver with a [RetryDecision] to indicate 
what to do next. There are four possible retry decisions:

* retry on the same node;
* retry on the next node in the [query plan](../load_balancing/) for this statement;
* rethrow the exception to the user code (from the `session.execute` call, or as a failed future if
  using the asynchronous API);
* ignore the exception. That is, mark the request as successful, and return an empty result set.

#### `onUnavailableVerdict`

A request reached the coordinator, but there weren't enough live replicas to achieve the requested
consistency level. The coordinator replied with an `UNAVAILABLE` error.

If the policy rethrows the error, the user code will get an [UnavailableException]. You can inspect
the exception's fields to get the amount of replicas that were known to be *alive* when the error
was triggered, as well as the amount of replicas that where *required* by the requested consistency
level.

The default policy triggers a maximum of one retry, to the next node in the query plan. The
rationale is that the first coordinator might have been network-isolated from all other nodes
(thinking they're down), but still able to communicate with the client; in that case, retrying on
the same node has almost no chance of success, but moving to the next node might solve the issue.

`ConsistencyDowngradingRetryPolicy` also triggers a maximum of one retry, but instead of trying the
next node, it will downgrade the initial consistency level, if possible, and retry *the same node*.
Note that if it is not possible to downgrade, this policy will rethrow the exception. For example, 
if the original consistency level was QUORUM, and 2 replicas were required to achieve a quorum, but 
only one replica is alive, then the query will be retried with consistency ONE. If no replica was 
alive however, there is no point in downgrading, and the policy will rethrow.

#### `onReadTimeoutVerdict`

A read request reached the coordinator, which initially believed that there were enough live
replicas to process it. But one or several replicas were too slow to answer within the predefined
timeout (`read_request_timeout_in_ms` in `cassandra.yaml`); therefore the coordinator replied to the
client with a `READ_TIMEOUT` error.

This could be due to a temporary overloading of these replicas, or even that they just failed or
were turned off. During reads, Cassandra doesn't request data from every replica to minimize
internal network traffic; instead, some replicas are only asked for a checksum of the data. A read
timeout may occur even if enough replicas responded to fulfill the consistency level, but only
checksum responses were received (the method's `dataPresent` parameter allow you to check if you're
in that situation).

If the policy rethrows the error, the user code will get a [ReadTimeoutException] \(do not confuse
this error with [DriverTimeoutException], which happens when the coordinator didn't reply at all to
the client).

The default policy triggers a maximum of one retry (to the same node), and only if enough replicas
had responded to the read request but data was not retrieved amongst those. That usually means that
enough replicas are alive to satisfy the consistency, but the coordinator picked a dead one for data
retrieval, not having detected that replica as dead yet. The reasoning is that by the time we get
the timeout, the dead replica will likely have been detected as dead and the retry has a high chance
of success.

`ConsistencyDowngradingRetryPolicy` behaves like the default policy when enough replicas responded.
If not enough replicas responded however, it will attempt to downgrade the initial consistency 
level, and retry *the same node*. If it is not possible to downgrade, this policy will rethrow the 
exception.

#### `onWriteTimeoutVerdict`

This is similar to `onReadTimeout`, but for write operations. The reason reads and writes are
handled separately is because a read is obviously a non mutating operation, whereas a write is
likely to be. If a write times out at the coordinator level, there is no way to know whether the
mutation was applied or not on the non-answering replica.

If the policy rethrows the error, the user code will get a [WriteTimeoutException].

This method is only invoked for [idempotent](../idempotence/) statements. Otherwise, the driver
bypasses the retry policy and always rethrows the error.

The default policy triggers a maximum of one retry (to the same node), and only for a `BATCH_LOG`
write. The reasoning is that the coordinator tries to write the distributed batch log against a
small subset of nodes in the local datacenter; a timeout usually means that none of these nodes were
alive but the coordinator hadn't detected them as dead yet. By the time we get the timeout, the dead
nodes will likely have been detected as dead, and the retry has a high chance of success.

`ConsistencyDowngradingRetryPolicy` also triggers a maximum of one retry, but behaves differently:

* For `SIMPLE` and `BATCH` write types: if at least one replica acknowledged the write, the policy 
  will assume that the write will be eventually replicated, and decide to ignore the error; in other
  words, it will consider that the write already succeeded, albeit with weaker consistency 
  guarantees: retrying is therefore useless. If no replica acknowledged the write, the policy will 
  rethrow the error.
* For `UNLOGGED_BATCH` write type: since only part of the batch could have been persisted, the
  policy will attempt to downgrade the consistency level and retry *on the same node*. If 
  downgrading is not possible, the policy will rethrow. 
* For `BATCH_LOG` write type: the policy will retry the same node, for the reasons explained above.
* For other write types: the policy will always rethrow.

#### `onRequestAbortedVerdict`

The request was aborted before we could get a response from the coordinator. This can happen in two
cases:

* if the connection was closed due to an external event. This will manifest as a
  [ClosedConnectionException] \(network failure) or [HeartbeatException] \(missed
  [heartbeat](../pooling/#heartbeat));
* if there was an unexpected error while decoding the response (this can only be a driver bug).

This method is only invoked for [idempotent](../idempotence/) statements. Otherwise, the driver
bypasses the retry policy and always rethrows the error.

Both the default policy and `ConsistencyDowngradingRetryPolicy` retry on the next node if the 
connection was closed, and rethrow (assuming a driver bug) in all other cases.

#### `onErrorResponseVerdict`

The coordinator replied with an error other than `READ_TIMEOUT`, `WRITE_TIMEOUT` or `UNAVAILABLE`.
Namely, this covers [OverloadedException], [ServerError], [TruncateException],
[ReadFailureException] and [WriteFailureException].

This method is only invoked for [idempotent](../idempotence/) statements. Otherwise, the driver
bypasses the retry policy and always rethrows the error.

Both the default policy and `ConsistencyDowngradingRetryPolicy` rethrow read and write failures, 
and retry other errors on the next node.

### Hard-coded rules

There are a few cases where retrying is always the right thing to do. These are not covered by
`RetryPolicy`, but instead hard-coded in the driver:

* **any error before a network write was attempted**: to send a query, the driver selects a node,
  borrows a connection from the host's [connection pool](../pooling/), and then writes the message
  to the connection. Errors can occur before the write was even attempted, for example if the
  connection pool is saturated, or if the node went down right after we borrowed. In those cases, it
  is always safe to retry since the request wasn't sent, so the driver will transparently move to
  the next node in the query plan.
* **re-preparing a statement**: when the driver executes a prepared statement, it may find out that
  the coordinator doesn't know about it, and need to re-prepare it on the fly (this is described in
  detail [here](../statements/prepared/)). The query is then retried on the same node.
* **trying to communicate with a node that is bootstrapping**: this is a rare edge case, as in
  practice the driver should never try to communicate with a bootstrapping node (the only way is if
  it was specified as a contact point). It is again safe to assume that the query was not executed
  at all, so the driver moves to the next node.

Similarly, some errors have no chance of being solved by a retry. They will always be rethrown
directly to the user. These include:

* [QueryValidationException] and any of its subclasses;
* [FunctionFailureException];
* [ProtocolError].

### Using multiple policies

The retry policy can be overridden in [execution profiles](../configuration/#profiles):

```
datastax-java-driver {
  advanced.retry-policy {
    class = DefaultRetryPolicy
  }
  profiles {
    custom-retries {
      advanced.retry-policy {
        class = CustomRetryPolicy
      }
    }
    slow {
      request.timeout = 30 seconds
    }
  }
}
```

The `custom-retries` profile uses a dedicated policy. The `slow` profile inherits the default
profile's. Note that this goes beyond configuration inheritance: the driver only creates a single
`DefaultRetryPolicy` instance and reuses it (this also occurs if two sibling profiles have the same
configuration).

Each request uses its declared profile's policy. If it doesn't declare any profile, or if the
profile doesn't have a dedicated policy, then the default profile's policy is used.

[AllNodesFailedException]:   https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/AllNodesFailedException.html
[ClosedConnectionException]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/connection/ClosedConnectionException.html
[DriverTimeoutException]:    https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/DriverTimeoutException.html
[FunctionFailureException]:  https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/servererrors/FunctionFailureException.html
[HeartbeatException]:        https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/connection/HeartbeatException.html
[ProtocolError]:             https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/servererrors/ProtocolError.html
[OverloadedException]:       https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/servererrors/OverloadedException.html
[QueryValidationException]:  https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/servererrors/QueryValidationException.html
[ReadFailureException]:      https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/servererrors/ReadFailureException.html
[ReadTimeoutException]:      https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/servererrors/ReadTimeoutException.html
[RetryDecision]:             https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/retry/RetryDecision.html
[RetryPolicy]:               https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/retry/RetryPolicy.html
[RetryVerdict]:              https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/retry/RetryVerdict.html
[ServerError]:               https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/servererrors/ServerError.html
[TruncateException]:         https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/servererrors/TruncateException.html
[UnavailableException]:      https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/servererrors/UnavailableException.html
[WriteFailureException]:     https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/servererrors/WriteFailureException.html
[WriteTimeoutException]:     https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/servererrors/WriteTimeoutException.html
