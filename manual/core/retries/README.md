## Retries

When a query fails, it sometimes makes sense to retry it: the error might be temporary, or the query
might work on a different node. The driver uses a *retry policy* to determine when and how to retry.
It is defined in the [configuration](../configuration/):
                     
```
datastax-java-driver.request.retry-policy {
  class = com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy
}
```

The behavior of the default policy will be detailed in the sections below. You can also use your
own policy by specifying the fully-qualified name of a class that implements [RetryPolicy].

The policy has several methods that cover different error cases. Each method returns a decision to
indicate what to do next:

* retry on the same node;
* retry on the next node in the [query plan](../load_balancing/) for this statement;
* rethrow the exception to the user code (from the `session.execute` call, or as a failed future if
  using the asynchronous API);
* ignore the exception. That is, mark the request as successful, and return an empty result set.

### onUnavailable

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

### onReadTimeout

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

### onWriteTimeout

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

### onRequestAborted

The request was aborted before we could get a response from the coordinator. This can happen in two
cases:

* if the connection was closed due to an external event. This will manifest as a
  [ClosedConnectionException] \(network failure) or [HeartbeatException] \(missed
  [heartbeat](../pooling/#heartbeat));
* if there was an unexpected error while decoding the response (this can only be a driver bug).

This method is only invoked for [idempotent](../idempotence/) statements. Otherwise, the driver
bypasses the retry policy and always rethrows the error.

The default policy retries on the next node if the connection was closed, and rethrows (assuming a
driver bug) in all other cases.

### onErrorResponse

The coordinator replied with an error other than `READ_TIMEOUT`, `WRITE_TIMEOUT` or `UNAVAILABLE`.
Namely, this covers [OverloadedException], [ServerError], [TruncateException],
[ReadFailureException] and [WriteFailureException].

This method is only invoked for [idempotent](../idempotence/) statements. Otherwise, the driver
bypasses the retry policy and always rethrows the error.

The default policy rethrows read and write failures, and retries other errors on the next node.

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

The retry policy can be overridden in [configuration profiles](../configuration/#profiles):

```
datastax-java-driver {
  request.retry-policy {
    class = DefaultRetryPolicy
  }
  profiles {
    custom-retries {
      request.retry-policy {
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

[AllNodesFailedException]:   https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/AllNodesFailedException.html
[ClosedConnectionException]: https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/connection/ClosedConnectionException.html
[DriverTimeoutException]:    https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/DriverTimeoutException.html
[FunctionFailureException]:  https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/servererrors/FunctionFailureException.html
[HeartbeatException]:        https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/connection/HeartbeatException.html
[ProtocolError]:             https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/servererrors/ProtocolError.html
[OverloadedException]:       https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/servererrors/OverloadedException.html
[QueryValidationException]:  https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/servererrors/QueryValidationException.html
[ReadFailureException]:      https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/servererrors/ReadFailureException.html
[ReadTimeoutException]:      https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/servererrors/ReadTimeoutException.html
[RetryDecision]:             https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/retry/RetryDecision.html
[RetryPolicy]:               https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/retry/RetryPolicy.html
[ServerError]:               https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/servererrors/ServerError.html
[TruncateException]:         https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/servererrors/TruncateException.html
[UnavailableException]:      https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/servererrors/UnavailableException.html
[WriteFailureException]:     https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/servererrors/WriteFailureException.html
[WriteTimeoutException]:     https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/servererrors/WriteTimeoutException.html
