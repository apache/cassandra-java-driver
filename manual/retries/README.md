## Retries

When a query fails, it sometimes makes sense to retry it: the error might be temporary, or the query might work on a
different host, or with different options.

The driver uses a configurable set of rules to determine when and how to retry.

### Concepts

When the driver executes a statement, it first obtains a [query plan] \(a list of hosts) from the load balancing policy.
Then it picks the first host and sends it the request; this host acts as the **coordinator** for the query, it will
communicate with the rest of the cluster and reply to the client.

If the coordinator can't be reached or replies with an error, there are various things that the driver can do; they are
expressed as [RetryDecision] objects:

* [retry()]: retry the query on the same host. It's possible to retry with a different consistency level than the one
  that was originally requested;
* [tryNextHost()]: retry on the next host in the query plan. Again, it can be with a different CL;
* [rethrow()]: rethrow the exception to the user code. This means it will be thrown from the `session.execute` call (or
  returned as a failed future if `executeAsync` was used);
* [ignore()]: mark the request as successful, and return an empty result set.

If the driver retries on every host and reaches the end of the query plan, a [NoHostAvailableException] is thrown to the
user code. You can use its [getErrors()] method to find out what went wrong on each host.


### Retry policy

[RetryPolicy] is a pluggable component that determines the retry decisions for various types of errors. It is configured
when initializing the cluster:

```java
Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withRetryPolicy(new MyCustomPolicy())
        .build();
```

Once the cluster has been built, you can't change the policy, but you may inspect it at runtime:

```java
RetryPolicy policy = cluster.getConfiguration().getPolicies().getRetryPolicy();
```

If you don't explicitly configure it, you get a [DefaultRetryPolicy].

The policy's methods cover different types of errors:

#### [onUnavailable]

A request reached the coordinator, but there weren't enough live replicas to achieve the requested consistency level.
The coordinator replied with an `UNAVAILABLE` error.

If the policy rethrows the error, the user code will get an [UnavailableException]. You can inspect the exception's
fields to get the amount of replicas that were *known* to be alive when the error was triggered, as well as the amount
of replicas that where *required* by the requested consistency level.

#### [onReadTimeout]

A read request reached the coordinator, which initially believed that there were enough live replicas to process it.
But, for some reason, one or several replicas were too slow to answer within the predefined timeout
(`read_request_timeout_in_ms` in `cassandra.yaml`), and the coordinator replied to the client with a `READ_TIMEOUT`
error.

This could be due to a temporary overloading of these replicas, or even
that they just failed or were turned off. During reads, Cassandra doesn't request data from every replica to minimize
internal network traffic; instead, some replicas are only asked for a checksum of the data. A read timeout may occur
even if enough replicas responded to fulfill the consistency level, but only checksum responses were received (the
method's `dataRetrieved` parameter allow you to check if you're in that situation).

If the policy rethrows the error, the user code will get a [ReadTimeoutException].

Note: do not confuse this error with a [driver read timeout], which happens when the coordinator didn't reply at all to
the client.

#### [onWriteTimeout]

This is similar to `onReadTimeout`, but for write operations. The reason reads and writes are handled separately is
because a read is obviously a non mutating operation, whereas a write is likely to be. If a write times out at the
coordinator level, there is no way to know whether the mutation was applied or not on the non-answering replica.
Policy implementations are usually more conservative in `onWriteTimeout` in case the write was not idempotent.

If the policy rethrows the error, the user code will get a [WriteTimeoutException].

#### [onRequestError]

This gets called for any other error occurring after the request was sent.

The method receives the exception as a parameter, so that implementations can refine their decision based on what
happened. The possible exceptions are:

* [ServerError]: thrown by the coordinator when an unexpected error occurs. This is generally a Cassandra bug;
* [OperationTimedOutException]: thrown by the client when it didn't hear back from the coordinator within the
  [driver read timeout];
* [ConnectionException]: thrown by the client for any network issue while or after the request was written;
* [OverloadedException]: thrown by the coordinator when replicas are down and the number of hinted handoffs gets too
  high; the coordinator temporarily refuses writes for these replicas (see [hinted handoffs] in the Cassandra
  documentation).

In all these cases, there is no absolute certainty that the request was not applied by a host, so implementations must
consider the risk of retrying non-idempotent statements.

### Hard-coded rules

There are a few cases where retrying is always the right thing to do. These are not covered by `RetryPolicy`, but
hard-coded in the driver:

* **any error before a network write was attempted**: to send a query, the driver selects a host, borrows a connection
  from the host's [connection pool], and then writes the message to the connection.
  Errors can occur before the write was even attempted, for example if the connection pool is saturated, or if the
  host went down right after we borrowed. In those cases, it is always safe to retry since the request wasn't sent, so
  the driver will transparently move to the next host in the query plan.

* **re-preparing a statement**: when the driver executes a prepared statement, it may find out that the coordinator
  doesn't know about it, and need to re-prepare it on the fly (this is described in detail [here][prepared]). The query
  is then retried on the same host.

* **trying to communicate with a host that is bootstrapping**: this is a rare edge case, as in practice the driver
  should never try to communicate with a bootstrapping host (the only way is if it was specified as a contact point).
  Anyway, it is again safe to assume that the query was not executed at all, so the driver moves to the next host.

Similarly, some errors have no chance of being solved by a retry. They will always be rethrown directly to the user.
These include:

* [QueryValidationException] and any of its subclasses ([InvalidQueryException], [InvalidConfigurationInQueryException],
  [UnauthorizedException], [SyntaxError], [AlreadyExistsException]);
* [TruncateException].


### Retries and idempotence

Retry policies should avoid retrying [idempotent queries] when it's not clear whether the query was applied or not.

It's always safe to retry in `onReadTimeout`, since by definition we know that the query is a read, which doesn't mutate
any data. Similarly, `onUnavailable` is safe: the coordinator is telling us that it didn't find enough replicas, so we
know that it didn't try to apply the query.

`onWriteTimeout` is not safe. The default retry policy is very conservative (it only retries batch log writes) so it
will never cause any issue. Custom implementations should check the statements' idempotent flag.

`onRequestError` isn't safe either.

**For historical reasons, the built-in retry policy implementations do not check the idempotent flag in
`onRequestError`**. This is based on the fact that:

* previous versions of the driver (which did `onRequestError` internally) didn't check the flag either;
* the majority of queries in an application should be idempotent;
* statements start out as non-idempotent by default.

If `onRequestError` enforced idempotence strictly, this would cause a lot of queries that were legitimately retried with
previous driver versions to not be retried anymore. This would be a big behavioral change, so we decided to prefer
consistency with previous versions.

If you prefer strict handling of the idempotent flag, you can wrap your retry policy into an
[IdempotenceAwareRetryPolicy]. Make sure to position the flag properly on all statements.


[RetryDecision]:                        http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/RetryPolicy.RetryDecision.html
[retry()]:                              http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/RetryPolicy.RetryDecision.html#retry-com.datastax.driver.core.ConsistencyLevel-
[tryNextHost()]:                        http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/RetryPolicy.RetryDecision.html#tryNextHost-com.datastax.driver.core.ConsistencyLevel-
[rethrow()]:                            http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/RetryPolicy.RetryDecision.html#rethrow--
[ignore()]:                             http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/RetryPolicy.RetryDecision.html#ignore--
[NoHostAvailableException]:             http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/NoHostAvailableException.html
[getErrors()]:                          http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/NoHostAvailableException.html#getErrors--
[RetryPolicy]:                          http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/RetryPolicy.html
[DefaultRetryPolicy]:                   http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/DefaultRetryPolicy.html
[onReadTimeout]:                        http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/DefaultRetryPolicy.html#onReadTimeout-com.datastax.driver.core.Statement-com.datastax.driver.core.ConsistencyLevel-int-int-boolean-int-
[onWriteTimeout]:                       http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/DefaultRetryPolicy.html#onWriteTimeout-com.datastax.driver.core.Statement-com.datastax.driver.core.ConsistencyLevel-com.datastax.driver.core.WriteType-int-int-int-
[onUnavailable]:                        http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/DefaultRetryPolicy.html#onUnavailable-com.datastax.driver.core.Statement-com.datastax.driver.core.ConsistencyLevel-int-int-int-
[onRequestError]:                       http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/ExtendedRetryPolicy.html#onRequestError-com.datastax.driver.core.Statement-com.datastax.driver.core.ConsistencyLevel-java.lang.Exception-int-
[UnavailableException]:                 http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/UnavailableException.html
[ReadTimeoutException]:                 http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/ReadTimeoutException.html
[WriteTimeoutException]:                http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/WriteTimeoutException.html
[OverloadedException]:                  http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/OverloadedException.html
[ServerError]:                          http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/ServerError.html
[OperationTimedOutException]:           http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/OperationTimedOutException.html
[ConnectionException]:                  http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/ConnectionException.html
[QueryValidationException]:             http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/QueryValidationException.html
[InvalidQueryException]:                http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/InvalidQueryException.html
[InvalidConfigurationInQueryException]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/InvalidConfigurationInQueryException.html
[UnauthorizedException]:                http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/UnauthorizedException.html
[SyntaxError]:                          http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/SyntaxError.html
[AlreadyExistsException]:               http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/AlreadyExistsException.html
[TruncateException]:                    http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/exceptions/TruncateException.html
[IdempotenceAwareRetryPolicy]:          http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/IdempotenceAwareRetryPolicy.html

[query plan]: ../load_balancing/#query-plan
[connection pool]: ../pooling/
[prepared]: ../statements/prepared/#preparing-on-multiple-nodes
[driver read timeout]: ../socket_options/#driver-read-timeout
[hinted handoffs]: https://docs.datastax.com/en/cassandra/2.1/cassandra/dml/dml_about_hh_c.html?scroll=concept_ds_ifg_jqx_zj__performance
[idempotent queries]: ../idempotence/