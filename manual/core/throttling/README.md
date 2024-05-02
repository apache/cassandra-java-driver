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

## Request throttling

### Quick overview

Limit session throughput. 

* `advanced.throttler` in the configuration; defaults to pass-through (no throttling), also
  available: concurrency-based (max simultaneous requests), rate-based (max requests per time unit),
  or write your own.
* metrics: `throttling.delay`, `throttling.queue-size`, `throttling.errors`.

-----

Throttling allows you to limit how many requests a session can execute concurrently. This is
useful if you have multiple applications connecting to the same Cassandra cluster, and want to
enforce some kind of SLA to ensure fair resource allocation.

The request throttler tracks the level of utilization of the session, and lets requests proceed as
long as it is under a predefined threshold. When that threshold is exceeded, requests are enqueued
and will be allowed to proceed when utilization goes back to normal.

From a user's perspective, this process is mostly transparent: any time spent in the queue is
included in the `session.execute()` or `session.executeAsync()` call. Similarly, the request timeout
encompasses throttling: it starts ticking before the request is passed to the throttler; in other
words, a request may time out while it is still in the throttler's queue, before the driver has even
tried to send it to a node.

The only visible effect is that a request may fail with a [RequestThrottlingException], if the
throttler has determined that it can neither allow the request to proceed now, nor enqueue it;
this indicates that your session is overloaded. How you react to that is specific to your
application; typically, you could display an error asking the end user to retry later.

Note that the following requests are also affected by throttling:

* preparing a statement (either directly, or indirectly when the driver reprepares on other nodes,
  or when a node comes back up -- see
  [how the driver prepares](../statements/prepared/#how-the-driver-prepares));
* fetching the next page of a result set (which happens in the background when you iterate the
  synchronous variant `ResultSet`).
* fetching a [query trace](../tracing/).

### Configuration

Request throttling is parameterized in the [configuration](../configuration/) under
`advanced.throttler`. There are various implementations, detailed in the following sections:

#### Pass through

```
datastax-java-driver {
  advanced.throttler {
    class = PassThroughRequestThrottler
  }
}
```

This is a no-op implementation: requests are simply allowed to proceed all the time, never enqueued.

Note that you will still hit a limit if all your connections run out of stream ids. In that case,
requests will fail with an [AllNodesFailedException], with the `getErrors()` method returning a
[BusyConnectionException] for each node. See the [connection pooling](../pooling/) page.

#### Concurrency-based

```
datastax-java-driver {
  advanced.throttler {
    class = ConcurrencyLimitingRequestThrottler
    
    # Note: the values below are for illustration purposes only, not prescriptive
    max-concurrent-requests = 10000
    max-queue-size = 100000
  }
}
```

This implementation limits the number of requests that are allowed to execute simultaneously.
Additional requests get enqueued up to the configured limit. Every time an active request completes
(either by succeeding, failing or timing out), the oldest enqueued request is allowed to proceed.

Make sure you pick a threshold that is consistent with your pooling settings; the driver should
never run out of stream ids before reaching the maximum concurrency, otherwise requests will fail
with [BusyConnectionException] instead of being throttled. The total number of stream ids is a
function of the number of connected nodes and the `connection.pool.*.size` and
`connection.max-requests-per-connection` configuration options. Keep in mind that aggressive
speculative executions and timeout options can inflate stream id consumption, so keep a safety
margin. One good way to get this right is to track the `pool.available-streams` [metric](../metrics)
on every node, and make sure it never reaches 0. See the [connection pooling](../pooling/) page.

#### Rate-based

```
datastax-java-driver {
  advanced.throttler {
    class = RateLimitingRequestThrottler
    
    # Note: the values below are for illustration purposes only, not prescriptive
    max-requests-per-second = 5000
    max-queue-size = 50000
    drain-interval = 1 millisecond
  }
}
```

This implementation tracks the rate at which requests start, and enqueues when it exceeds the
configured threshold.

With this approach, we can't dequeue when requests complete, because having less active requests
does not necessarily mean that the rate is back to normal. So instead the throttler re-checks the
rate periodically and dequeues when possible, this is controlled by the `drain-interval` option.
Picking the right interval is a matter of balance: too low might consume too many resources and only
dequeue a few requests at a time, but too high will delay your requests too much; start with a few
milliseconds and use the `cql-requests` [metric](../metrics/) to check the impact on your latencies.

Like with the concurrency-based throttler, you should make sure that your target rate is in line
with the pooling options; see the recommendations in the previous section.

### Monitoring

Enable the following [metrics](../metrics/) to monitor how the throttler is performing:

```
datastax-java-driver {
  advanced.metrics.session.enabled = [
    # How long requests are being throttled (exposed as a Timer).
    #
    # This is the time between the start of the session.execute() call, and the moment when the
    # throttler allows the request to proceed.
    throttling.delay,
    
    # The size of the throttling queue (exposed as a Gauge<Integer>).
    #
    # This is the number of requests that the throttler is currently delaying in order to
    # preserve its SLA. This metric only works with the built-in concurrency- and rate-based
    # throttlers; in other cases, it will always be 0.
    throttling.queue-size,
    
    # The number of times a request was rejected with a RequestThrottlingException (exposed as a
    # Counter)
    throttling.errors,
  ]
}
```

If you enable `throttling.delay`, make sure to also check the associated extra options to correctly
size the underlying histograms (`metrics.session.throttling.delay.*`).

[RequestThrottlingException]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/RequestThrottlingException.html
[AllNodesFailedException]:    https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/AllNodesFailedException.html
[BusyConnectionException]:    https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/connection/BusyConnectionException.html
