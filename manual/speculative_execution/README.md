## Speculative query execution

Sometimes a Cassandra node might be experiencing difficulties (ex: long
GC pause) and take longer than usual to reply. Queries sent to that node
will experience bad latency.

One thing we can do to improve that is pre-emptively start a second
execution of the query against another node, before the first node has
replied or errored out. If that second node replies faster, we can send
the response back to the client (we also cancel the first query):

```ditaa
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query host1
  |                |------------->|
  |                |              |
  |                |              |
  |                |     query host2
  |                |-------------------->|
  |                |              |      |
  |                |              |      |
  |                |     host2 replies   |
  |                |<--------------------|
  |   complete     |              |
  |<---------------|              |
  |                | cancel       |
  |                |------------->|
```

Or the first node could reply just after the second execution was
started. In this case, we cancel the second execution. In other words,
whichever node replies faster "wins" and completes the client query:

```ditaa
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query host1
  |                |------------->|
  |                |              |
  |                |              |
  |                |     query host2
  |                |-------------------->|
  |                |              |      |
  |                |              |      |
  |                | host1 replies|      |
  |                |<-------------|      |
  |   complete     |                     |
  |<---------------|                     |
  |                | cancel              |
  |                |-------------------->|
```

Speculative executions are **disabled** by default. The following
sections cover the practical details and how to enable them.

### Query idempotence

If a query is [not idempotent](../idempotence/), the driver will never schedule speculative executions for it, because
there is no way to guarantee that only one node will apply the mutation.

### Enabling speculative executions

Speculative executions are controlled by an instance of
[SpeculativeExecutionPolicy][sep] provided when initializing the
`Cluster`.  This policy defines the threshold after which a new
speculative execution will be triggered.

[sep]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/SpeculativeExecutionPolicy.html

Two implementations are provided with the driver:

#### [ConstantSpeculativeExecutionPolicy][csep]

This simple policy uses a constant threshold:

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withSpeculativeExecutionPolicy(
        new ConstantSpeculativeExecutionPolicy(
            500, // delay before a new execution is launched
            2    // maximum number of executions
        ))
    .build();
```

Given the above configuration, an idempotent query would be handled this
way:

* start the initial execution at t0;
* if no response has been received at t0 + 500 milliseconds, start a
  speculative execution on another node;
* if no response has been received at t0 + 1000 milliseconds, start
  another speculative execution on a third node.

[csep]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/ConstantSpeculativeExecutionPolicy.html

#### [PercentileSpeculativeExecutionPolicy][psep]

This policy sets the threshold at a given latency percentile for the
current host, based on recent statistics.

**As of 2.1.6, this class is provided as a beta preview: it hasn't been
extensively tested yet, and the API is still subject to change.**

First and foremost, make sure that the [HdrHistogram][hdr] library (used
under the hood to collect latencies) is in your classpath. It's defined
as an optional dependency in the driver's POM, so you'll need to
explicitly depend on it:

```xml
<dependency>
  <groupId>org.hdrhistogram</groupId>
  <artifactId>HdrHistogram</artifactId>
  <version>2.1.9</version>
</dependency>
```

Then create an instance of [PerHostPercentileTracker][phpt] that will collect
latency statistics for your `Cluster`:

```java
// There are more options than shown here, please refer to the API docs
// for more information
PerHostPercentileTracker tracker = PerHostPercentileTracker
    .builderWithHighestTrackableLatencyMillis(15000)
    .build();
```

Create an instance of the policy with the tracker, and pass it to your
cluster:

```java
PercentileSpeculativeExecutionPolicy policy =
    new PercentileSpeculativeExecutionPolicy(
        tracker,
        99.0,     // percentile
        2);       // maximum number of executions

Cluster cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withSpeculativeExecutionPolicy(policy)
    .build();
```

Finally, don't forget to register your tracker with the cluster (the
policy does not do this itself):

```java
cluster.register(tracker);
```

Note that `PerHostPercentileTracker` may also be used with a slow query
logger (see the [Logging](../logging/) section). In that case, you would
create a single tracker object and share it with both components.

[psep]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/PercentileSpeculativeExecutionPolicy.html
[hdr]: http://hdrhistogram.github.io/HdrHistogram/
[phpt]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/PerHostPercentileTracker.html

#### Using your own

As with all policies, you are free to provide your own by implementing
`SpeculativeExecutionPolicy`.

### How speculative executions affect retries

Turning speculative executions on doesn't change the driver's [retry](../retries/) behavior. Each
parallel execution will trigger retries independently:

```ditaa
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query host1
  |                |------------->|
  |                |              |
  |                | unavailable  |
  |                |<-------------|
  |                |
  |                |retry at lower CL
  |                |------------->|
  |                |              |
  |                |     query host2
  |                |-------------------->|
  |                |              |      |
  |                |     server error    |
  |                |<--------------------|
  |                |              |
  |                |   retry on host3
  |                |-------------------->|
  |                |              |      |
  |                | host1 replies|      |
  |                |<-------------|      |
  |   complete     |                     |
  |<---------------|                     |
  |                | cancel              |
  |                |-------------------->|
```

The only impact is that all executions of the same query always share
the same query plan, so each host will be used by at most one execution.

[retry_policy]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/RetryPolicy.html

### Tuning and practical details

The goal of speculative executions is to improve overall latency (the
time between `execute(query)` and `complete` in the diagrams above) at
high percentiles. On the flipside, they cause the driver to send more
individual requests, so throughput will not necessarily improve.

You can monitor how many speculative executions were triggered with the
`speculative-executions` metric (exposed in the Java API as
[cluster.getMetrics().getErrors().getSpeculativeExecutions()][se_metric]).
It should only be a few percents of the total number of requests
([cluster.getMetrics().getRequestsTimer().getCount()][request_metric]).

[se_metric]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/Metrics.Errors.html#getSpeculativeExecutions--
[request_metric]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/Metrics.html#getRequestsTimer--

#### Stream id exhaustion

One side-effect of speculative executions is that many requests are
cancelled, which can lead to a phenomenon called *stream id exhaustion*:
each TCP connection can handle multiple simultaneous requests,
identified by a unique number called *stream id*. When a request gets
cancelled, we can't reuse its stream id immediately because we might
still receive a response from the server later. If this happens often,
the number of available stream ids diminishes over time, and when it
goes below a given threshold we close the connection and create a new
one. If requests are often cancelled, so will see connections being
recycled at a high rate.

One way to detect this is to monitor open connections per host
([Session.getState().getOpenConnections(host)][session_state]) against
TCP connections at the OS level. If open connections stay constant but
you see many TCP connections in closing states, you might be running
into this issue. Try raising the speculative execution threshold.

This problem is more likely to happen with version 2 of the native
protocol, because each TCP connection only has 128 stream ids. With
version 3 (driver 2.1.2 or above with Cassandra 2.1 or above), there are
32K stream ids per connection, so higher cancellation rates can be
sustained. If you're unsure of which native protocol version you're
using, you can check with
[cluster.getConfiguration().getProtocolOptions().getProtocolVersion()][protocol_version].

[session_state]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/Session.State.html
[protocol_version]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/ProtocolOptions.html#getProtocolVersion--

#### Request ordering and client timestamps

Another issue that might arise is that you get unintuitive results
because of request ordering. Suppose you run the following query with
speculative executions enabled:

    insert into my_table (k, v) values (1, 1);

The first execution is a bit too slow, so a second execution gets
triggered. Finally, the first execution completes, so the client code
gets back an acknowledgement, and the second execution is cancelled.
However, cancelling only means that the driver stops waiting for the
server's response, the request could still be "on the wire"; let's
assume that this is the case.

Now you run the following query, which completes successfully:

    delete from my_table where k = 1;

But now the second execution of the first query finally reaches its
target node, which applies the mutation. The row that you've just
deleted is back!

The workaround is to use a timestamp with your queries:

    insert into my_table (k, v) values (1, 1) USING TIMESTAMP 1432764000;

If you're using native protocol v3, you can also enable [client-side
timestamps](../query_timestamps/#client-side-generation) to have this done
automatically.
