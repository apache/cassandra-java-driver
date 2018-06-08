## Speculative query execution

Sometimes a Cassandra node might be experiencing difficulties (ex: long GC pause) and take longer
than usual to reply. Queries sent to that node will experience bad latency.

One thing we can do to improve that is pre-emptively start a second execution of the query against
another node, before the first node has replied or errored out. If that second node replies faster,
we can send the response back to the client. We also cancel the first execution (note that
"cancelling" in this context simply means discarding the response when it arrives later, Cassandra
does not support cancellation of in flight requests):

```ditaa
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query node1
  |                |------------->|
  |                |              |
  |                |              |
  |                |     query node2
  |                |-------------------->|
  |                |              |      |
  |                |              |      |
  |                |     node2 replies   |
  |                |<--------------------|
  |   complete     |              |
  |<---------------|              |
  |                | cancel       |
  |                |------------->|
```

Or the first node could reply just after the second execution was started. In this case, we cancel
the second execution. In other words, whichever node replies faster "wins" and completes the client
query:

```ditaa
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query node1
  |                |------------->|
  |                |              |
  |                |              |
  |                |     query node2
  |                |-------------------->|
  |                |              |      |
  |                |              |      |
  |                | node1 replies|      |
  |                |<-------------|      |
  |   complete     |                     |
  |<---------------|                     |
  |                | cancel              |
  |                |-------------------->|
```

Speculative executions are **disabled** by default. The following sections cover the practical
details and how to enable them.

### Query idempotence

If a query is [not idempotent](../idempotence/), the driver will never schedule speculative
executions for it, because there is no way to guarantee that only one node will apply the mutation.

### Configuration

Speculative executions are controlled by a policy defined in the [configuration](../configuration/).
The default implementation never schedules an execution:

```
datastax-java-driver.advanced.speculative-execution-policy {
  class = NoSpeculativeExecutionPolicy
}
```

The "constant" policy schedules executions at a fixed delay:

```
datastax-java-driver.advanced.speculative-execution-policy {
  class = ConstantSpeculativeExecutionPolicy
  
  # The maximum number of executions (including the initial, non-speculative execution).
  # This must be at least one.
  max-executions = 3

  # The delay between each execution. 0 is allowed, and will result in all executions being sent
  # simultaneously when the request starts.
  # Note that sub-millisecond precision is not supported, any excess precision information will
  # be dropped; in particular, delays of less than 1 millisecond are equivalent to 0.
  # This must be positive or 0.
  delay = 100 milliseconds
}
```

Given the above configuration, an idempotent query would be handled this way:

* start the initial execution at t0;
* if no response has been received at t0 + 100 milliseconds, start a speculative execution on
  another node;
* if no response has been received at t0 + 200 milliseconds, start another speculative execution on
  a third node;
* past that point, don't query other nodes, just wait for the first response to arrive.

Finally, you can create your own policy by implementing [SpeculativeExecutionPolicy], and
referencing your implementation class from the configuration.

### How speculative executions affect retries

Turning on speculative executions doesn't change the driver's [retry](../retries/) behavior. Each
parallel execution will trigger retries independently:

```ditaa
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query node1
  |                |------------->|
  |                |              |
  |                | unavailable  |
  |                |<-------------|
  |                |
  |                |retry at lower CL
  |                |------------->|
  |                |              |
  |                |     query node2
  |                |-------------------->|
  |                |              |      |
  |                |     server error    |
  |                |<--------------------|
  |                |              |
  |                |   retry on node3
  |                |-------------------->|
  |                |              |      |
  |                | node1 replies|      |
  |                |<-------------|      |
  |   complete     |                     |
  |<---------------|                     |
  |                | cancel              |
  |                |-------------------->|
```

The only impact is that all executions of the same query always share the same query plan, so each 
node will be used by at most one execution.

### Tuning and practical details

The goal of speculative executions is to improve overall latency (the time between `execute(query)`
and `complete` in the diagrams above) at high percentiles. On the flip side, too many speculative
executions increase the pressure on the cluster. 

If you use speculative executions to avoid unhealthy nodes, a good-behaving node should rarely hit
the threshold. We recommend running a benchmark on a healthy platform (all nodes up and healthy) and
monitoring the request percentiles with the `cql-requests` [metric](../metrics/). Then use the
latency at a high percentile (for example p99.9) as the threshold.

Alternatively, maybe low latency is your absolute priority, and you are willing to take the
increased throughput as a tradeoff. In that case, set the threshold to 0 and provision your cluster
accordingly. 

You can monitor the number of speculative executions triggered by each node with the
`speculative-executions` [metric](../metrics/).

#### Stream id exhaustion

One side-effect of speculative executions is that many requests get cancelled, which can lead to a
phenomenon called *stream id exhaustion*: each TCP connection can handle multiple simultaneous
requests, identified by a unique number called *stream id* (see also the [pooling](../pooling/)
section). When a request gets cancelled, we can't reuse its stream id immediately because we might
still receive a response from the server later. If this happens often, the number of available
stream ids diminishes over time, and when it goes below a given threshold we close the connection
and create a new one. If requests are often cancelled, you will see connections being recycled at a
high rate.

The best way to monitor this is to compare the `pool.orphaned-streams` [metric](../metrics/) to the
total number of available stream ids (which can be computed from the configuration:
`pool.local.size * max-requests-per-connection`). The `pool.available-streams` and `pool.in-flight`
metrics will also give you an idea of how many stream ids are left for active queries.

#### Request ordering

Note: ordering issues are only a problem with [server-side timestamps](../query_timestamps/), which
are not the default anymore in driver 4+. So unless you've explicitly enabled
`ServerSideTimestampGenerator`, you can skip this section.

Suppose you run the following query with speculative executions and server-side timestamps enabled:

    insert into my_table (k, v) values (1, 1);

The first execution is a bit too slow, so a second execution gets triggered. Finally, the first
execution completes, so the client code gets back an acknowledgement, and the second execution is
cancelled. However, cancelling only means that the driver stops waiting for the server's response,
the request could still be "on the wire"; let's assume that this is the case.

Now you run the following query, which completes successfully:

    delete from my_table where k = 1;

But now the second execution of the first query finally reaches its target node, which applies the
mutation. The row that you've just deleted is back!

The workaround is to either specify a timestamp in your CQL queries:

    insert into my_table (k, v) values (1, 1) USING TIMESTAMP 1432764000;
    
Or use a client-side [timestamp generator](../query_timestamps/).

### Using multiple policies

The speculative execution policy can be overridden in [configuration
profiles](../configuration/#profiles):

```
datastax-java-driver {
  advanced.speculative-execution-policy {
    class = ConstantSpeculativeExecutionPolicy
    max-executions = 3
    delay = 100 milliseconds
  }
  profiles {
    oltp {
      basic.request.timeout = 100 milliseconds
    }
    olap {
      basic.request.timeout = 30 seconds
      advanced.speculative-execution-policy.class = NoSpeculativeExecutionPolicy
    }
  }
}
```

The `olap` profile uses its own policy. The `oltp` profile inherits the default profile's. Note that
this goes beyond configuration inheritance: the driver only creates a single
`ConstantSpeculativeExecutionPolicy` instance and reuses it (this also occurs if two sibling
profiles have the same configuration).

Each request uses its declared profile's policy. If it doesn't declare any profile, or if the
profile doesn't have a dedicated policy, then the default profile's policy is used.

[SpeculativeExecutionPolicy]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/specex/SpeculativeExecutionPolicy.html