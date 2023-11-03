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

## Connection pooling

### Quick overview

One connection pool per node. **Many concurrent requests** per connection (don't tune like a JDBC
pool).

* `advanced.connection` in the configuration: `max-requests-per-connection`, `pool.local.size`,
  `pool.remote.size`.
* metrics (per node): `pool.open-connections`, `pool.in-flight`, `pool.available-streams`,
  `pool.orphaned-streams`.
* heartbeat: driver-level keepalive, prevents idle connections from being dropped;
  `advanced.heartbeat` in the configuration. 

-----

### Basics

The driver communicates with Cassandra over TCP, using the Cassandra binary protocol. This protocol
is asynchronous, which allows each TCP connection to handle multiple simultaneous requests:

* when a query gets executed, a *stream id* gets assigned to it. It is a unique identifier on the
  current connection;
* the driver writes a request containing the stream id and the query on the connection, and then
  proceeds without waiting for the response (if you're using the asynchronous API, this is when the
  driver will send you back a `java.util.concurrent.CompletionStage`). Once the request has been
  written to the connection, we say that it is *in flight*;
* at some point, Cassandra will send back a response on the connection. This response also contains
  the stream id, which allows the driver to trigger a callback that will complete the corresponding
  query (this is the point where your `CompletionStage` will get completed).

You don't need to manage connections yourself. You simply interact with a [CqlSession] object, which
takes care of it.

**For a given session, there is one connection pool per connected node** (a node is connected when
it is up and not ignored by the [load balancing policy](../load_balancing/)).

The number of connections per pool is configurable (this will be described in the next section).
There are up to 32768 stream ids per connection.

```ditaa
+-------+1     n+----+1     n+----------+1   32K+-------+
+Session+-------+Pool+-------+Connection+-------+Request+
+-------+       +----+       +----------+       +-------+
```

### Configuration

Pool sizes are defined in the `connection` section of the [configuration](../configuration/). Here
are the relevant options with their default values:

```
datastax-java-driver.advanced.connection {
  max-requests-per-connection = 1024
  pool {
    local.size = 1
    remote.size = 1
  }
}
```

Do not change those values unless informed by concrete performance measurements; see the
[Tuning](#tuning) section at the end of this page.

Unlike previous versions of the driver, pools do not resize dynamically. However you can adjust the
options at runtime, the driver will detect and apply the changes.

#### Heartbeat

If connections stay idle for too long, they might be dropped by intermediate network devices
(routers, firewalls...). Normally, TCP keepalive should take care of this; but tweaking low-level
keepalive settings might be impractical in some environments.

The driver provides application-side keepalive in the form of a connection heartbeat: when a
connection does not receive incoming reads for a given amount of time, the driver will simulate
activity by writing a dummy request to it. If that request fails, the connection is trashed and
replaced.

This feature is enabled by default. Here are the default values in the configuration:

```
datastax-java-driver.advanced.heartbeat {
  interval = 30 seconds

  # How long the driver waits for the response to a heartbeat. If this timeout fires, the heartbeat
  # is considered failed.
  timeout = 500 milliseconds
}
```

Both options can be changed at runtime, the new value will be used for new connections created after
the change.

### Monitoring

The driver exposes node-level [metrics](../metrics/) to monitor your pools (note that all metrics
are disabled by default, you'll need to change your configuration to enable them):

```
datastax-java-driver {
  advanced.metrics.node.enabled = [
    # The number of connections open to this node for regular requests (exposed as a
    # Gauge<Integer>).
    #
    # This includes the control connection (which uses at most one extra connection to a random
    # node in the cluster).
    pool.open-connections,
    
    # The number of stream ids available on the connections to this node (exposed as a
    # Gauge<Integer>).
    #
    # Stream ids are used to multiplex requests on each connection, so this is an indication of
    # how many more requests the node could handle concurrently before becoming saturated (note
    # that this is a driver-side only consideration, there might be other limitations on the
    # server that prevent reaching that theoretical limit).
    pool.available-streams,
    
    # The number of requests currently executing on the connections to this node (exposed as a
    # Gauge<Integer>). This includes orphaned streams.
    pool.in-flight,
    
    # The number of "orphaned" stream ids on the connections to this node (exposed as a
    # Gauge<Integer>).
    #
    # See the description of the connection.max-orphan-requests option for more details.
    pool.orphaned-streams,
  ]
}
```

In particular, it's a good idea to keep an eye on those two metrics:

* `pool.open-connections`: if this doesn't match your configured pool size, something is preventing
  connections from opening (either configuration or network issues, or a server-side limitation --
  see [CASSANDRA-8086]);
* `pool.available-streams`: if this is often close to 0, it's a sign that the pool is getting
  saturated. Consider adding more connections per node.

### Tuning

The driver defaults should be good for most scenarios.

#### Number of requests per connection

In our experience, raising `max-requests-per-connection` above 1024 does not bring any significant
improvement: the server is only going to service so many requests at a time anyway, so additional
requests are just going to pile up.

Lowering the value is not a good idea either. If your goal is to limit the global throughput of the
driver, a [throttler](../throttling) is a better solution.

#### Number of connections per node 

1 connection per node (`pool.local.size` or `pool.remote.size`) is generally sufficient. However, it
might become a bottleneck in very high performance scenarios: all I/O for a connection happens on
the same thread, so it's possible for that thread to max out its CPU core. In our benchmarks, this
happened with a single-node cluster and a high throughput (approximately 80K requests / second /
connection).

It's unlikely that you'll run into this issue: in most real-world deployments, the driver connects
to more than one node, so the load will spread across more I/O threads. However if you suspect that
you experience the issue, here's what to look out for:

* the driver throughput plateaus but the process does not appear to max out any system resource (in
  particular, overall CPU usage is well below 100%);
* one of the driver's I/O threads maxes out its CPU core. You can see that with a profiler, or
  OS-level tools like `pidstat -tu` on Linux. By default, I/O threads are named
  `<session_name>-io-<n>`.

Try adding more connections per node. Thanks to the driver's hot-reload mechanism, you can do that
at runtime and see the effects immediately.

[CqlSession]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/CqlSession.html
[CASSANDRA-8086]: https://issues.apache.org/jira/browse/CASSANDRA-8086
