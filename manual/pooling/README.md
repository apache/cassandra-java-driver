## Connection pooling

### Basics

The driver communicates with Cassandra over TCP, using the Cassandra
binary protocol. This protocol is asynchronous, which allows each TCP
connection to handle multiple simultaneous requests:

* when a query gets executed, a *stream id* gets assigned to it. It is a
  unique identifier on the current connection;
* the driver writes a request containing the stream id and the query on
  the connection, and then proceeds without waiting for the response (if
  you're using the asynchronous API, this is when the driver will send you
  back a [ResultSetFuture][result_set_future]).
  Once the request has been written to the
  connection, we say that it is *in flight*;
* at some point, Cassandra will send back a response on the connection.
  This response also contains the stream id, which allows the driver to
  trigger a callback that will complete the corresponding query (this is
  the point where your `ResultSetFuture` will get completed).

You don't need to manage connections yourself. You simply interact with a `Session` object, which takes care of it.

**For each `Session`, there is one connection pool per connected host** (a host is connected when it is up and
not ignored by the [load balancing policy](../load_balancing)).

The number of connections per pool is configurable (this will be
described in the next section).  The number of stream ids depends on the
[native protocol version](../native_protocol/):

* protocol v2 or below: 128 stream ids per connection.
* protocol v3 or above: up to 32768 stream ids per connection.

```ditaa
+-------+1   n+-------+1   n+----+1   n+----------+1   128/32K+-------+
|Cluster+-----+Session+-----+Pool+-----+Connection+-----------+Request+
+-------+     +-------+     +----+     +----------+           +-------+
```

If there are several connections in pool, driver evenly spreads new requests between connections.

### Configuring the connection pool

Connections pools are configured with a [PoolingOptions][pooling_options] object, which 
is global to a `Cluster` instance. You can pass that object when
building the cluster:

```java
PoolingOptions poolingOptions = new PoolingOptions();
// customize options...

Cluster cluster = Cluster.builder()
    .withContactPoints("127.0.0.1")
    .withPoolingOptions(poolingOptions)
    .build();
```

Most options can also be changed at runtime. If you don't have a
reference to the `PoolingOptions` instance, here's how you can get it:

```java
PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();
// customize options...
```

#### Pool size

Connection pools have a variable size, which gets adjusted automatically
depending on the current load. There will always be at least a *core*
number of connections, and at most a *max* number. These values can be
configured independently by host *distance* (the distance is determined
by your [LoadBalancingPolicy][lbp], and will generally indicate whether a
host is in the same datacenter or not).

```java
poolingOptions
    .setCoreConnectionsPerHost(HostDistance.LOCAL,  4)
    .setMaxConnectionsPerHost( HostDistance.LOCAL, 10)
    .setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
    .setMaxConnectionsPerHost( HostDistance.REMOTE, 4);
```

For convenience, core and max can be set simultaneously:

```java
poolingOptions
    .setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
    .setConnectionsPerHost(HostDistance.REMOTE, 2, 4);
```

The default settings are:

* protocol v2:
  * `LOCAL` hosts: core = 2, max = 8
  * `REMOTE` hosts: core = 1, max = 2
* protocol v3:
  * `LOCAL` hosts: core = max = 1
  * `REMOTE` hosts: core = max = 1

[PoolingOptions.setNewConnectionThreshold][nct] determines the threshold
that triggers the creation of a new connection when the pool is not at
its maximum capacity. In general, you shouldn't need to change its
default value.

#### Dynamic resizing

If core != max, the pool will resize automatically to adjust to the
current activity on the host.

When activity goes up and there are *n* connections with n < max, the driver
will add a connection when the number of concurrent requests is more than
(n - 1) * 128 + [PoolingOptions.setNewConnectionThreshold][nct]
(in layman's terms, when all but the last connection are full and the last
connection is above the threshold).

When activity goes down, the driver will "trash" connections if the maximum
number of requests in a 10 second time period can be satisfied by less than
the number of connections opened. Trashed connections are kept open but do
not accept new requests. After a given timeout (defined by
[PoolingOptions.setIdleTimeoutSeconds][sits]), trashed connections are closed
and removed. If during that idle period activity increases again, those
connections will be resurrected back into the active pool and reused. The
main intent of that is to not constantly recreate connections if activity
changes quickly over an interval.

#### Simultaneous requests per connection

[PoolingOptions.setMaxRequestsPerConnection][mrpc] allows you to
throttle the number of concurrent requests per connection.

With protocol v2, there is no reason to throttle. It is set to 128 (the
max) and you should not change it.

With protocol v3, it is set to 1024 for `LOCAL`  hosts, and 256 for
`REMOTE` hosts. These low defaults were chosen so that the default
configuration for protocol v2 and v3 allow the same total number of
simultaneous requests (to avoid bad surprises when clients migrate from
v2 to v3). You can raise this threshold, or even set it to the max:

```java
poolingOptions
    .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
    .setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);
```

Just keep in mind that high values will give clients more bandwidth and
therefore put more pressure on your cluster. This might require some
tuning, especially if you have many clients.

#### Heartbeat

If connections stay idle for too long, they might be dropped by
intermediate network devices (routers, firewalls...). Normally, TCP
keepalive should take care of this; but tweaking low-level keepalive
settings might be impractical in some environments.

The driver provides application-side keepalive in the form of a
connection heartbeat: when a connection has been idle for a given amount
of time, the driver will simulate activity by writing a dummy request to
it.

This feature is enabled by default. The default heartbeat interval is 30
seconds, it can be customized with the following method:

```java
poolingOptions.setHeartbeatIntervalSeconds(60);
```

If it gets changed at runtime, only connections created after that will
use the new interval. Most users will want to do this at startup.

The heartbeat interval should be set higher than
[SocketOptions.readTimeoutMillis][rtm]:
the read timeout is the maximum time that the driver waits for a regular
query to complete, therefore the connection should not be considered
idle before it has elapsed.

To disable heartbeat, set the interval to 0.

Implementation note: the dummy request sent by heartbeat is an
[OPTIONS](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v3.spec#L278)
message.


#### Acquisition queue

When the driver tries to send a request to a host, it will first try to
acquire a connection from this host's pool. If the pool is busy (i.e.
all connections are already handling their maximum number of in flight
requests), the acquisition attempt gets enqueued until a connection becomes
available again.

Two options control that queue: a maximum size ([PoolingOptions.setMaxQueueSize][smqs]) and a timeout 
([PoolingOptions.setPoolTimeoutMillis][sptm]).

* if either option is set to zero, the attempt is rejected immediately;
* else if more than `maxQueueSize` requests are already waiting for a connection, the attempt is also rejected;
* otherwise, the attempt is enqueued; if a connection becomes available before `poolTimeoutMillis` has elapsed,
  then the attempt succeeds, otherwise it is rejected.

If the attempt is rejected, the driver will move to the next host in the [query plan](../load_balancing/#query-plan),
and try to acquire a connection again.

If all hosts are busy with a full queue, the request will fail with a
[NoHostAvailableException][nhae]. If you inspect the map returns by this
exception's [getErrors] method, you will see a [BusyPoolException] for
each host.


### Monitoring and tuning the pool

The easiest way to monitor pool usage is with [Session.getState][get_state]. Here's
a simple example that will print the number of open connections, active
requests, and maximum capacity for each host, every 5 seconds:

```java
final LoadBalancingPolicy loadBalancingPolicy =
    cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
final PoolingOptions poolingOptions =
cluster.getConfiguration().getPoolingOptions();

ScheduledExecutorService scheduled =
Executors.newScheduledThreadPool(1);
scheduled.scheduleAtFixedRate(new Runnable() {
    @Override
    public void run() {
        Session.State state = session.getState();
        for (Host host : state.getConnectedHosts()) {
            HostDistance distance = loadBalancingPolicy.distance(host);
            int connections = state.getOpenConnections(host);
            int inFlightQueries = state.getInFlightQueries(host);
            System.out.printf("%s connections=%d, current load=%d, max
load=%d%n",
                host, connections, inFlightQueries,
                connections *
poolingOptions.getMaxRequestsPerConnection(distance));
        }
    }
}, 5, 5, TimeUnit.SECONDS);
```

In real life, you'll probably want something more sophisticated, like
exposing a JMX MBean or sending the data to your favorite monitoring
tool.

If you find that the current load stays close or equal to the maximum
load at all time, it's a sign that your connection pools are saturated
and you should raise the max connections per host, or max requests per
connection (protocol v3).

If you're using protocol v2 and the load is often less than core * 128,
your pools are underused and you could get away with less core
connections.

#### Tuning protocol v3 for very high throughputs

As mentioned above, the default pool size for protocol v3 is core = max
= 1. This means all requests to a given node will share a single
connection, and therefore a single Netty I/O thread.

There is a corner case where this I/O thread can max out its CPU core
and become a bottleneck in the driver; in our benchmarks, this happened
with a single-node cluster and a high throughput (approximately 80K
requests / second).

It's unlikely that you'll run into this issue: in most real-world
deployments, the driver connects to more than one node, so the load will
spread across more I/O threads. However if you suspect that you
experience the issue, here's what to look out for:

* the driver throughput plateaus but the process does not appear to
  max out any system resource (in particular, overall CPU usage is well
  below 100%);
* one of the driver's I/O threads maxes out its CPU core. You can see
  that with a profiler, or OS-level tools like `pidstat -tu` on Linux.
  I/O threads are called `<cluster_name>-nio-worker-<n>`, unless you're
  injecting your own `EventLoopGroup` with `NettyOptions`.

The solution is to add more connections per node. To ensure that
additional connections get created before you run into the bottleneck,
either:

* set core = max;
* keep core = 1, but adjust [maxRequestsPerConnection][mrpc] and
  [newConnectionThreshold][nct] so that enough connections are added by
  the time you reach the bottleneck.

[result_set_future]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/ResultSetFuture.html
[pooling_options]:   https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/PoolingOptions.html
[lbp]:               https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/policies/LoadBalancingPolicy.html
[nct]:               https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/PoolingOptions.html#setNewConnectionThreshold-com.datastax.driver.core.HostDistance-int-
[mrpc]:              https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/PoolingOptions.html#setMaxRequestsPerConnection-com.datastax.driver.core.HostDistance-int-
[sits]:              https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/PoolingOptions.html#setIdleTimeoutSeconds-int-
[rtm]:               https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/SocketOptions.html#getReadTimeoutMillis--
[smqs]:              https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/PoolingOptions.html#setMaxQueueSize-int-
[sptm]:              https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/PoolingOptions.html#setPoolTimeoutMillis-int-
[nhae]:              https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/exceptions/NoHostAvailableException.html
[getErrors]:         https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/exceptions/NoHostAvailableException.html#getErrors--
[get_state]:         https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/Session.html#getState--
[BusyPoolException]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/exceptions/BusyPoolException.html
