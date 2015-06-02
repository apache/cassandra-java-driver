## Connection pooling

### Basics

The driver communicates with Cassandra over TCP, using the Cassandra
binary protocol. This protocol is asynchronous, which allows each TCP
connection to handle multiple simultaneous requests:

* when a query gets executed, a *stream id* gets assigned to it. It is a
  unique identifier for the current connection;
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

To increase the number of concurrent requests, we use a pool of
connections to each host.  **For each `Session` object, there is one
connection pool per connected host**.

The number of connections and stream ids depends on the [native protocol
version](../native_protocol/):

* **protocol v2 or below**: there are **128 stream ids per connection.**
  The number of connections per pool is configurable (this will be
  described in the next section).
* v3 or above: there are **up to 32768 stream ids per connection**, and
  **1 connection per pool**.

```ditaa
+-------+1   n+-------+1   n+----+1   n/1+----------+1   128/32K+-------+
|Cluster+-----+Session+-----+Pool+-------+Connection+-----------+Request+
+-------+     +-------+     +----+       +----------+           +-------+
```

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

#### Pool size (protocol v2 only)

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

Note: the setters always enforce core <= max, which can get annoying
when you try to set both values at once.
[JAVA-662](https://datastax-oss.atlassian.net/browse/JAVA-662) will
address this issue.

[PoolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold][msrpct]
has a slightly misleading name: it does not limit the number of requests per
connection, but only determines the threshold that triggers the creation
of a new connection when the pool is not at its maximum capacity. In
general, you shouldn't have to change its default value.

#### Number of simultaneous requests per host (protocol v3 only)

Protocol v3 uses a single connection per host. While the number of
simultaneous requests can go up to 32768, it is limited by
[PoolingOptions.setMaxSimultaneousRequestsPerHostThreshold][msrpht]. The
goal of this option is to throttle the load that each client can
generate on the server. By default, it is set to 1024 for local hosts,
and 256 for remote hosts (this was chosen to match the capacity of a v2
pool with the default configuration).

If your performance tests show that your cluster can handle a higher
load, you can raise this threshold:

```java
poolingOptions
    .setMaxSimultaneousRequestsPerHostThreshold(HostDistance.LOCAL, 20000)
    .setMaxSimultaneousRequestsPerHostThreshold(HostDistance.REMOTE, 1000);
```

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


#### Acquisition timeout

When the driver tries to send a request to a host, it will first try to
acquire a connection from this host's pool. If the pool is busy (i.e.
all connections are already handling their maximum number of in flight
requests), the client thread will block for a while, until a connection
becomes available (note that this will block even if you're using the
asynchronous API, like [Session.executeAsync][exec_async]).

The time that the driver blocks is controlled by
[PoolingOptions.setPoolTimeoutMillis][ptm]. If there is still no connection
available after this timeout, the driver will try the next host.

For some applications, blocking is not acceptable, and it is preferable
to fail fast if the request cannot be fulfilled. If that's your case,
set the pool timeout to 0. If all hosts are busy, you will get a
[NoHostAvailableException][nhae] (if you look at the exception's details, you
will see a `java.util.concurrent.TimeoutException` for each host).


### Monitoring and tuning the pool

The easiest way to monitor pool usage is with [Session.getState][get_state]. Here's
a simple example that will print the number of open connections, active
requests, and maximum capacity for each host, every 5 seconds:

```java
final LoadBalancingPolicy loadBalancingPolicy =
    cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();
ProtocolOptions protocolOptions = cluster.getConfiguration().getProtocolOptions();

final Map<HostDistance, Integer> requestsPerConnection;
ProtocolVersion protocolVersion = protocolOptions.getProtocolVersionEnum();
if (protocolVersion == ProtocolVersion.V3) {
    requestsPerConnection = ImmutableMap.of(
        HostDistance.LOCAL,
        poolingOptions.getMaxSimultaneousRequestsPerHostThreshold(HostDistance.LOCAL),
        HostDistance.REMOTE,
        poolingOptions.getMaxSimultaneousRequestsPerHostThreshold(HostDistance.REMOTE));
} else {
    requestsPerConnection = ImmutableMap.of(
        HostDistance.LOCAL, 128,
        HostDistance.REMOTE, 128);
}

ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
scheduled.scheduleAtFixedRate(new Runnable() {
    @Override
    public void run() {
        Session.State state = session.getState();
        for (Host host : state.getConnectedHosts()) {
            HostDistance distance = loadBalancingPolicy.distance(host);
            int connections = state.getOpenConnections(host);
            int inFlightQueries = state.getInFlightQueries(host);
            System.out.printf("%s connections=%d current load=%d max load=%d%n",
                host, connections, inFlightQueries,
                connections * requestsPerConnection.get(distance));
        }
    }
}, 5, 5, TimeUnit.SECONDS);
```

In real life, you'll probably want something more sophisticated, like
exposing a JMX MBean or sending the data to your favorite monitoring
tool.

If you find that the current load stays close or equal to the maximum
load at all time, it's a sign that your connection pools are saturated
and you should raise the max connections per host (protocol v2) or max
requests per host (protocol v3).

If you're using protocol v2 and the load is often less than core * 128,
your pools are underused and you could get away with less core
connections.

[result_set_future]:http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/ResultSetFuture.html
[pooling_options]:http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/PoolingOptions.html
[lbp]:http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/policies/LoadBalancingPolicy.html
[msrpct]:http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/PoolingOptions.html#setMaxSimultaneousRequestsPerConnectionThreshold(com.datastax.driver.core.HostDistance,%20int)
[msrpht]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/PoolingOptions.html#setMaxSimultaneousRequestsPerHostThreshold(com.datastax.driver.core.HostDistance,%20int)
[rtm]:http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/SocketOptions.html#getReadTimeoutMillis()
[exec_async]:http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Session.html#executeAsync(com.datastax.driver.core.Statement)
[ptm]:http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/PoolingOptions.html#setPoolTimeoutMillis(int)
[nhae]:http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/exceptions/NoHostAvailableException.html
[get_state]:http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Session.html#getState()
