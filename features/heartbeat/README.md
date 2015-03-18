## Connection heartbeat

If connections stay idle for too long, they might be dropped by
intermediate network devices (routers, firewalls...). Normally, TCP
keepalive should take care of this; but tweaking low-level keepalive
settings might be impractical in some environments.

The driver provides application-side keepalive in the form of a
connection heartbeat: when a connection has been idle for a given amount
of time, the driver will simulate activity by writing a dummy request to
it.

This feature is enabled by default. The default heartbeat interval is 30
seconds, it can be customized through `PoolingOptions`:

    cluster.getConfiguration().getPoolingOptions()
        .setHeartbeatIntervalSeconds(60);

This can be changed at runtime, but only connections created after that
will use the new interval. Most users will want to do this at startup:

    Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withPoolingOptions(new PoolingOptions()
            .setHeartbeatIntervalSeconds(60))
        .build();

The heartbeat interval should be set higher than
[SocketOptions.readTimeoutMillis](http://www.datastax.com/drivers/java/2.1/com/datastax/driver/core/SocketOptions.html#getReadTimeoutMillis()):
the read timeout is the maximum time that the driver waits for a regular
query to complete, therefore the connection should not be considered
idle before it has elapsed.

To disable heartbeat, set the interval to 0.

Implementation note: the dummy request sent by heartbeat is an
[OPTIONS](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v3.spec#L278)
message.
