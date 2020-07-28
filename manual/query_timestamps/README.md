## Query timestamps

In Cassandra, each mutation has a microsecond-precision timestamp, which
is used to order operations relative to each other.

There are various ways to assign it:

### CQL `USING TIMESTAMP`

You can explicitly provide the timestamp in your CQL query:

```java
session.execute("INSERT INTO my_table(c1, c2) values (1, 1) " +
    "USING TIMESTAMP 1432815430948040");
```

### Client-side generation

This is enabled by default if you're using the driver 3.0+ and a version
of Cassandra that supports [native protocol](../native_protocol) v3 or
above.

The driver will use a [TimestampGenerator] to generate a timestamp for each query, and send it
to the server as part of the request.

You can configure the generator at initialization:

```java
Cluster.builder().addContactPoint("127.0.0.1")
    .withTimestampGenerator(new SomeTimestampGeneratorImpl())
    .build();
```

Two generator implementations are provided out of the box:

1. [AtomicMonotonicTimestampGenerator], which guarantees monotonicity of timestamps for all 
   threads. This is the default implementation (i.e. what will be used if you don't call 
   `withTimestampGenerator`);
2. [ThreadLocalMonotonicTimestampGenerator], which guarantees per-thread monotonicity of timestamps.

There is less contention using `ThreadLocalMonotonicTimestampGenerator`, but beware
that there is a risk of timestamp collision with this generator when accessed by more than one
thread; only use it when threads are not in direct competition for timestamp ties (i.e., they are executing
independent statements).

#### Accuracy

Both implementations strive to achieve microsecond resolution on a best-effort basis.
But in practice, the real accuracy of generated timestamps is largely dependent on the
granularity of the underlying operating system's clock.

For most systems, this minimum granularity is millisecond, and
the sub-millisecond part of generated timestamps is simply a counter that gets incremented
until the next clock tick, as provided by `System.currentTimeMillis()`.

On some systems, however, it is possible to have a better granularity by using a [JNR]
call to [gettimeofday]. This native call will be used when available, unless the system
property `com.datastax.driver.USE_NATIVE_CLOCK` is explicitly set to `false`.

To check what's available on your system:

* make sure your `Cluster` uses a `TimestampGenerator`;
* [configure your logging framework](../logging/) to use level `INFO` for the category
  `com.datastax.driver.core.ClockFactory`;
* look for one of the following messages at startup:

    ```
    INFO  com.datastax.driver.core.ClockFactory - Using java.lang.System clock to generate timestamps
    INFO  com.datastax.driver.core.ClockFactory - Using native clock to generate timestamps
    ```

#### Monotonicity

The aforementioned implementations also guarantee
that returned timestamps will always be monotonically increasing, even if multiple updates
happen under the same millisecond.

Note that to guarantee such monotonicity, if more than one timestamp is generated
within the same microsecond, or in the event of a system clock skew, _both implementations might
return timestamps that drift out in the future_.

When this happens, the built-in generators log a periodic warning message in the category
`com.datastax.driver.core.TimestampGenerator`. See their non-default constructors for ways to control the warning
interval.


#### Overriding the timestamp manually

You can override the generator and provide and explicit timestamp for a given query: 

```java
Statement statement = new SimpleStatement(
    "UPDATE users SET email = 'x@y.com' where id = 1");
statement.setDefaultTimestamp(1234567890);
session.execute(statement);
```


### Server-side generation

This is the "legacy" behavior if you're connected to a Cassandra version
that only supports protocol v2 or below. The server will assign a
timestamp based on the time it received the query.

This can be a problem when the order of the writes matter: with unlucky
timing (different coordinators, network latency, etc.), two successive
requests from the same client might be processed in a different order
server-side, and end up with out-of-order timestamps. This is why client-side
generation is the default starting with protocol v3. With older versions, the
only way to get client timestamps is to add a `USING TIMESTAMP` clause to your
CQL queries.

Note that, although client-side timestamps are recommended when available, you can still opt for 
server-side timestamps with protocol v3 or above, using [ServerSideTimestampGenerator]:

```java
Cluster.builder().addContactPoint("127.0.0.1")
    .withTimestampGenerator(ServerSideTimestampGenerator.INSTANCE)
    .build();
```

Server-side generation might be more suitable for situations where it is more difficult to keep
time synchronized across all clients.


### Summary

As shown in the previous sections, there are multiple ways to provide a
timestamp, some of which overlap. The order of precedence is the
following:

1. if there is a `USING TIMESTAMP` clause in the CQL string, use that
   over anything else;
2. otherwise, if a default timestamp was set on the statement and is
   different from `Long.MIN_VALUE`, use it;
3. otherwise, if a generator is specified, invoke it and use its result
   if it is different from `Long.MIN_VALUE`;
4. otherwise, let the server assign the timestamp.

Steps 2 and 3 only apply if native protocol v3 or above is in use.


[TimestampGenerator]:                     https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/TimestampGenerator.html
[AtomicMonotonicTimestampGenerator]:      https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/AtomicMonotonicTimestampGenerator.html
[ThreadLocalMonotonicTimestampGenerator]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/ThreadLocalMonotonicTimestampGenerator.html
[ServerSideTimestampGenerator]:           https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/ServerSideTimestampGenerator.html

[gettimeofday]: http://man7.org/linux/man-pages/man2/settimeofday.2.html
[JNR]: https://github.com/jnr/jnr-ffi
