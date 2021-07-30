## Query timestamps

### Quick overview

Defines the order in which mutations are applied on the server. Ways to set it (by order of
precedence, higher priority first):

* `USING TIMESTAMP` in the query string.
* programmatically with [Statement.setQueryTimestamp()].
* timestamp generator: `advanced.timestamp-generator` in the configuration. Defaults to session-wide
  monotonic, also available: per-thread monotonic, server-side, or write your own.
* if the generator didn't set it, assigned server-side.

-----

In Cassandra, each mutation has a microsecond-precision timestamp, which is used to order operations
relative to each other.

There are various ways to assign it:

### CQL `USING TIMESTAMP`

You can explicitly provide the timestamp in your CQL query:

```java
session.execute("INSERT INTO my_table(c1, c2) values (1, 1) " +
    "USING TIMESTAMP 1432815430948040");
```

### Timestamp generator

The driver has a timestamp generator that gets invoked for every outgoing request; it either assigns
a client-side timestamp to the request, or indicates that the server should assign it. 

The timestamp generator is defined in the [configuration](../configuration/).

#### AtomicTimestampGenerator

```
datastax-java-driver.advanced.timestamp-generator {
  class = AtomicTimestampGenerator
}
```

This is the default implementation. It always generates a client timestamp, and guarantees
monotonicity (i.e. ever-increasing timestamps) across all application threads.

Note that, in order to achieve monotonicity, the generator might return timestamps that drift out in
the future. This happens if timestamps are generated at a rate of more than one per microsecond, or
more likely in the event of a system clock skew. When this happens, the generator logs a warning
message in the category `com.datastax.oss.driver.internal.core.time.MonotonicTimestampGenerator`:

```
Clock skew detected: current tick (...) was ... microseconds behind the last generated timestamp (...),
returned timestamps will be artificially incremented to guarantee monotonicity.
```

You can control that message with these options:

```
datastax-java-driver.advanced.timestamp-generator {
  drift-warning {
    # How far in the future timestamps are allowed to drift before the warning is logged.
    # If it is undefined or set to 0, warnings are disabled.
    threshold = 1 second
    # How often the warning will be logged if timestamps keep drifting above the threshold.
    interval = 10 seconds
  }
}
```

This generator strives to achieve microsecond resolution on a best-effort basis. But in practice,
the real accuracy of generated timestamps is largely dependent on the granularity of the operating
system's clock. For most systems, this minimum granularity is millisecond, and the sub-millisecond
part is simply a counter that gets incremented until the next clock tick, as provided by
`System.currentTimeMillis()`.
                                                                                          
On some systems, however, it is possible to have a better granularity by using a [JNR] call to
[gettimeofday]. This native call will be used when available, unless use of the Java clock is forced
with this configuration option: 

```
datastax-java-driver.advanced.timestamp-generator {
  force-java-clock = true
}
```

To check what the driver is currently using, turn on `INFO` logs for the category
`com.datastax.oss.driver.internal.core.time`, and look for one of the following messages at
initialization:

* `Using Java system clock because this was explicitly required in the configuration`
* `Could not access native clock (see debug logs for details), falling back to Java system clock`
* `Using native clock for microsecond precision`

#### ThreadLocalTimestampGenerator

```
datastax-java-driver.advanced.timestamp-generator {
  class = ThreadLocalTimestampGenerator
}
```

This is similar to the atomic generator, except that it only guarantees monotonicity within each
thread. In other words, if a given application thread invokes `session.execute()` multiple times,
the timestamps will be strictly increasing; but across two or more application threads, there might
be duplicates.

This is a bit more efficient, but should only be used when threads are not in direct competition for
timestamp ties (i.e., they are executing independent statements).

It uses the same configuration options `drift-warning` and`force-java-clock`; see the previous
section for details. 

#### ServerSideTimestampGenerator

```
datastax-java-driver.advanced.timestamp-generator {
  class = ServerSideTimestampGenerator
}
```

This implementation always lets the server assign a timestamp.

#### Custom

You can create your own generator by implementing [TimestampGenerator], and referencing your
implementation class from the configuration.

#### Using multiple generators

The timestamp generator can be overridden in [execution profiles](../configuration/#profiles):

```
datastax-java-driver {
  advanced.timestamp-generator.class = AtomicTimestampGenerator
  profiles {
    profile1 {
      advanced.timestamp-generator.class = ServerSideTimestampGenerator
    }
    profile2 {}
  } 
}
```

The `profile1` profile uses its own generator. The `profile2` profile inherits the default
profile's. Note that this goes beyond configuration inheritance: the driver only creates a single
`AtomicTimestampGenerator` instance and reuses it (this also occurs if two sibling profiles have the
same configuration).

Each request uses its declared profile's generator. If it doesn't declare any profile, or if the
profile doesn't have a dedicated policy, then the default profile's generator is used.

### Per-statement timestamp

Finally, you can assign a timestamp to a statement directly from application code:

```java
Statement statement =
    SimpleStatement.builder("UPDATE users SET email = 'x@y.com' where id = 1")
        .setQueryTimestamp(1432815430948040L)
        .build();
session.execute(statement);
```

### Timestamps and lightweight transactions

Client-side timestamps are prohibited for [lightweight transactions] \(used for conditional updates
such as `INSERT... IF NOT EXISTS`, `UPDATE... IF...`, etc.).

If you add a `USING TIMESTAMP` clause to such a query, the server will return an error:

```
cqlsh> UPDATE foo USING TIMESTAMP 1234 SET v=1 WHERE k=0 IF v=2;
InvalidRequest: Error from server: code=2200 [Invalid query] message="Cannot provide custom timestamp for conditional updates"
```

If you execute a conditional update through the driver with a client-side timestamp generator, the
client-side timestamp will be silently ignored and the server will provide its own.

### Summary

Here is the order of precedence of all the methods described so far:

1. if there is a `USING TIMESTAMP` clause in the CQL string, use that over anything else;
2. otherwise, if a default timestamp was set directly on the statement, use it;
3. otherwise, if the timestamp generator assigned a timestamp, use it;
4. otherwise, let the server assign the timestamp.

[TimestampGenerator]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/time/TimestampGenerator.html

[gettimeofday]: http://man7.org/linux/man-pages/man2/settimeofday.2.html
[JNR]: https://github.com/jnr/jnr-posix
[Lightweight transactions]: https://docs.datastax.com/en/dse/6.0/cql/cql/cql_using/useInsertLWT.html
[Statement.setQueryTimestamp()]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/Statement.html#setQueryTimestamp-long-
