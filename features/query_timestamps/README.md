## Query timestamps

In Cassandra, each mutation has a microsecond-precision timestamp, which
is used to order operations relative to each other.

There are various ways to assign it:

### Server-side generation

If the client does not specify any timestamp, the server will assign one
based on the time it receives the query.

This can be a problem when the order of the writes matter: with unlucky
timing (different coordinators, network latency, etc.), two successive
requests from the same client might be processed in a different order
server-side, and end up with out-of-order timestamps.

### CQL `USING TIMESTAMP`

You can explicitly provide the timestamp in your CQL query:

```java
session.execute("INSERT INTO my_table(c1, c2) values (1, 1) " +
    "USING TIMESTAMP 1432815430948040");
```

This solves the problems of server-side generation: the client has
control over the ordering of the statements it generates. On the other
hand, it puts the burden of generating timestamps on client code.

### Client-side generation

Starting with version 2.1.2 of the driver, and if [native
protocol](../native_protocol/) v3 or above is in use, a *default
timestamp* can be sent with each query.

To enable this feature, provide an instance of [TimestampGenerator][tsg]
at initialization:

```java
Cluster.builder().addContactPoint("127.0.0.1")
    .withTimestampGenerator(new AtomicMonotonicTimestampGenerator())
    .build();
```

The default is still server-side generation. So unless you explicitly
provide a generator, you get the same behavior as previous driver
versions.

In addition, you can also override the default timestamp on a
per-statement basis:

```java
Statement statement = new SimpleStatement(
    "UPDATE users SET email = 'x@y.com' where id = 1");
statement.setDefaultTimestamp(1234567890);
session.execute(statement);
```

[tsg]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TimestampGenerator.html

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
