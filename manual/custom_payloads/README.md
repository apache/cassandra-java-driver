## Custom Payloads

The [native protocol](../native_protocol/) version 4 introduces a new feature called [Custom Payloads][CASSANDRA-8553].

According to the [protocol V4 specification][v4spec], custom payloads are generic key-value maps
where keys are strings and each value is an arbitrary sequence of bytes. Currently payloads 
can be sent by clients along with all `QUERY`, `PREPARE`, `EXECUTE` and `BATCH` requests.

Custom payloads can be used to convey user-specific information from clients to servers and vice versa.
_Please note that this is an advanced feature that requires specific server-side configuration (see below)._

### Enabling custom payloads on C* nodes

What the server is supposed to do with a payload depends on the [QueryHandler][qh] used server-side
to decode requests. Unsurprisingly, the default `QueryHandler` implementation used by Cassandra 
simply ignores all payloads. However, users can replace it with their own implementation,
thus being able to to process user-specific payloads; the payload format as well as
the serialization and deserialization logic is entirely left for clients to implement.
Needless to say, the same encoding and decoding logic should be applied on both client
and server endpoints.

This section is a small how-to to help driver users getting started with 
custom payload processing server-side. For more detailed information, 
please refer to the Cassandra documentation itself.

To enable a custom `QueryHandler` for a Cassandra node, its JVM should be
started with the system property `cassandra.custom_query_handler_class` set
to a fully qualified name of a class implementing `org.apache.cassandra.cql3.QueryHandler`.
This class should be of course available on the node's classpath.

This can be achieved with the following steps:

1. Add your implementation jar to the `CLASSPATH` environment variable;
2. Add the following to `$CASSANDRA_CONF/cassandra-env.sh`:

```
JVM_EXTRA_OPTS="-Dcassandra.custom_query_handler_class=fully.qualified.name.of.MyQueryHandler"
```

Of course, all nodes in the cluster *must* be started with the same QueryHandler, otherwise
payloads sent by the driver could get lost.

### Implementation Notes

Payloads in the Java driver are represented as `Map<String,ByteBuffer>` instances.
It is safe to use any `Map` implementation, including unsynchronized implementations 
such as `java.util.HashMap`; the driver will create defensive, thread-safe copies of
user-supplied maps. However, `ByteBuffer` instances are inherently mutable,
so callers should take care not to modify the `ByteBuffer` values in a payload map *after* submitting it 
to the driver as it could lead to unexpected results.

#### Null values

Note that, for thread safety reasons, the Java driver does not permit `null` keys nor `null` values in a payload map; 
including a `null` in your payload will result in a `NullPointerException` being immediately thrown.

However, the protocol specification *does* allow `null` values. If you need to include
a `null` value in your payload map, this can be achieved with the Java driver
by using the special value `Statement.NULL_PAYLOAD_VALUE`.

##### Payload length limitations

1. Payload maps cannot have more than 65,535 entries.
2. Payload map values are limited to a maximum length of `Integer.MAX_VALUE` (2<sup><small>31</small></sup> − 1) bytes each.

### Sending custom payloads

A custom payload can be attached to any `Statement` via `Statement.setOutgoingPayload()`.

Once the payload is set, you can either prepare the statement or execute it; 
in both cases, the payload will be sent along with the `PREPARE` or `QUERY` request respectively:

```java
Statement statement = new SimpleStatement("SELECT foo FROM bar WHERE qix = 1");
statement.setOutgoingPayload(myCustomPayload);
session.execute(statement); // myCustomPayload will be sent with QUERY request
session.prepare(statement); // myCustomPayload will be sent with PREPARE request
```

Naturally, you can also set a payload when using the `QueryBuilder` API:

```java
Statement statement = QueryBuilder.select("c2").from("t1").where(eq("c1", 1)).setOutgoingPayload(myCustomPayload);
session.execute(statement); // myCustomPayload will be sent with EXECUTE request
```

When sending payloads with batches, please note that payloads must be attached to the 
`BatchStatement` itself, *not* to internal statements:

```java
Statement statement = new SimpleStatement("INSERT INTO t1 (c1, c2) values ('foo', 'bar')")
// correct
Statement batch = new BatchStatement().add(statement);
batch.setOutgoingPayload(myCustomPayload);
session.execute(batch);  // myCustomPayload will be sent with BATCH request
// wrong
statement.setOutgoingPayload(myCustomPayload);
Statement batch = new BatchStatement().add(statement);
session.execute(batch); // myCustomPayload will NOT be sent with BATCH request
```

One important note about paging requests: if your query needs to paginate,
any outgoing payload set on the executed statement *will be transparently 
sent over and over for every new page request*.

```java
Statement statement = new SimpleStatement("...");
statement.setOutgoingPayload(myCustomPayload);
ResultSet rows = session.execute(...); // myCustomPayload will be sent with first QUERY request
for (Row row : rows) {
    // if additional QUERY requests are sent, all of them will send the same payload
}
```

And finally, note that custom payloads can only be used with protocol versions >= 4; 
trying to set a payload under lower protocol versions will result in 
an [UnsupportedFeatureException][ufe] (wrapped in a [NoHostAvailableException][nhae])
when the request is encoded.

If you want to defensively protect your code against these errors, you can either:

1) Force the protocol version when creating your `Cluster` instance:

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("...")
    .withProtocolVersion(ProtocolVersion.V4)
    .build();
```

2) Inspect the current negotiated protocol version, and only send payloads if version is equal to or
greater than 4:

```java
ProtocolVersion myCurrentVersion = cluster.getConfiguration()
    .getProtocolOptions()
    .getProtocolVersion();
if (myCurrentVersion.compareTo(ProtocolVersion.V4) >= 0) {
    // only send custom payloads if current protocol version supports it
    statement.setOutgoingPayload(myCustomPayload);
}
```

### Receiving custom payloads
 
Custom payloads sent back by the server can be retrieved in the following ways:

1) From a `ResultSet`: use the `ExecutionInfo` class to retrieve the payload sent by the server.

```java
Statement statement = new SimpleStatement("...");
ResultSet rows = session.execute(...);
// last payload sent by the server
Map<String,ByteBuffer> serverPayload = rows.getExecutionInfo().getIncomingPayload();
```

If your query required pagination (multiple `QUERY` requests),
the above method would only return the last payload sent by server
with its last `RESULT` response. If you need to retrieve all the 
payloads for all `RESULT` responses, use the following method instead:

```java
Statement statement = new SimpleStatement("...");
ResultSet rows = session.execute(...);
//all payloads sent by the server
for (ExecutionInfo info : rows.getAllExecutionInfo()) {
    Map<String,ByteBuffer> serverPayload = info.getIncomingPayload();
}
```

2) From a `PreparedStatement`: to retrieve the payload that the server sent back
with its `PREPARED` response, use the following method:

```java
PreparedStatement ps = session.prepare(...);
Map<String,ByteBuffer> serverPayload = ps.getIncomingPayload();
```

### Custom Payloads and Prepared Statements

If you use either `Session.prepare(RegularStatement)` or `Session.prepareAsync(RegularStatement)`
to prepare a statement, as with all other properties in the given statement, its outgoing
payload, if any, will become the default outgoing payload for the prepared statement.

```java
Statement statement = new SimpleStatement("...");
statement.setOutgoingPayload(myCustomPayload)
PreparedStatement ps = session.prepare(...); // myCustomPayload will be sent with PREPARE request 
                                             // AND become the default outgoing payload
```

Naturally, you can also set a default outgoing payload on the `PreparedStatement` instance itself.
The code below is roughly equivalent to the one above - except that the payload is not sent
with the `PREPARE` request:

```java
Statement statement = new SimpleStatement("...");
PreparedStatement ps = session.prepare(...); // no payload sent with PREPARE request
ps.setOutgoingPayload(myCustomPayload) // default outgoing payload is now myCustomPayload
```

But `PreparedStatement` has also a `getIncomingPayload()` method, which, as we already know, 
can be used to retrieve incoming payloads sent back by the server that prepared the request.

When binding a statement - with either `PreparedStatement.bind()` or `PreparedStatement.bind(Object...)`) - 
`BoundStatement` instances will also inherit payloads.

To determine which payload is going to be used as outgoing payload for a bound statement,
the following rules apply:

1. If the prepared statement has a non-null outgoing payload, all instances of `BoundStatement`
   created out of it will inherit that payload as their outgoing payload;
2. Otherwise, if the prepared statement has a non-null incoming payload, all instances of `BoundStatement`
   created out of it will inherit that payload as their outgoing payload;
3. Otherwise, all instances of `BoundStatement` created out of it
   won't have any outgoing payload set by default; users can however set an outgoing payload on 
   individual `BoundStatement` instances by calling their `setOutgoingPayload()` method,
   like in the example below:
   
```java
Statement statement = new SimpleStatement("...");
PreparedStatement ps = session.prepare(...); // no payload sent with PREPARE request
BoundStatement bs = ps.bind(1); // no outgoing payload by default
bs.setOutgoingPayload(myCustomPayload); // outgoing payload for this specific statement
session.execute(statement); // myCustomPayload will be sent with EXECUTE request for this bound statement only
```

### Debugging custom payloads

When debugging custom payloads, set the `com.datastax.driver.core.Message` logger to the `TRACE` level, e.g. with Log4J:
                                                                                                              
```xml
<logger name="com.datastax.driver.core.Message">
  <level value="TRACE"/>
</logger>
```

This will log a message for every request and every response that contains a non-null payload. 
The log message contains a pretty-printed version of the payload itself, and its total length in bytes.

[CASSANDRA-8553]: https://issues.apache.org/jira/browse/CASSANDRA-8553
[v4spec]: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
[qh]: https://issues.apache.org/jira/browse/CASSANDRA-6659
[nhae]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/exceptions/NoHostAvailableException.html
[chm]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html
[immutablemap]: http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/collect/ImmutableMap.html
[ufe]:http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/exceptions/UnsupportedFeatureException.html

