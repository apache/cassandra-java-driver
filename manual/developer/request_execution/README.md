## Request execution

The [Netty pipeline](../netty_pipeline/) gives us the ability to send low-level protocol messages on
a single connection.

The request execution layer builds upon that to:

* manage multiple connections (many nodes, possibly many connections per node);
* abstract the protocol layer behind higher-level, user-facing types.

The session is the main entry point. `CqlSession` is the type that users will most likely reference
in their applications. It extends a more generic `Session` type, for the sake of extensibility; this
will be explained in [Request processors](#request-processors). 


```ditaa
+----------------------------------+
| Session                          |
+----------------------------------+
| ResultT execute(                 |
|   RequestT, GenericType[ResultT])|
+----------------------------------+
                 ^
                 |
+----------------+-----------------+
| CqlSession                       |
+----------------------------------+
| ResultSet execute(Statement)     |
+----------------+-----------------+
                 ^
                 |
+----------------+-----------------+
| DefaultSession                   |
+----------------+-----------------+
                 |
                 |
                 | 1 per node +-------------+
                 +------------+ ChannelPool |
                 |            +----+--------+
                 |                 |
                 |                 |  n +---------------+
                 |                 +----+ DriverChannel |
                 |                      +---------------+
                 |
                 |          1 +--------------------------+
                 +------------+ RequestProcessorRegistry |
                              +----+---------------------+
                                   |
                                   |  n +---------------------------+
                                   +----+ RequestProcessor          |
                                        +---------------------------+
                                        | ResultT process(RequestT) |
                                        +---------------------------+
```

`DefaultSession` contains the session implementation. It follows the [confined inner
class](../common/concurrency/#cold-path) pattern to simplify concurrency.

### Connection pooling

```ditaa
+----------------------+       1 +------------+
| ChannelPool          +---------+ ChannelSet |
+----------------------+         +-----+------+
| DriverChannel next() |               |
+----------+-----------+              n|
           |                    +------+--------+
          1|                    | DriverChannel |
    +------+-------+            +---------------+
    | Reconnection |
    +--------------+
```

`ChannelPool` handles the connections to a given node, for a given session. It follows the [confined
inner class](../common/concurrency/#cold-path) pattern to simplify concurrency. There are a few
differences compared to the 3.x implementation:

#### Fixed size

The pool has a fixed number of connections, it doesn't grow or shrink dynamically based on current
usage. In other words, there is no more "max" size, only a "core" size.

However, this size is specified in the configuration. If the value is changed at runtime, the driver
will detect it, and trigger a resize of all active pools.

The rationale for removing the dynamic behavior is that it introduced a ton of complexity in the
implementation and configuration, for unclear benefits: if the load fluctuates very rapidly, then
you need to provision for the max size anyway, so you might as well run with all the connections all
the time. If on the other hand the fluctuations are rare and predictable (e.g. peak for holiday
sales), then a manual configuration change is good enough.

#### No queuing

To get a connection to a node, client code calls `ChannelPool.next()`. This returns the less busy
connection, based on the the `getAvailableIds()` counter exposed by
[InFlightHandler](netty_pipeline/#in-flight-handler).

If all connections are busy, there is no queuing; the driver moves to the next node immediately. The
rationale is that it's better to try another node that might be ready to reply, instead of
introducing an additional wait for each node. If the user wants queuing when all nodes are busy,
it's better to do it at the session level with a [throttler](../../core/throttling/), which provides
more intuitive configuration.

Before 4.5.0, there was also no preemptive acquisition of the stream id outside of the event loop:
`getAvailableIds()` had volatile semantics, and a client could get a pooled connection that seemed
not busy, but fail to acquire a stream id when it later tried the actual write. This turned out to
not work well under high load, see [JAVA-2644](https://datastax-oss.atlassian.net/browse/JAVA-2644).

Starting with 4.5.0, we've reintroduced a stronger guarantee (reminiscent of how things worked in
3.x): clients **must call `DriverChannel.preAcquireId()` exactly once before each write**. If the
call succeeds, `getAvailableIds()` is incremented immediately, and the client is guaranteed that
there will be a stream id available for the write. `preAcquireId()` and `getAvailableIds()` have
atomic semantics, so we can distribute the load more accurately.

This comes at the cost of additional complexity: **we must ensure that every write is pre-acquired
first**, so that `getAvailableIds()` doesn't get out of sync with the actual stream id usage inside
`InFlightHandler`. This is explained in detail in the javadocs of `DriverChannel.preAcquireId()`,
read them carefully. 

The pool manages its channels with `ChannelSet`, a simple copy-on-write data structure.

#### Built-in reconnection

The pool has its own independent reconnection mechanism (based on the `Reconnection` utility class).
The goal is to keep the pool at its expected capacity: whenever a connection is lost, the task
starts and will try to reopen the missing connections at regular intervals.

### Request processors

```ditaa
+----------------------------------+
| Session                          |
+----------------------------------+
| ResultT execute(                 |
|   RequestT, GenericType[ResultT])|
+----------------------------------+
                 ^
                 |
+----------------+-----------------+
| CqlSession                       |
+----------------------------------+
| ResultSet execute(Statement)     |
+----------------+-----------------+
```

The driver can execute different types of requests, in different ways. This is abstracted by the
top-level `Session` interface, with a very generic execution method:

```java
<RequestT extends Request, ResultT> ResultT execute(
      RequestT request, GenericType<ResultT> resultType);
```

It takes a request, and a type token that serves as a hint at the expected result. Each `(RequestT,
ResultT)` combination defines an execution model, for example:

| `RequestT` | `ResultT` | Execution |
| --- | --- | ---|
| `Statement` | `ResultSet` | CQL, synchronous |
| `Statement` | `CompletionStage<AsyncResultSet>` | CQL, asynchronous |
| `Statement` | `ReactiveResultSet` | CQL, reactive |
| `GraphStatement` | `GraphResultSet` | DSE Graph, synchronous |
| `GraphStatement` | `CompletionStage<AsyncGraphResultSet>` | DSE Graph, asynchronous |

In general, regular client code doesn't use `Session.execute` directly. Instead, child interfaces
expose more user-friendly shortcuts for a given result type:

```java
public interface CqlSession extends Session {
  default ResultSet execute(Statement<?> statement) {
    return execute(statement, Statement.SYNC);
  }
}
```

The logic for each execution model is encapsulated in a `RequestProcessor<RequestT, ResultT>`.
Processors are stored in a `RequestProcessorRegistry`. For each request, the session invokes the
registry to find the processor that matches the request and result types. 

```ditaa
+----------------+  1+-----------------------------------+
| DefaultSession +---+ RequestProcessorRegistry          |
+----------------+   +-----------------------------------+
                     | processorFor(                     |
                     |   RequestT, GenericType[ResultT]) |
                     +-----------------+-----------------+
                                       |
                                       |n
                +----------------------+----------------------+
                | RequestProcessor[RequestT, ResultT]         |
                +---------------------------------------------+
                | boolean canProcess(Request, GenericType[?]) |
                | ResultT process(RequestT)                   |
                +---------------------------------------------+
                        ^
                        |         +--------------------------+
                        +---------+ CqlRequestSyncProcessor  |
                        |         +--------------------------+
                        |
                        |         +--------------------------+
                        +---------+ CqlRequestAsyncProcessor |
                        |         +--------------------------+
                        |
                        |         +--------------------------+
                        +---------+ CqlPrepareSyncProcessor  |
                        |         +--------------------------+
                        |
                        |         +--------------------------+
                        +---------+ CqlPrepareAsyncProcessor |
                                  +--------------------------+
```

A processor is responsible for:

* converting the user request into [protocol-level messages](../native_protocol/);
* selecting a coordinator node, and obtaining a channel from its connection pool;
* writing the request to the channel;
* handling timeouts, retries and speculative executions;
* translating the response into user-level types.

The `RequestProcessor` interface makes very few assumptions about the actual processing; but in
general, implementations create a handler for the lifecycle of every request. For example,
`CqlRequestHandler` is the central component for basic CQL execution.

Processors can be implemented in terms of other processors. In particular, this is the case for
synchronous execution models, which are just a blocking wrapper around their asynchronous
counterpart. You can observe this in `CqlRequestSyncProcessor`.

Note that preparing a statement is treated as just another execution model. It has its own
processors, that operate on a special `PrepareRequest` type:

```java
public interface CqlSession extends Session {
  default PreparedStatement prepare(SimpleStatement statement) {
    return execute(new DefaultPrepareRequest(statement), PrepareRequest.SYNC);
  }
}
```

### Extension points

#### RequestProcessorRegistry

You can customize the set of request processors by [extending the
context](../common/context/#overriding-a-context-component) and overriding
`buildRequestProcessorRegistry`.

This can be used to either:

* add your own execution models (new request types and/or return types);
* remove existing ones;
* or a combination of both.

The driver codebase contains an integration test that provides a complete example:
[RequestProcessorIT]. It shows how you can build a session that returns Guava's `ListenableFuture`
instead of Java's `CompletionStage` (existing request type, different return type).

[GuavaDriverContext] is the custom context subclass. It plugs a custom registry that wraps the
default async processors with [GuavaRequestAsyncProcessor], to transform the returned futures.

Note that the default async processors are not present in the registry anymore; if you try to call
a method that returns a `CompletionStage`, it fails. See the next section for how to hide those
methods.   

#### Exposing a custom session interface

If you add or remove execution models, you probably want to expose a session interface that matches
the underlying capabilities of the implementation.

For example, in the [RequestProcessorIT] example mentioned in the previous section, we remove the
ability to return `CompletionStage`, but add the ability to return `ListenableFuture`. Therefore we
expose a custom [GuavaSession] with a different return type for async methods:

```java
public interface GuavaSession extends Session {
  default ListenableFuture<AsyncResultSet> executeAsync(Statement<?> statement) { ... }
  default ListenableFuture<PreparedStatement> prepareAsync(SimpleStatement statement) { ... }
}
```

We need an implementation of this interface. Our new methods all have default implementations in
term of the abstract `Session.execute()`, so the only thing we need is to delegate to an existing
`Session`. The driver provides `SessionWrapper` to that effect. See [DefaultGuavaSession]:

```java
public class DefaultGuavaSession extends SessionWrapper implements GuavaSession {
  public DefaultGuavaSession(Session delegate) {
    super(delegate);
  }
}
```

Finally, we want to create an instance of this wrapper. Since we extended the context (see previous
section), we already wrote a custom builder subclass; there is another protected method we can
override to plug our wrapper. See [GuavaSessionBuilder]:

```java
public class GuavaSessionBuilder extends SessionBuilder<GuavaSessionBuilder, GuavaSession> {

  @Override
  protected DriverContext buildContext( ... ) { ... }

  @Override
  protected GuavaSession wrap(CqlSession defaultSession) {
    return new DefaultGuavaSession(defaultSession);
  }
```

Client code can now use the familiar pattern to create a session:   

```java
GuavaSession session = new GuavaSessionBuilder()
    .addContactEndPoints(...)
    .withKeyspace("test")
    .build();
```

[RequestProcessorIT]: https://github.com/datastax/java-driver/blob/4.x/integration-tests/src/test/java/com/datastax/oss/driver/core/session/RequestProcessorIT.java
[GuavaDriverContext]: https://github.com/datastax/java-driver/blob/4.x/integration-tests/src/test/java/com/datastax/oss/driver/example/guava/internal/GuavaDriverContext.java
[GuavaRequestAsyncProcessor]: https://github.com/datastax/java-driver/blob/4.x/integration-tests/src/test/java/com/datastax/oss/driver/example/guava/internal/GuavaRequestAsyncProcessor.java
[GuavaSession]: https://github.com/datastax/java-driver/blob/4.x/integration-tests/src/test/java/com/datastax/oss/driver/example/guava/api/GuavaSession.java
[DefaultGuavaSession]: https://github.com/datastax/java-driver/blob/4.x/integration-tests/src/test/java/com/datastax/oss/driver/example/guava/internal/DefaultGuavaSession.java
[GuavaSessionBuilder]: https://github.com/datastax/java-driver/blob/4.x/integration-tests/src/test/java/com/datastax/oss/driver/example/guava/api/GuavaSessionBuilder.java
