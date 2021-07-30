## Reactive Style Programming

The driver provides built-in support for reactive queries. The [CqlSession] interface extends
[ReactiveSession], which adds specialized methods to execute requests expressed in [reactive
streams].

Notes:

* Reactive capabilities require the [Reactive Streams API] to be present on the classpath. The
  driver has a dependency on that library, but if your application does not use reactive queries at
  all, it is possible to exclude it to minimize the number of runtime dependencies. If the library
  cannot be found at runtime, reactive queries won't be available, and a warning will be logged, but
  the driver will otherwise operate normally (this is also valid for OSGi deployments).
* For historical reasons, reactive-related driver types reside in a package prefixed with `dse`;
  however, reactive queries also work with regular Cassandra.
* The reactive execution model is implemented in a non-blocking fashion: see the manual page on 
  [non-blocking programming](../non_blocking) for details.

### Overview

`ReactiveSession` exposes two public methods:

```java
ReactiveResultSet executeReactive(String query);
ReactiveResultSet executeReactive(Statement<?> statement);
```

Both methods return a [ReactiveResultSet], which is the reactive streams version of a regular
[ResultSet]. In other words, a `ReactiveResultSet` is a [Publisher] for query results.

When subscribing to and consuming from a `ReactiveResultSet`, there are two important caveats to
bear in mind:

1. By default, all `ReactiveResultSet` implementations returned by the driver are cold, unicast,
  single-subscription-only publishers. In other words, they do not support multiple subscribers;
  consider caching the results produced by such publishers if you need to consume them by more than
  one downstream subscriber. We provide a few examples of caching further in this document.
2. Also, note that reactive result sets may emit items to their subscribers on an internal driver IO
  thread. Subscriber implementors are encouraged to abide by [Reactive Streams Specification rule
  2.2] and avoid performing heavy computations or blocking calls inside `onNext` calls, as doing so
  could slow down the driver and impact performance. Instead, they should asynchronously dispatch
  received signals to their processing logic.

### Basic usage

The examples in this page make usage of [Reactor], a popular reactive library, but they should be
easily adaptable to any other library implementing the concepts of reactive streams.

#### Reading in reactive style

The following example reads from a table and prints all the returned rows to the console. In case of
error, a `DriverException` is thrown and its stack trace is printed to standard error:

```java 
try (CqlSession session = ...) {
      Flux.from(session.executeReactive("SELECT ..."))
          .doOnNext(System.out::println)
          .blockLast();
} catch (DriverException e) {
  e.printStackTrace();
}
```

#### Writing in reactive style

The following example inserts rows into a table after printing the queries to the console, stopping
at the first error, if any. Again, in case of error, a `DriverException` is thrown:

```java
try (CqlSession session = ...) {
  Flux.just("INSERT ...", "INSERT ...", "INSERT ...", ...)
      .doOnNext(System.out::println)
      .flatMap(session::executeReactive)
      .blockLast();
} catch (DriverException e) {
  e.printStackTrace();
}
```

Note that when a statement is executed reactively, the actual request is only triggered when the
`ReactiveResultSet` is subscribed to; in other words, when the `executeReactive` method returns,
_nothing has been executed yet_. This is why the write example above uses a `flatMap` operator,
which takes care of subscribing to each `ReactiveResultSet` returned by successive calls to
`session.executeReactive`. A common pitfall is to use an operator that silently ignores the returned
`ReactiveResultSet`; for example, the code below seems correct, but will not execute any query:

```java
// DON'T DO THIS
Flux.just("INSERT INTO ...")
     // The returned ReactiveResultSet is not subscribed to
    .doOnNext(session::executeReactive)
    .blockLast();
```

Since a write query does not return any rows, it may appear difficult to count the number of rows
written to the database. Hopefully most reactive libraries have operators that are useful in these
scenarios. The following example demonstrates how to achieve this goal with Reactor:

```java
Flux<Statement<?>> stmts = ...;
long count =
    stmts
        .flatMap(
            stmt ->
                Flux.from(session.executeReactive(stmt))
                    // dummy cast, since result sets are always empty for write queries
                    .cast(Integer.class)
                    // flow will always be empty, so '1' will be emitted for each query
                    .defaultIfEmpty(1))
        .count()
        .block();
System.out.printf("Executed %d write statements%n", count);
```

### Accessing query metadata

`ReactiveResultSet` exposes useful information about request execution and query metadata:

```java
Publisher<? extends ColumnDefinitions> getColumnDefinitions();
Publisher<? extends ExecutionInfo> getExecutionInfos();
Publisher<Boolean> wasApplied(); 
```

Refer to the javadocs of [getColumnDefinitions], [getExecutionInfos] and [wasApplied] for more 
information on these methods.

To inspect the contents of the above publishers, simply subscribe to them. Note that these 
publishers cannot complete before the query itself completes; if the query fails, then these 
publishers will fail with the same error.

The following example executes a query, then prints all the available metadata to the console:

```java
ReactiveResultSet rs = session.executeReactive("SELECT ...");
// execute the query first
Flux.from(rs).blockLast();
// then retrieve query metadata
System.out.println("Column definitions: ");
Mono.from(rs.getColumnDefinitions()).doOnNext(System.out::println).block();
System.out.println("Execution infos: ");
Flux.from(rs.getExecutionInfos()).doOnNext(System.out::println).blockLast();
System.out.println("Was applied: ");
Mono.from(rs.wasApplied()).doOnNext(System.out::println).block();
```

Note that it is also possible to inspect query metadata at row level. Each row returned by a 
reactive query execution implements [`ReactiveRow`][ReactiveRow], the reactive equivalent of a 
[`Row`][Row].

`ReactiveRow` exposes the same kind of query metadata and execution info found in 
`ReactiveResultSet`, but for each individual row:

```java
ColumnDefinitions getColumnDefinitions();
ExecutionInfo getExecutionInfo();
boolean wasApplied();
```

Refer to the javadocs of [`getColumnDefinitions`][ReactiveRow.getColumnDefinitions],
[`getExecutionInfo`][ReactiveRow.getExecutionInfo] and [`wasApplied`][ReactiveRow.wasApplied] for
more information on these methods.

The following example executes a query and, for each row returned, prints the coordinator that
served that row, then retrieves all the coordinators that were contacted to fulfill the query and
prints them to the console:
  
```java
Iterable<Node> coordinators = Flux.from(session.executeReactive("SELECT ..."))
    .doOnNext(
        row ->
            System.out.printf(
                "Row %s was obtained from coordinator %s%n",
                row,
                row.getExecutionInfo().getCoordinator()))
    .map(ReactiveRow::getExecutionInfo)
    // dedup by coordinator (note: this is dangerous on a large result set)
    .groupBy(ExecutionInfo::getCoordinator)
    .map(GroupedFlux::key)
    .toIterable();
System.out.println("Contacted coordinators: " + coordinators);
```

### Advanced topics

#### Applying backpressure

One of the key features of reactive programming is backpressure.

Unfortunately, the Cassandra native protocol does not offer proper support for exchanging
backpressure information between client and server over the network. Cassandra is able, since
version 3.10, to [throttle clients](https://issues.apache.org/jira/browse/CASSANDRA-9318) but at the
time of writing, there is no proper [client-facing backpressure
mechanism](https://issues.apache.org/jira/browse/CASSANDRA-11380) available.

When reading from Cassandra, this shouldn't however be a problem for most applications. Indeed, in a
read scenario, Cassandra acts as a producer, and the driver is a consumer; in such a setup, if a
downstream subscriber is not able to cope with the throughput, the driver would progressively adjust
the rate at which it requests more pages from the server, thus effectively regulating the server
throughput to match the subscriber's. The only caveat is if the subscriber is really too slow, which
could eventually trigger a query timeout, be it on the client side (`DriverTimeoutException`), or on
the server side (`ReadTimeoutException`).

When writing to Cassandra, the lack of backpressure communication between client and server is more
problematic. Indeed in a write scenario, the driver acts as a producer, and Cassandra is a consumer;
in such a setup, if an upstream producer generates too much data, the driver would blindly send the
write statements to the server as quickly as possible, eventually causing the cluster to become
overloaded or even crash. This usually manifests itself with errors like `WriteTimeoutException`, or
`OverloadedException`.

It is strongly advised for users to limit the concurrency at which write statements are executed in
write-intensive scenarios. A simple way to achieve this is to use the `flatMap` operator, which, in
most reactive libraries, has an overloaded form that takes a parameter that controls the desired
amount of concurrency. The following example executes a flow of statements with a maximum
concurrency of 10, leveraging the `concurrency` parameter of Reactor's `flatMap` operator:

```java
Flux<Statement<?>> stmts = ...;
stmts.flatMap(session::executeReactive, 10).blockLast();
```

In the example above, the `flatMap` operator will subscribe to at most 10 `ReactiveResultSet`
instances simultaneously, effectively limiting the number of concurrent in-flight requests to 10.
This is usually enough to prevent data from being written too fast. More sophisticated operators are
capable of rate-limiting or throttling the execution of a flow; for example, Reactor offers a
`delayElements` operator that rate-limits the throughput of its upstream publisher. Consult the
documentation of your reactive library for more information.

As a last resort, it is also possible to limit concurrency at driver level, for example using the
driver's built-in [request throttling] mechanism, although this is usually not required in reactive
applications. See "[Managing concurrency in asynchronous query execution]" in the Developer Guide
for a few examples.

#### Caching query results

As stated above, a `ReactiveResultSet` can only be subscribed once. This is an intentional design
decision, because otherwise users could inadvertently trigger a spurious execution of the same query
again when subscribing for the second time to the same `ReactiveResultSet`.

Let's suppose that we want to compute both the average and the sum of all values from a table
column. The most naive approach would be to create two flows and subscribe to both:

 ```java
// DON'T DO THIS
ReactiveResultSet rs = session.executeReactive("SELECT n FROM ...");
double avg = Flux.from(rs)
    .map(row -> row.getLong(0))
    .reduce(0d, (a, b) -> (a + b / 2.0))
    .block();
// will fail with IllegalStateException
long sum = Flux.from(rs)
    .map(row -> row.getLong(0))
    .reduce(0L, (a, b) -> a + b)
    .block();
 ```

Unfortunately, the second `Flux` above with terminate immediately with an `onError` signal
encapsulating an `IllegalStateException`, since `rs` was already subscribed to.

To circumvent this limitation, while still avoiding to query the table twice, the easiest technique
consists in using the `cache` operator that most reactive libraries offer:

```java
Flux<Long> rs = Flux.from(session.executeReactive("SELECT n FROM ..."))
    .map(row -> row.getLong(0))
    .cache();
double avg = rs
    .reduce(0d, (a, b) -> (a + b / 2.0))
    .block();
long sum = rs
    .reduce(0L, (a, b) -> a + b)
    .block();
```

The above example works just fine.

The `cache` operator will subscribe at most once to the `ReactiveResultSet`, cache the results, and
serve the cached results to downstream subscribers. This is obviously only possible if your result
set is small and can fit entirely in memory.

If caching is not an option, most reactive libraries also offer operators that multicast their
upstream subscription to many subscribers on the fly.

The above example could be rewritten with a different approach as follows:

```java
Flux<Long> rs = Flux.from(session.executeReactive("SELECT n FROM ..."))
    .map(row -> row.getLong(0))
    .publish()       // multicast upstream to all downstream subscribers
    .autoConnect(2); // wait until two subscribers subscribe
long sum = rs
    .reduce(0L, (a, b) -> a + b)
    .block();
double avg = rs
    .reduce(0d, (a, b) -> (a + b / 2.0))
    .block();
```

In the above example, the `publish` operator multicasts every `onNext` signal to all of its
subscribers; and the `autoConnect(2)` operator instructs `publish` to wait until it gets 2
subscriptions before subscribing to its upstream source (and triggering the actual query execution).

This approach should be the preferred one for large result sets since it does not involve caching
results in memory.

#### Resuming from and retrying after failed queries

When executing a flow of statements, any failed query execution would trigger an `onError` signal
and terminate the subscription immediately, potentially preventing subsequent queries from being
executed at all.

If this behavior is not desired, it is possible to mimic the behavior of a fail-safe system. This
usually involves the usage of operators such as `onErrorReturn` or `onErrorResume`. Consult your
reactive library documentation to find out which operators allow you to intercept failures.

The following example executes a flow of statements; for each failed execution, the stack trace is
printed to standard error and, thanks to the `onErrorResume` operator, the error is completely
ignored and the flow execution resumes normally:

```java
Flux<Statement<?>> stmts = ...;
stmts.flatMap(
    statement ->
        Flux.from(session.executeReactive(statement))
            .doOnError(Throwable::printStackTrace)
            .onErrorResume(error -> Mono.empty()))
    .blockLast();
```

The following example expands on the previous one: for each failed execution, at most 3 retries are
attempted if the error was an ` UnavailableException`, then, if the query wasn't successful after
retrying, a message is logged. Finally, all the errors are collected and the total number of failed
queries is printed to the console:

```java
Flux<Statement<?>> statements = ...;
long failed = statements.flatMap(
    stmt ->
        Flux.defer(() -> session.executeReactive(stmt))
            // retry at most 3 times on Unavailable
            .retry(3, UnavailableException.class::isInstance)
            // handle errors
            .doOnError(
                error -> {
                  System.err.println("Statement failed: " + stmt);
                  error.printStackTrace();
                })
            // Collect errors and discard all returned rows
            .ignoreElements()
            .cast(Long.class)
            .onErrorReturn(1L))
    .sum()
    .block();
System.out.println("Total failed queries: " + failed);
```

The example above uses `Flux.defer()` to wrap the call to `session.executeReactive()`. This is
required because, as mentioned above, the driver always creates single-subscription-only publishers.
Such publishers are not compatible with operators like `retry` because these operators sometimes
subscribe more than once to the upstream publisher, thus causing the driver to throw an exception.
Hopefully it's easy to solve this issue, and that's exactly what the `defer` operator is designed
for: each subscription to the `defer` operator triggers a distinct call to
`session.executeReactive()`, thus causing the session to re-execute the query and return a brand-new
publisher at every retry.

Note that the driver already has a [built-in retry mechanism] that can transparently retry failed
queries; the above example should be seen as a demonstration of application-level retries, when a
more fine-grained control of what should be retried, and how, is required.

[CqlSession]:                       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/CqlSession.html
[ReactiveSession]:                  https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveSession.html
[ResultSet]:                        https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/ResultSet.html
[ReactiveResultSet]:                https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveResultSet.html
[ReactiveRow]:                      https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveRow.html
[Row]:                              https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/Row.html
[getColumnDefinitions]:             https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveResultSet.html#getColumnDefinitions--
[getExecutionInfos]:                https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveResultSet.html#getExecutionInfos--
[wasApplied]:                       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveResultSet.html#wasApplied--
[ReactiveRow.getColumnDefinitions]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveRow.html#getColumnDefinitions--
[ReactiveRow.getExecutionInfo]:     https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveRow.html#getExecutionInfo--
[ReactiveRow.wasApplied]:           https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveRow.html#wasApplied--

[built-in retry mechanism]: ../retries/
[request throttling]: ../throttling/

[Managing concurrency in asynchronous query execution]: https://docs.datastax.com/en/devapp/doc/devapp/driverManagingConcurrency.html]
[Publisher]: https://www.reactive-streams.org/reactive-streams-1.0.2-javadoc/org/reactivestreams/Publisher.html
[reactive streams]: https://en.wikipedia.org/wiki/Reactive_Streams
[Reactive Streams API]: https://github.com/reactive-streams/reactive-streams-jvm
[Reactive Streams Specification rule 2.2]: https://github.com/reactive-streams/reactive-streams-jvm#2.2
[Reactor]: https://projectreactor.io/
