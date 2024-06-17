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

## Frequently asked questions

### I'm modifying a statement and the changes get ignored, why?

In driver 4, statement classes are **immutable**. All mutating methods return a new instance, so
make sure you don't accidentally ignore their result:

```java
BoundStatement boundSelect = preparedSelect.bind();

// This doesn't work: setInt and setPageSize don't modify boundSelect in place:
boundSelect.setInt("k", key);
boundSelect.setPageSize(1000);
session.execute(boundSelect);

// Instead, reassign the statement every time:
boundSelect = boundSelect.setInt("k", key).setPageSize(1000);
```

All of these mutating methods are annotated with `@CheckReturnValue`. Some code analysis tools --
such as [ErrorProne](https://errorprone.info/) -- can check correct usage at build time, and report
mistakes as compiler errors.

The driver also provides builders:

```java
BoundStatement boundSelect =
    preparedSelect.boundStatementBuilder()
        .setInt("k", key)
        .setPageSize(1000)
        .build();
```

### Why do asynchronous methods return `CompletionStage<T>` instead of `CompletableFuture<T>`?

Because it's the right abstraction to use. A completable future, as its name indicates, is a future
that can be completed manually; that is not what we want to return from our API: the driver
completes the futures, not the user.

Also, `CompletionStage` does not expose a `get()` method; one can view that as an encouragement to
use a fully asynchronous programming model (chaining callbacks instead of blocking for a result).

At any rate, `CompletionStage` has a `toCompletableFuture()` method. In current JDK versions, every
`CompletionStage` is a `CompletableFuture`, so the conversion has no performance overhead.

### Where is `DowngradingConsistencyRetryPolicy` from driver 3?

**As of driver 4.10, this retry policy was made available again as a built-in alternative to the 
default retry policy**: see the [manual](../manual/core/retries) to understand how to use it. 
For versions between 4.0 and 4.9 inclusive, there is no built-in equivalent of driver 3 
`DowngradingConsistencyRetryPolicy`.

That retry policy was indeed removed in driver 4.0.0. The main motivation is that this behavior 
should be the application's concern, not the driver's. APIs provided by the driver should instead 
encourage idiomatic use of a distributed system like Apache Cassandra, and a downgrading policy 
works against this. It suggests that an anti-pattern such as "try to read at QUORUM, but fall back 
to ONE if that fails" is a good idea in general use cases, when in reality it provides no better 
consistency guarantees than working directly at ONE, but with higher latencies. 

However, we recognize that there are use cases where downgrading is good -- for instance, a 
dashboard application would present the latest information by reading at QUORUM, but it's acceptable 
for it to display stale information by reading at ONE sometimes. 

Thanks to [JAVA-2900], an equivalent retry policy with downgrading behavior was re-introduced in
driver 4.10. Nonetheless, we urge users to avoid using it unless strictly required, and instead, 
carefully choose upfront the consistency level that works best for their use cases. Even if there 
is a legitimate reason to downgrade and retry, that should be preferably handled by the application 
code. An example of downgrading retries implemented at application level can be found in the driver
[examples repository].

[JAVA-2900]: https://datastax-oss.atlassian.net/browse/JAVA-2900
[examples repository]: https://github.com/datastax/java-driver/blob/4.x/examples/src/main/java/com/datastax/oss/driver/examples/retry/DowngradingRetry.java

### Where is the cross-datacenter failover feature that existed in driver 3?

In driver 3, it was possible to configure the load balancing policy to automatically failover to
a remote datacenter, when the local datacenter is down.

This ability is considered a misfeature and has been removed from driver 4.0 onwards.

However, due to popular demand, cross-datacenter failover has been brought back to driver 4 in
version 4.10.0.

If you are using a driver version >= 4.10.0, read the [manual](../manual/core/loadbalancing/) to
understand how to enable this feature; for driver versions < 4.10.0, this feature is simply not
available.

### I want to set a date on a bound statement, where did `setTimestamp()` go?

The driver now uses Java 8's improved date and time API. CQL type `timestamp` is mapped to
`java.time.Instant`, and the corresponding getter and setter are `getInstant` and `setInstant`.

See [Temporal types](../manual/core/temporal_types/) for more details.

### Why do DDL queries have a higher latency than driver 3?

If you benchmark DDL queries such as `session.execute("CREATE TABLE ...")`, you will observe a
noticeably higher latency than driver 3 (about 1 second).

This is because those queries are now *debounced*: the driver adds a short wait in an attempt to
group multiple schema changes into a single metadata refresh. If you want to mitigate this, you can
either adjust the debouncing settings, or group your schema updates while temporarily disabling the
metadata; see the [performance](../manual/core/performance/#debouncing) page.

This only applies to DDL queries; DML statements (`SELECT`, `INSERT`...) are not debounced.
