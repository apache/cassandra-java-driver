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

## Asynchronous programming

### Quick overview

Async driver methods return Java 8's [CompletionStage].

* don't call synchronous methods from asynchronous callbacks (the driver detects that and throws).
* callbacks execute on I/O threads: consider providing your own executor for expensive computations.
* be careful not to accidentally ignore errors thrown from callbacks.

-----

The driver exposes an asynchronous API that allows you to write fully non-blocking programs.
Asynchronous methods return instances of the JDK's [CompletionStage], that can be conveniently
chained and composed.

Here is a short example that opens a session and runs a query asynchronously:

```java
CompletionStage<CqlSession> sessionStage = CqlSession.builder().buildAsync();

// Chain one async operation after another:
CompletionStage<AsyncResultSet> responseStage =
    sessionStage.thenCompose(
        session -> session.executeAsync("SELECT release_version FROM system.local"));

// Apply a synchronous computation:
CompletionStage<String> resultStage =
    responseStage.thenApply(resultSet -> resultSet.one().getString("release_version"));

// Perform an action once a stage is complete:
resultStage.whenComplete(
    (version, error) -> {
      if (error != null) {
        System.out.printf("Failed to retrieve the version: %s%n", error.getMessage());
      } else {
        System.out.printf("Server version: %s%n", version);
      }
      sessionStage.thenAccept(CqlSession::closeAsync);
    });
```

### Threading model

The driver uses two internal thread pools: one for request I/O and one for administrative tasks
(such as metadata refreshes, schema agreement or processing server events). Note that you can
control the size of these pools with the `advanced.netty` options in the
[configuration](../configuration).

When you register a callback on a completion stage, it will execute on a thread in the corresponding
pool:

```java
CompletionStage<CqlSession> sessionStage = CqlSession.builder().buildAsync();
sessionStage.thenAccept(session -> System.out.println(Thread.currentThread().getName()));
// prints s0-admin-n (admin pool thread)

CompletionStage<AsyncResultSet> resultStage =
    session.executeAsync("SELECT release_version FROM system.local");
resultStage.thenAccept(resultSet -> System.out.println(Thread.currentThread().getName()));
// prints s0-io-n (I/O pool thread)
```

As long as you use the asynchronous API, the driver will behave in a non-blocking manner: its 
internal threads will almost never block. There are a few exceptions to the rule though: see the 
manual page on [non-blocking programming](../non_blocking) for details. 

Because the asynchronous API is non-blocking, you can safely call a driver method from inside a 
callback, even when the callback's execution is triggered by a future returned by the driver:

```java
// Get the department id for a given user:
CompletionStage<AsyncResultSet> idStage =
    session.executeAsync("SELECT department_id FROM user WHERE id = 1");

// Once we have the id, query the details of that department:
CompletionStage<AsyncResultSet> dataStage =
    idStage.thenCompose(
        resultSet -> {
          UUID departmentId = resultSet.one().getUuid(0);
          return session.executeAsync(
              SimpleStatement.newInstance(
                  "SELECT * FROM department WHERE id = ?", departmentId));
        });
```

However, you can't call a synchronous method from a callback. This would be very unsafe, because the
driver blocks until the response is received; if the request happened to be assigned to the same
I/O thread that is currently running the callback, it would deadlock. In fact, the driver detects
this situation, and fails fast with a runtime exception to eliminate any chance of a hard-to-debug
deadlock:  

```java
CompletionStage<ResultSet> dataStage =
    idStage.thenApply(
        resultSet -> {
          UUID departmentId = resultSet.one().getUuid(0);
          // WRONG: calling a synchronous method from an asynchronous callback. DON'T DO THIS!
          return session.execute(
              SimpleStatement.newInstance(
                  "SELECT * FROM department WHERE id = ?", departmentId));
        });

// This is just to show the exception:
dataStage.whenComplete(
    (resultSet, error) -> {
      if (error != null) {
        error.printStackTrace();
      }
    });
// java.util.concurrent.CompletionException:
//   java.lang.IllegalStateException: Detected a synchronous API call on a driver thread,
//                                    failing because this can cause deadlocks.
```

You should also be careful about expensive computations: if your callbacks hold I/O threads for too
long, they will negatively impact the driver's throughput. Consider providing your own executor:

```java
// Create this as a global resource in your application
Executor computeExecutor = ...

CompletionStage<Integer> resultStage =
    responseStage.thenApplyAsync( // note: thenApplyAsync instead of thenApply
        resultSet -> someVeryExpensiveComputation(resultSet),
        computeExecutor);
```

Note that an alternate executor can also be used to allow synchronous driver API calls in callbacks,
but the recommended approach is to fully commit to the asynchronous model described above. 

### Error propagation

One thing to pay attention to when programming asynchronously is error handling (this is not
specific to the driver). When all your callback does is a side effect, it's easy to accidentally
swallow an exception: 

```java
CompletionStage<AsyncResultSet> responseStage =
    sessionStage.thenCompose(
        session -> session.executeAsync("SELECT release_version FROM system.local"));
responseStage.thenAccept(
    resultSet -> {
      String version = resultSet.one().getString(0);
      System.out.printf("Server version: %s%n", version);
    });
```

If the request fails, `responseStage` is failed, and `thenAccept` doesn't run the callback at all
(it just returns another failed stage). The error won't be surfaced anywhere, just silently ignored.
One way to address this is with `whenComplete`, which explicitly handles the error:

```java
responseStage.whenComplete(
    (resultSet, error) -> {
      if (error != null) {
        System.out.printf("Failed to retrieve the version: %s%n", error.getMessage());
      } else {
        String version = resultSet.one().getString(0);
        System.out.printf("Server version: %s%n", version);
      }
    });
```

Or you can chain more operations on the result of `printStage`, and handle the error further down
the chain:

```java
CompletionStage<Void> printStage =
    responseStage.thenAccept(
        resultSet -> {
          String version = resultSet.one().getString(0);
          System.out.printf("Server version: %s%n", version);
        });
// Here trivially handled right away for the sake of example, but could be after more operations:
printStage.exceptionally(error -> {
  System.out.printf("Failed to retrieve the version: %s%n", error.getMessage());
  return null;
});
```

One more subtle source for errors is if the callback itself throws:

```java
responseStage.whenComplete(
    (resultSet, error) -> {
      if (error != null) {
        System.out.printf("Request failed: %s%n", error.getMessage());
      } else {
        int v = resultSet.one().getInt(0);
        System.out.printf("The result is %f%n", 1.0 / v);
      }
    });
```

There is a potential division by zero on the last line; the resulting `ArithmeticException` wouldn't
be handled anywhere. Either add a `try/catch` block in the callback, or don't ignore the result of
`whenComplete`.

### Asynchronous paging

Unlike previous versions of the driver, the asynchronous API never triggers synchronous behavior,
even when iterating through the results of a request. `session.executeAsync` returns a dedicated
[AsyncResultSet] that only iterates the current page, the next pages must be fetched explicitly.
This greatly simplifies asynchronous paging; see the [paging](../paging/#asynchronous-paging)
documentation for more details and an example. 

[CompletionStage]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html

[AsyncResultSet]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/AsyncResultSet.html
