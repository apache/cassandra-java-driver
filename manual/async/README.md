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

The driver exposes an asynchronous API that allows you to write programs
in a fully-non blocking manner. Asynchronous methods return instances of
Guava's [ListenableFuture], that can be conveniently chained and
composed.

Here is a short example that opens a session and runs a query
asynchronously:

```java
import com.google.common.util.concurrent.*;

ListenableFuture<Session> session = cluster.connectAsync();

// Use transform with an AsyncFunction to chain an async operation after another:
ListenableFuture<ResultSet> resultSet = Futures.transform(session,
    new AsyncFunction<Session, ResultSet>() {
        public ListenableFuture<ResultSet> apply(Session session) throws Exception {
            return session.executeAsync("select release_version from system.local");
        }
    });

// Use transform with a simple Function to apply a synchronous computation on the result:
ListenableFuture<String> version = Futures.transform(resultSet,
    new Function<ResultSet, String>() {
        public String apply(ResultSet rs) {
            return rs.one().getString("release_version");
        }
    });

// Use a callback to perform an action once the future is complete:
Futures.addCallback(version, new FutureCallback<String>() {
    public void onSuccess(String version) {
        System.out.printf("Cassandra version: %s%n", version);
    }

    public void onFailure(Throwable t) {
        System.out.printf("Failed to retrieve the version: %s%n",
            t.getMessage());
    }
});
```

### Async paging

If you consume a `ResultSet` in a callback, be aware that iterating the
rows will trigger synchronous queries as you page through the results.
To avoid this, use [getAvailableWithoutFetching] to limit the iteration
to the current page, and [fetchMoreResults] to get a future to the next
page (see also the section on [paging](../paging/)).
Here is a full example:

[getAvailableWithoutFetching]: http://docs.datastax.com/en/drivers/java/3.4/com/datastax/driver/core/ResultSet.html#getAvailableWithoutFetching--
[fetchMoreResults]: http://docs.datastax.com/en/drivers/java/3.4/com/datastax/driver/core/ResultSet.html#fetchMoreResults--

```java
Statement statement = new SimpleStatement("select * from foo").setFetchSize(20);
ListenableFuture<ResultSet> future = Futures.transform(
    session.executeAsync(statement),
    iterate(1));

private static AsyncFunction<ResultSet, ResultSet> iterate(final int page) {
    return new AsyncFunction<ResultSet, ResultSet>() {
        @Override
        public ListenableFuture<ResultSet> apply(ResultSet rs) throws Exception {

            // How far we can go without triggering the blocking fetch:
            int remainingInPage = rs.getAvailableWithoutFetching();

            System.out.printf("Starting page %d (%d rows)%n", page, remainingInPage);

            for (Row row : rs) {
                System.out.printf("[page %d - %d] row = %s%n", page, remainingInPage, row);
                if (--remainingInPage == 0)
                    break;
            }
            System.out.printf("Done page %d%n", page);

            boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
            if (wasLastPage) {
                System.out.println("Done iterating");
                return Futures.immediateFuture(rs);
            } else {
                ListenableFuture<ResultSet> future = rs.fetchMoreResults();
                return Futures.transform(future, iterate(page + 1));
            }
        }
    };
}
```

### Good practices

If your callback is slow, consider providing a separate executor.
Otherwise the callback might run on one of the driver's I/O threads,
blocking I/O operations for other requests while it is running:

```java
ListenableFuture<String> result = Futures.transform(resultSet,
    new Function<ResultSet, String>() {
        public String apply(ResultSet rs) {
            return someVeryLongComputation(rs);
        }
    }, myCustomExecutor);
```

Avoid blocking operations in callbacks, especially if you don't provide
a separate executor. This could easily lead to deadlock if the thread
that's supposed to complete the blocking call is also the thread that's
waiting on it:

```java
ListenableFuture<ResultSet> resultSet = Futures.transform(session,
    new Function<Session, ResultSet>() {
        public ResultSet apply(Session session) {
            // Synchronous operation in a callback.
            // DON'T DO THIS! It might deadlock.
            return session.execute("select release_version from system.local");
        }
    });
```

### Known limitations

There are still a few places where the driver will block internally
(mainly for historical reasons):

* [Cluster#init][init] performs blocking I/O operations. To avoid
  issues, you should create your `Cluster` instances while bootstrapping
  your application, and call `init` immediately. If you need to create new
  instances at runtime, make sure this does not happen on an I/O thread.
* trying to read fields from a [query trace] will block if the trace
  hasn't been fetched already.

[ListenableFuture]: https://github.com/google/guava/wiki/ListenableFutureExplained
[init]: http://docs.datastax.com/en/drivers/java/3.4/com/datastax/driver/core/Cluster.html#init--
[query trace]: http://docs.datastax.com/en/drivers/java/3.4/com/datastax/driver/core/QueryTrace.html
