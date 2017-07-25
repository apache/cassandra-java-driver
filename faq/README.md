## Frequently asked questions

### I'm modifying a statement and the changes get ignored, why?

In driver 4, statement classes are immutable. All mutating methods return a new instance, so make
sure you don't accidentally ignore their result:

```java
BoundStatement boundSelect = preparedSelect.bind();

// This doesn't work: setInt doesn't modify boundSelect in place:
boundSelect.setInt("k", key);
session.execute(boundSelect);

// Instead, do this:
boundSelect = boundSelect.setInt("k", key);
```

### Why do asynchronous methods return `CompletionStage<T>` instead of `CompletableFuture<T>`?

Because it's the right abstraction to use. A completable future, as its name indicates, is a future
that can be completed manually; that is not what we want to return from our API: the driver
completes the futures, not the user.

Also, `CompletionStage` does not expose a `get()` method; one can view that as an encouragement to
use a fully asynchronous programming model (chaining callbacks instead of blocking for a result).

At any rate, `CompletionStage` has a `toCompletableFuture()` method. In current JDK versions, every
`CompletionStage` is a `CompletableFuture`, so the conversion has no performance overhead.