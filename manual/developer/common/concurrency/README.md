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

## Concurrency

The driver is a highly concurrent environment. We try to use thread confinement to simplify the
code, when that does not impact performance.

### Hot path

The hot path is everything that happens for a `session.execute` call. In a typical client
application, this is where the driver will likely spend the majority of its time, so it must be
fast.

Write path:

1. convert the statement into a protocol-level `Message` (`CqlRequestHandler` constructor);
2. find a node and a connection, and write the message to it (`CqlRequestHandler.sendRequest`);
3. assign a stream id and wrap the message into a frame (`InflightHandler.write`);
4. encode the frame into a binary payload (`FrameEncoder`).

Read path:

1. decode the binary payload into a frame (`FrameDecoder`);
2. find the handler that corresponds to the stream id (`InFlightHandler.channelRead`);
3. complete the client's future (`CqlRequestHandler.NodeResponseCallback.onResponse`).

Various policies are also invoked along the way (load balancing, retry, speculative execution,
timestamp generator...), they are considered on the hot path too.

Steps 1 and 2 of the write path happen on the client thread, and 3 and 4 on the Netty I/O thread
(which is one of the threads in `NettyOptions.ioEventLoopGroup()`). 
On the read path, everything happens on the Netty I/O thread. Beyond that, we want to avoid context
switches for performance reasons: in early prototypes, we tried confining `CqlRequestHandler` to a
particular thread, but that did not work well; so you will find that the code is fairly similar to
driver 3 in terms of concurrency control (reliance on atomic structures, volatile fields, etc).

Note: code on the hot path should prefer the `TRACE` log level.

### Cold path

The cold path is everything else: initialization and shutdown, metadata refreshes, tracking node
states, etc. They will typically be way less frequent than user requests, so we can tolerate a small
performance hit in order to make concurrency easier to handle.

One pattern we use a lot is a confined inner class: 

```java
public class ControlConnection {
  // some content omitted for brevity
  
  private final EventExecutor adminExecutor;
  private final SingleThreaded singleThreaded;
  
  // Called from other components, from any thread
  public void reconnectNow() {
    RunOrSchedule.on(adminExecutor, singleThreaded::reconnectNow);
  }
  
  private class SingleThreaded {
    private void reconnectNow() {
      assert adminExecutor.inEventLoop();
      // this method is only ever called from one thread, much easier to handle concurrency
    }
  }
}
```

Public outer methods such as `reconnectNow()` are called concurrently. But they delegate to a method
of the internal class, that always runs on the same `adminExecutor` thread. `RunOrSchedule.on` calls
the method directly if we're already on the target thread, otherwise it schedules a task. If we need
to propagate a result, the outer method injects a future that the inner method completes.

`adminExecutor` is picked randomly from `NettyOptions.adminEventExecutorGroup()` at construction
time.

Confining `SingleThreaded` simplifies the code tremendously: we can use regular, non-volatile
fields, and methods are guaranteed to always run in isolation, eliminating subtle race conditions
(this idea was borrowed from actor systems).  

### Non-blocking

Whether on the hot or cold path, internal code is almost 100% lock-free. The driver guarantees on
lock-freedom are [detailed](../../../core/non_blocking) in the core manual.

If an internal component needs to execute a query, it does so asynchronously, and registers 
callbacks to process the results. Examples of this can be found in `ReprepareOnUp` and 
`DefaultTopologyMonitor` (among others). 

The only place where the driver blocks is when using the synchronous API (methods declared in 
[`SyncCqlSession`]), and when calling other synchronous wrapper methods in the public API, for
example, [`ExecutionInfo.getQueryTrace()`]:

```java
public interface ExecutionInfo {
  // some content omitted for brevity
  
  default QueryTrace getQueryTrace() {
    BlockingOperation.checkNotDriverThread();
    return CompletableFutures.getUninterruptibly(getQueryTraceAsync());
  }
}
```

When a public API method is blocking, this is generally clearly stated in its javadocs. 

[`ExecutionInfo.getQueryTrace()`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/ExecutionInfo.html#getQueryTrace--
[`SyncCqlSession`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/SyncCqlSession.html`

`BlockingOperation` is a utility to check that those methods aren't called on I/O threads, which
could introduce deadlocks.

Keeping the internals fully asynchronous is another major improvement over driver 3, where internal
requests were synchronous, and required multiple internal executors to avoid deadlocks.

In driver 4, there are only two executors: `NettyOptions.ioEventLoopGroup()` and
`NettyOptions.adminEventLoopGroup()`, that are guaranteed to never run blocking tasks. They can be
shared with application code, or across multiple sessions, or can even be one and the same (in
theory, it's possible to use a single 1-thread executor, although there's probably no practical
reason to do that).

To be exhaustive, `NettyOptions.getTimer()` also uses its own thread; we tried scheduling request
timeouts and speculative executions on I/O threads in early alphas, but that didn't perform as well
as Netty's `HashedWheelTimer`.

So the total number of threads created by a session is
```
advanced.netty.io-group.size + advanced.netty.admin-group.size + 1
```
