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

## Non-blocking programming

### Quick overview

With the advent of reactive programming, the demand for fully non-blocking libraries has become 
popular among application developers. The recent availability of frameworks enforcing lock-freedom, 
such as [Vert.x] or [Reactor], along with tools for automatic detection of blocking calls like 
[BlockHound], has exacerbated this trend even more so.

[Vert.x]: https://vertx.io
[Reactor]: https://projectreactor.io
[BlockHound]: https://github.com/reactor/BlockHound

**In summary, when used properly, the Java Driver offers non-blocking guarantees for most 
of its operations, and during most of the session lifecycle.**

These guarantees and their exceptions are detailed below. A final chapter explains how to use the 
driver with BlockHound.

The developer guide also has more information on driver internals and its 
[concurrency model](../../developer/common/concurrency).

### Definition of "non-blocking"

Since the term "non-blocking" is subject to interpretation, in this page the term should be 
understood as "[lock-free]": a program is non-blocking if at least one thread is guaranteed to make 
progress; such programs are implemented without locks, mutexes nor semaphores, using only low-level 
primitives such as atomic variables and CAS (compare-and-swap) instructions. 

A further distinction is generally established between "lock-free" and "wait-free" algorithms: the 
former ones allow progress of the overall system, while the latter ones allow each thread to make 
progress at any time. This distinction is however rather theoretical and is outside the scope of 
this document.

[lock-free]: https://www.baeldung.com/lock-free-programming

### Driver lock-free guarantees

#### Driver lock-free guarantees per execution models

The driver offers many execution models. For the built-in ones, the lock-free guarantees are as 
follows:

* The synchronous API is blocking and does not offer any lock-free guarantee.
* The [asynchronous](../async) API is implemented in lock-free algorithms.
* The [reactive](../reactive) API is implemented in lock-free algorithms (it's actually wait-free).

For example, calling any synchronous method declared in [`SyncCqlSession`], such as [`execute`], 
will block until the result is available. These methods should never be used in non-blocking 
applications.

[`SyncCqlSession`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/SyncCqlSession.html`
[`execute`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/SyncCqlSession.html#execute-com.datastax.oss.driver.api.core.cql.Statement-

However, the asynchronous methods declared in [`AsyncCqlSession`], such as [`executeAsync`], are all 
safe for use in non-blocking applications; the statement execution and asynchronous result delivery 
is guaranteed to never block. 

[`AsyncCqlSession`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/AsyncCqlSession.html
[`executeAsync`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/AsyncCqlSession.html#executeAsync-com.datastax.oss.driver.api.core.cql.Statement-

The same applies to the methods declared in [`ReactiveSession`] such as [`executeReactive`]: the 
returned publisher will never block when subscribed to, until the final results are delivered to 
the subscriber.

[`ReactiveSession`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/cql/reactive/ReactiveSession.html
[`executeReactive`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/cql/reactive/ReactiveSession.html#executeReactive-com.datastax.oss.driver.api.core.cql.Statement-

There is one exception though: continuous paging queries (a feature specific to DSE) have a special
execution model which uses internal locks for coordination. Although such locks are only held for 
extremely brief periods of time, and never under high contention, this execution model doesn't 
qualify as lock-free. 

As a consequence, all methods declared in [`ContinuousSession`] and [`ContinuousReactiveSession`] 
cannot be considered as implemented 100% lock-free, even those built on top of the asynchronous or 
reactive APIs like [`executeContinuouslyAsync`] and [`executeContinuouslyReactive`]. In practice 
though, continuous paging is extremely efficient and can safely be used in most non-blocking 
contexts, unless they require strict lock-freedom.

[`ContinuousSession`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/cql/continuous/ContinuousSession.html
[`ContinuousReactiveSession`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/cql/continuous/reactive/ContinuousReactiveSession.html
[`executeContinuouslyAsync`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/cql/continuous/ContinuousSession.html#executeContinuouslyAsync-com.datastax.oss.driver.api.core.cql.Statement-
[`executeContinuouslyReactive`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/cql/continuous/reactive/ContinuousReactiveSession.html#executeContinuouslyReactive-com.datastax.oss.driver.api.core.cql.Statement-

#### Driver lock-free guarantees per session lifecycle phases

The guarantees vary according to three possible session states: initializing, running, and closing.

Session initialization is a costly operation that performs many I/O operations, hitting both the 
local filesystem (configuration files) and the network (connection initialization). This procedure 
is triggered by a call to [`SessionBuilder.buildAsync()`] and happens partially on the calling 
thread, and partially asynchronously on an internal driver thread.

* The creation of the [driver context] happens synchronously on the calling thread. The context 
  creation usually requires file I/O, mainly to read configuration files. A call to 
  `SessionBuilder.buildAsync()`, in spite of its name, is thus a blocking call and must be 
  dispatched to a thread that is allowed to block. 
* The rest of the initialization process will happen asynchronously, on an internal driver admin
  thread. This process is mostly non-blocking, with a few exceptions listed below. Therefore,
  the driver admin thread performing the initialization tasks must be allowed to block, at least
  temporarily.

[driver context]: ../../developer/common/context

For the reasons above, the initialization phase obviously doesn't qualify as lock-free. For 
non-blocking applications, it is generally advised to trigger session initialization during 
application startup, before strong non-blocking guarantees are enforced on application threads.

Similarly, a call to [`SessionBuilder.build()`] should be considered blocking as it will block the 
calling thread and wait until the method returns. For this reason, calls to `SessionBuilder.build()` 
should be avoided in non-blocking applications.

[`SessionBuilder.buildAsync()`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/SessionBuilder.html#buildAsync--
[`SessionBuilder.build()`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/SessionBuilder.html#build--

Once the session is initialized, however, the driver is guaranteed to be non-blocking during the
session's lifecycle, and under normal operation, unless otherwise noted elsewhere in this document.

Finally, closing the session is generally non-blocking, but the driver offers no strong guarantees 
during that phase. Therefore, calls to any method declared in [`AsyncAutoCloseable`], including the 
asynchronous ones like [`closeAsync()`], should also be preferably deferred until the application is 
shut down and lock-freedom enforcement is disabled.

[`AsyncAutoCloseable`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/AsyncAutoCloseable.html
[`closeAsync()`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/AsyncAutoCloseable.html#closeAsync--

#### Driver lock-free guarantees for specific components

Certain driver components are not implemented in lock-free algorithms. 

For example, [`SafeInitNodeStateListener`] is implemented with internal locks for coordination. It 
should not be used if strict lock-freedom is enforced.

[`SafeInitNodeStateListener`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/metadata/SafeInitNodeStateListener.html

The same is valid for both built-in [request throttlers]:

* `ConcurrencyLimitingRequestThrottler`
* `RateLimitingRequestThrottler`

See the section about [throttling](../throttling) for details about these components. Again, they 
use locks internally, and depending on how many requests are being executed in parallel, the thread
contention on these locks can be high: in short, if your application enforces strict lock-freedom, 
then these components should not be used.

[request throttlers]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/throttling/RequestThrottler.html

Other components may be lock-free, *except* for their first invocation. This is the case of the 
following items:

* All built-in implementations of [`TimestampGenerator`], upon instantiation;
* The utility method [`Uuids.timeBased()`].

[`TimestampGenerator`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/time/TimestampGenerator.html
[`Uuids.timeBased()`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/uuid/Uuids.html#timeBased--

Both components need to access native libraries when they get initialized and this may involve 
hitting the local filesystem, thus causing the initialization to become a blocking call.

Timestamp generators are automatically created when the session is initialized, and are thus 
generally safe to use afterwards.

`Uuids.timeBased()`, however, is a convenience method that the driver doesn't use internally. For 
this reason, it is advised that this method be called once during application startup, so that it is 
safe to use it afterwards in a non-blocking context.

Alternatively, it's possible to disable the usage of client-side timestamp generation, and/or the
usage of native libraries. See the manual sections on [query timestamps](../query_timestamps) and 
[integration](../integration) for more information.

One component, the codec registry, can block when its [`register`] method is called; it is 
therefore advised that codecs should be registered during application startup exclusively. See the
[custom codecs](../custom_codecs) section for more details about registering codecs.

[`register`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/type/codec/registry/MutableCodecRegistry.html#register-com.datastax.oss.driver.api.core.type.codec.TypeCodec-

Finally, a few internal components also use locks, but only during session initialization; once the 
session is ready, they are either discarded, or don't use locks anymore for the rest of the 
session's lifecycle.
 
These components are safe to use once the session is ready, although they could be reported by
lock-freedom monitoring tools. They are listed below in case their exclusion is necessary:

* `com.datastax.oss.driver.internal.core.context.DefaultNettyOptions`
* `com.datastax.oss.driver.internal.core.util.concurrent.LazyReference`
* `com.datastax.oss.driver.internal.core.util.concurrent.ReplayingEventFilter`

#### Driver lock-free guarantees on topology and status events

Topology and status events can cause the driver to use locks temporarily. 

When a node gets added to the cluster, or when a node state changes (DOWN to UP or vice versa), the 
driver needs to notify a few components: the load balancing policies need to coordinate in order to 
assign a new distance to the node (LOCAL, REMOTE or IGNORED); and the node connection pool will have 
to be resized either to accommodate new connections, or to close existing ones.

These operations use internal locks for coordination. Again, they are only held for extremely brief 
periods of time, and never under high contention. Note that this behavior cannot be disabled or 
changed; if you need to enforce strict lock-freedom, and topology or status changes are being 
reported as infringements, consider adding exceptions for the following method calls:

  * `com.datastax.oss.driver.internal.core.pool.ChannelSet#add(DriverChannel)`
  * `com.datastax.oss.driver.internal.core.pool.ChannelSet#remote(DriverChannel)`
  * `com.datastax.oss.driver.internal.core.metadata.LoadBalancingPolicyWrapper$SinglePolicyDistanceReporter#setDistance(Node,NodeDistance)`

#### Driver lock-free guarantees on random uuid generation

Until driver 4.9, the [`Uuids.random()`] method was a blocking call. Because of that, this method 
could not be used in non-blocking contexts, making UUID generation a difficult issue to solve.

Moreover, this method is used in a few places internally. This situation was unfortunate because
lock-freedom enforcement tools could report calls to that method, but it was impossible to suppress
these calls. Thanks to [JAVA-2449], released with driver 4.10.0, `Uuids.random()` became a
non-blocking call and random UUIDs can now be safely generated in non-blocking applications.

[`Uuids.random()`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/uuid/Uuids.html#random--
[JAVA-2449]: https://datastax-oss.atlassian.net/browse/JAVA-2449

#### Driver lock-free guarantees when reloading the configuration

The driver has a pluggable configuration mechanism built around the [`DriverConfigLoader`] 
interface. Implementors may choose to support [hot-reloading] of configuration files, and the 
default built-in implementation has this feature enabled by default.

Beware that a hot-reloading of the default configuration mechanism is performed on a driver internal 
admin thread. If hot-reloading is enabled, then this might be reported by lock-freedom infringement 
detectors. If that is the case, it is advised to disable hot-reloading by setting the 
`datastax-java-driver.basic.config-reload-interval` option to 0. See the manual page on 
[configuration](../configuration) for more information.

[`DriverConfigLoader`]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/config/DriverConfigLoader.html
[hot-reloading]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/config/DriverConfigLoader.html#supportsReloading--

#### Driver lock-free guarantees when connecting to DSE

When connecting to clusters running recent DSE versions, the driver automatically enables periodic 
status reporting. When preparing the status report, the driver has to hit the local filesystem, and
because of that, the status reporting process does not qualify as lock-free.

If lock-freedom is being enforced, then automatic status reporting must be disabled by setting the
`datastax-java-driver.advanced.monitor-reporting.enabled` property to false in the driver 
configuration.

### Driver mechanism for detection of blocking calls

The driver has its own mechanism for detecting blocking calls happening on an internal driver 
thread. This mechanism is capable of detecting and reporting blatant cases of misuse of the 
asynchronous and reactive APIs, e.g. when the synchronous API is invoked inside a future or callback
produced by the asynchronous execution of a statement. See the core manual page on the 
[asynchronous](../async) API or the developer manual page on 
[driver concurrency](../../developer/common/concurrency) for details.

The driver is not capable, however, of detecting low-level lock-freedom infringements, such as the
usage of locks. You must use an external tool to achieve that. See below how to use BlockHound for 
that.

### Using the driver with Reactor BlockHound

[Reactor]'s tool for automatic detection of blocking calls, [BlockHound], is capable of detecting 
and reporting any sort of blocking calls, including I/O, locks, `Thread.sleep`, etc. 

When used with the driver, BlockHound can report some calls that, for the reasons explained above, 
could be safely considered as false positives. 

For this reason, the driver, since version 4.10, ships with a custom `DriverBlockHoundIntegration`
class which is automatically discovered by BlockHound through the Service Loader mechanism. It 
contains BlockHound customizations that target most of the cases detailed above, and prevent them 
from being reported as blocking calls. 

More specifically, the following items are currently declared to be allowed:

* Loading of native libraries during startup (`TimestampGenerator`);
* Locks held during startup only (`DefaultNettyOptions`, `LazyReference`, `ReplayingEventFilter`);
* Locks held during startup and topology and status events processing (`ChannelSet`, 
  `DistanceReporter`);
* Locks held when executing continuous paging queries;
* Locks held during calls to `MutableCodecRegistry.register()` and `Uuids.timeBased()`.

The following items are NOT declared to be allowed and are likely to be reported by BlockHound if
used:

* Request throttlers;
* Automatic status reporting;
* `SafeInitNodeStateListener`.

Note that other blocking startup steps, e.g. loading of configuration files, are also not declared 
to be allowed, because these are genuine blocking I/O calls. For this reason, if BlockHound is being
used, the loading of the driver context, performed by the thread calling `SessionBuilder.build()`
or `SessionBuilder.buildAsync()`, must be allowed to perform blocking calls.
