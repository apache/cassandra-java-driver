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

## Request tracker

### Quick overview

Callback that gets invoked for every request: success or error, globally and for every tried node.

* `advanced.request-tracker` in the configuration; defaults to none, also available: request logger,
  or write your own.
* or programmatically:
  [CqlSession.builder().addRequestTracker()][SessionBuilder.addRequestTracker].

-----

The request tracker is a session-wide component that gets notified of the latency and outcome of
every application request. The driver comes with an optional implementation that logs requests.

### Configuration

Request trackers can be declared in the [configuration](../configuration/) as follows:

```
datastax-java-driver.advanced.request-tracker {
  classes = [com.example.app.MyTracker1,com.example.app.MyTracker2]
}
```

By default, no tracker is registered. To register your own trackers, specify the name of a class
that implements [RequestTracker]. One such class is the built-in request logger (see the next
section), but you can also create your own implementation.

Also, trackers registered via configuration will be instantiated with reflection; they must have a
public constructor taking a `DriverContext` argument.

Sometimes you have a tracker instance already in your code, and need to pass it programmatically
instead of referencing a class. The session builder has a method for that:

```java
RequestTracker myTracker1 = ...;
RequestTracker myTracker2 = ...;
CqlSession session = CqlSession.builder()
        .addRequestTracker(myTracker1)
        .addRequestTracker(myTracker2)
        .build();
```

The two registration methods (programmatic and via the configuration) can be used simultaneously.

### Request logger

The request logger is a built-in implementation that logs every request. It has many options to mark
requests as "slow" above a given threshold, limit the line size for large queries, etc:

```
datastax-java-driver.advanced.request-tracker {
  classes = [RequestLogger]

  logs {
    # Whether to log successful requests.
    success.enabled = true

    slow {
      # The threshold to classify a successful request as "slow". If this is unset, all
      # successful requests will be considered as normal.
      threshold = 1 second

      # Whether to log slow requests.
      enabled = true
    }

    # Whether to log failed requests.
    error.enabled = true

    # The maximum length of the query string in the log message. If it is longer than that, it
    # will be truncated.
    max-query-length = 500

    # Whether to log bound values in addition to the query string.
    show-values = true

    # The maximum length for bound values in the log message. If the formatted representation of
    # a value is longer than that, it will be truncated.
    max-value-length = 50

    # The maximum number of bound values to log. If a request has more values, the list of
    # values will be truncated.
    max-values = 50

    # Whether to log stack traces for failed queries. If this is disabled, the log will just
    # include the exception's string representation (generally the class name and message).
    show-stack-traces = true
}
```

All requests are logged under the category
`com.datastax.oss.driver.internal.core.tracker.RequestLogger`.

The prefix of the log will always contain at least: 

```
s0|274426173
```

Where `s0` is the session name (see the `basic.session-name` configuration option), and `274426173`
is a unique hash code calculated per request, that can be used for correlation with the driver's
debug and trace logs.


Successful and slow requests use the `INFO` level:

```
INFO  c.d.o.d.i.core.tracker.RequestLogger - [s0|274426173][/127.0.0.1:9042] Success (13 ms) [1 values]
SELECT * FROM users WHERE user_id=? [v0=42]

INFO  c.d.o.d.i.core.tracker.RequestLogger - [s0|1883237069][/127.0.0.1:9042] Slow (1.245 s) [1 values] SELECT
* FROM users WHERE user_id=? [v0=42]
```

Failed requests use the `ERROR` level:

```
ERROR c.d.o.d.i.core.tracker.RequestLogger - [s0|1883237069][/127.0.0.1:9042] Error (179 ms) [1 values] SELECT
all FROM users WHERE user_id=? [v0=42]
com.datastax.oss.driver.api.core.servererrors.InvalidQueryException: Undefined column name all
```

[RequestTracker]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/tracker/RequestTracker.html
[SessionBuilder.addRequestTracker]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/SessionBuilder.html#addRequestTracker-com.datastax.oss.driver.api.core.tracker.RequestTracker-
