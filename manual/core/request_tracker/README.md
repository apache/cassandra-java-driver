## Request tracker

The request tracker is a session-wide component that gets notified of the latency and outcome of
every application request. The driver comes with an optional implementation that logs requests.

### Configuration

The tracker is enabled in the [configuration](../configuration/). The default implementation does
nothing:

```
datastax-java-driver.advanced.request-tracker {
  class = NoopRequestTracker
}
```

To use a different tracker, specify the name of a class that implements [RequestTracker]. One such
class is the built-in request logger (see the next section), you can also create your own
implementation.

Sometimes you have a tracker instance already in your code, and need to pass it programmatically
instead of referencing a class. The session builder has a method for that:

```java
RequestTracker myTracker = ...;
CqlSession session = CqlSession.builder().withRequestTracker(myTracker).build();
```

When you provide the tracker in this manner, the configuration will be ignored.

### Request logger

The request logger is a built-in implementation that logs every request. It has many options to mark
requests as "slow" above a given threshold, limit the line size for large queries, etc:

```
datastax-java-driver.advanced.request-tracker {
  class = RequestLogger

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

Successful and slow requests use the `INFO` level:

```
INFO  c.d.o.d.i.core.tracker.RequestLogger - [s0][/127.0.0.1:9042] Success (13 ms) [1 values]
SELECT * FROM users WHERE user_id=? [v0=42]

INFO  c.d.o.d.i.core.tracker.RequestLogger - [s0][/127.0.0.1:9042] Slow (1.245 s) [1 values] SELECT
* FROM users WHERE user_id=? [v0=42]
```

Failed requests use the `ERROR` level:

```
ERROR c.d.o.d.i.core.tracker.RequestLogger - [s0][/127.0.0.1:9042] Error (179 ms) [1 values] SELECT
all FROM users WHERE user_id=? [v0=42]
com.datastax.oss.driver.api.core.servererrors.InvalidQueryException: Undefined column name all
```

[RequestTracker]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/tracker/RequestTracker.html