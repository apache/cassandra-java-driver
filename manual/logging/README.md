## Logging

### Setup

DataStax Java driver uses the popular [SLF4J](http://www.slf4j.org) library to emit log messages; 
SLF4J has the advantage of providing a logging API that is entirely decoupled from concrete
implementations, letting client applications free to seamlessly connect SLF4J to their preferred logging backend.

The driver itself does not attempt to configure any concrete logging framework. 
It is up to client applications using the driver to correctly set up their classpath and 
runtime configuration to be able to capture log messages emitted by the driver.
Concretely, client applications need to provide, at runtime, a *binding* to any logging framework 
of their choice that is [compatible with SLF4J](http://www.slf4j.org/manual.html#swapping).

If your application is built with Maven, this usually involves adding one (runtime) dependency to your POM file.
For example, if you intend to use [Logback](http://logback.qos.ch), add the following dependency:

```xml
<dependency>
	<groupId>ch.qos.logback</groupId>
	<artifactId>logback-classic</artifactId>
	<version>1.2.3</version>
</dependency>
```

If you are using Log4J 1.2 instead, add the following dependency:

```xml
<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>slf4j-log4j12</artifactId>
  <version>1.7.25</version>
</dependency>
```

Check [SLF4J's documentation](http://www.slf4j.org/manual.html#projectDep) for examples for 
other logging frameworks, and for troubleshooting dependency resolution problems. 

### Configuration

Each logging framework has its own configuration rules, but all of them provide
different levels (DEBUG, INFO, WARN, ERROR...), different *loggers* or *categories*
(messages from different categories or loggers can be filtered out separately or printed out
differently), and different *appenders* (message receptacles such as the standard console,
the error console, a file on disk, a socket...).

Check your logging framework documentation for more information about how to properly configure it.
You can also find some configuration examples below.

### Useful loggers

When debugging the Java driver, the following loggers could be particularly useful
and provide hints about what's going wrong.

* `com.datastax.driver.core.Cluster`
    * ERROR
        * Authentication errors
        * Unexpected errors when handling events, reconnecting, refreshing schemas
        * Unexpected events
    * WARN
        * Cluster name mismatches
        * Unreachable contact points
        * Unsupported protocol versions 
        * Schema disagreements
        * Ignored notifications due to high contention
    * INFO
        * Hosts added or removed
    * DEBUG
        * Cluster lifecycle (start, shutdown)
        * Event delivery notifications (schema changes, topology changes)
        * Topology change events: hosts Up / Down / Added / Removed
        * Hosts being ignored
        * Protocol version negotiation
        * Reconnection attempts
        * Schema metadata refreshes
    * TRACE
        * Connection pools lifecycle (create, renew, refresh)
* `com.datastax.driver.core.Session`
    * ERROR
        * Errors related to connection pools
    * WARN
        * Problems related to connection pools
    * DEBUG
        * Connection pools lifecycle (create, renew, refresh)
* `com.datastax.driver.core.RequestHandler`
    * ERROR
        * Unexpected errors preparing queries
        * Queries sent to a bootstrapping host
        * Unexpected server errors
    * WARN
        * Queries sent to an overloaded host
    * INFO
        * Unprepared queries
    * DEBUG
        * Retry attempts
    * TRACE
        * Host currently being queried
* `com.datastax.driver.core.Connection`
    * WARN
        * Problem setting keyspace
        * Errors closing Netty channel
    * DEBUG
        * Connection lifecycle (open, close, defunct)
        * Error connecting or writing requests
        * Heartbeats
    * TRACE
        * Authentication progress
        * Sending requests
        * Receiving responses
* `com.datastax.driver.core.Message`
    * TRACE
        * Custom payloads

### Logging query latencies

The `QueryLogger` provides clients with the ability to log queries
executed by the driver, and especially, it allows client to track slow
queries, i.e. queries that take longer to complete than a configured
threshold in milliseconds.

To turn on this feature, you first need to instantiate and register a `QueryLogger` instance:

```java
Cluster cluster = ...
QueryLogger queryLogger = QueryLogger.builder()
    .withConstantThreshold(...)
    .withMaxQueryStringLength(...)
.build();
cluster.register(queryLogger);
```

Note that `QueryLogger` instances are thread-safe and can be shared cluster-wide. 
Besides, you can adjust several parameters such as the maximum query string length to be printed, 
the maximum number of parameters to print, etc. Refer to the 
`QueryLogger` [API docs][query_logger] for more information.

Secondly, you need to adjust your logging framework to accept log messages from the `QueryLogger`. The `QueryLogger`
uses 3 different loggers:

* `com.datastax.driver.core.QueryLogger.NORMAL` : Used to log normal queries, i.e., queries that completed successfully within a configurable threshold in milliseconds.
* `com.datastax.driver.core.QueryLogger.SLOW` : Used to log slow queries, i.e., queries that completed successfully but that took longer than a configurable threshold in milliseconds to complete.
* `com.datastax.driver.core.QueryLogger.ERROR`: Used to log unsuccessful queries, i.e., queries that did not complete normally and threw an exception. Note this this logger will also print the full stack trace of the reported exception.

You need to set the above loggers to DEBUG level to turn them on. E.g. to track queries
that take more than 300 ms to complete, configure your `QueryLogger` with that threshold (see above), 
then set the `com.datastax.driver.core.QueryLogger.SLOW` logger to DEBUG, e.g. with Log4J:

```xml
  <logger name="com.datastax.driver.core.QueryLogger.SLOW">
    <level value="DEBUG"/>
  </logger>
```

The `QueryLogger` would then print messages such as this for every slow query:

```
DEBUG [cluster1] [/127.0.0.1:9042] Query too slow, took 329 ms: SELECT * FROM users WHERE user_id=?;
```

As you can see, actual query parameters are not logged; if you want them to be printed as well, set the `com.datastax.driver.core.QueryLogger.SLOW` logger
to TRACE instead, e.g. with Log4J:

```xml
  <logger name="com.datastax.driver.core.QueryLogger.SLOW">
    <level value="TRACE"/>
  </logger>
```

The `QueryLogger` would then print messages such as this for every slow query:

```
TRACE [cluster1] [/127.0.0.1:9042] Query too slow, took 329 ms: SELECT * FROM users WHERE user_id=? [user_id=42];
```

Be careful when logging large query strings (specially batches) and/or queries with considerable amounts of parameters. 
See the `QueryLogger` [API docs][query_logger] for examples of how to truncate the printed message when necessary.

#### Constant vs Dynamic thresholds

Currently the `QueryLogger` can be configured to track slow queries using either 
a constant threshold in milliseconds (which is the default behavior), or 
a dynamic threshold based on per-host latency percentiles, as computed by a `PercentileTracker`.

Refer to the `QueryLogger` [API docs][query_logger] for an example of usage.

### Performance Tips

* Use asynchronous appenders; both [Log4J](http://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/AsyncAppender.html) 
and [Logback](http://logback.qos.ch/manual/appenders.html#AsyncAppender) provide asynchronous appenders 
that can significantly boost latencies when writing log messages.
* While the driver does not provide such capability, it is possible for client applications to hot-reload the log configuration
without stopping the application. This usually involves JMX and is available for [Logback](http://logback.qos.ch/manual/jmxConfig.html);
Log4J provides a `configureAndWatch()` method but it is not recommended to use it inside J2EE containers (see [FAQ](https://logging.apache.org/log4j/1.2/faq.html#a3.6)).

### Server Side Warnings

When using the driver to execute queries, it is possible that the server will generate warnings and
return them along with the results. Consider the following query:

```sql
SELECT count(*) FROM cycling.cyclist_name;
```

Executing this query would generate a warning in Cassandra:

```
Aggregation query used without partition key
```

These
[query warnings](https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/ExecutionInfo.html#getWarnings--)
are available programmatically from the
[ExecutionInfo](https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/ExecutionInfo.html)
via
[ResultSet](https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/ResultSet.html)'s
[getExecutionInfo()](https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/PagingIterable.html#getExecutionInfo--)
method. They are also logged by the driver:

```
WARN  com.datastax.driver.core.RequestHandler - Query 'SELECT count(*) FROM cycling.cyclist_name' generated server side warning(s): Aggregation query used without partition key
```

Sometimes, it is not desirable for the driver to log server-side warnings. In such cases, logging
these warnings can be disabled in the driver by setting the system property `com.datastax.driver.DISABLE_QUERY_WARNING_LOGS`
to "true". This can be done at application startup (`-Dcom.datastax.driver.DISABLE_QUERY_WARNING_LOGS=true`)
or it can be toggled programmatically in application code:

```java
// disable driver logging of server-side warnings
System.setProperty("com.datastax.driver.DISABLE_QUERY_WARNING_LOGS", "true");
....
// enable driver logging of server-side warnings
System.setProperty("com.datastax.driver.DISABLE_QUERY_WARNING_LOGS", "false");
```

### Logback Example

Here is a typical example configuration for Logback. *Please adapt it to your specific needs before using it!*

It logs messages to the console with levels equal to or higher than INFO, and logs all messages to a rolling file. 
By default, only messages with ERROR level or higher are logged.
 
It also turns on slow query tracing as described above.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
 
    <!-- log INFO or higher messages to the console -->
	<appender name="console" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%-5p %msg%n</pattern>
        </encoder>
	</appender>
 
    <!-- log everything to a rolling file -->
	<appender name="file" class="ch.qos.logback.core.rolling.RollingFileAppender">
		<file>driver.log</file>
		<encoder>
			<pattern>%-5p [%d{ISO8601}] [%t] %F:%L - %msg%n</pattern>
		</encoder>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <!-- daily rollover -->
            <fileNamePattern>driver.%d{yyyy-MM-dd}.log</fileNamePattern>
            <!-- keep 30 days' worth of history -->
            <maxHistory>30</maxHistory>
    </rollingPolicy>
    </appender>
 
    <!-- use AsyncAppender for lower latencies -->
    <appender name="async" class="ch.qos.logback.classic.AsyncAppender">
        <appender-ref ref="console" />
        <appender-ref ref="file" />
    </appender>
   
    <!--
    Turn on slow query logging by setting this logger to DEBUG; 
    set level to TRACE to also log query parameters 
    -->
    <logger name="com.datastax.driver.core.QueryLogger.SLOW" level="DEBUG" />

	<root level="ERROR">
		<appender-ref ref="async" />
	</root>
 
</configuration>
```

### Log4J Example

Here is a typical example configuration for Log4J. *Please adapt it to your specific needs before using it!*

It logs messages to the console with levels equal to or higher than INFO, and logs all messages to a rolling file. 
By default, only messages with ERROR level or higher are logged.
 
It also turns on slow query tracing as described above.

```xml
<log4j:configuration>

  <!-- log INFO or higher messages to the console -->
  <appender name="console" class="org.apache.log4j.ConsoleAppender">
    <param name="threshold" value="INFO"/>
    <layout class="org.apache.log4j.PatternLayout">
      <param name="ConversionPattern" value="%-5p %m%n"/>
    </layout>
  </appender>
  
  <!-- log everything to a rolling file -->
  <appender name="file" class="org.apache.log4j.RollingFileAppender">
    <param name="file" value="driver.log"/>
    <param name="append" value="false"/>
    <param name="maxFileSize" value="1GB"/>
    <param name="maxBackupIndex" value="10"/>
    <layout class="org.apache.log4j.PatternLayout">
      <param name="ConversionPattern" value="%-5p [%d{ISO8601}] [%t] %F:%L - %m%n"/>
    </layout>
  </appender>
  
  <!-- use AsyncAppender for lower latencies -->
  <appender name="async" class="org.apache.log4j.AsyncAppender">
    <param name="BufferSize" value="500"/>
    <appender-ref ref="file"/>
    <appender-ref ref="console"/>
  </appender>
  
  <!--
   Turn on slow query logging by setting this logger to DEBUG; 
   set level to TRACE to also log query parameters 
  -->
  <logger name="com.datastax.driver.core.QueryLogger.SLOW">
    <level value="DEBUG"/>
  </logger>
  
  <root>
    <priority value="ERROR"/>
    <appender-ref ref="async"/>
  </root>
  
</log4j:configuration>
```

[query_logger]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/QueryLogger.html
