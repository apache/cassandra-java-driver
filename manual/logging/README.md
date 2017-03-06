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
	<version>1.1.3</version>
</dependency>
```

If you are using Log4J 1.2 instead, add the following dependency:

```xml
<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>slf4j-log4j12</artifactId>
  <version>1.7.12</version>
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

The `EnhancedQueryLogger` class provides clients with the ability to log queries
executed by the driver, and especially, it allows clients to track slow
queries, i.e. queries that take longer to complete than a configured
threshold in milliseconds.

To turn on this feature, you first need to instantiate and register an `EnhancedQueryLogger` instance:

```java
Cluster cluster = ...
EnhancedQueryLogger queryLogger = EnhancedQueryLogger.builder()
    .withConstantThreshold(...)
.build();
cluster.register(queryLogger);
```

Note that `EnhancedQueryLogger` instances are thread-safe and can be shared cluster-wide. 
Besides, you can adjust several parameters. Refer to the 
`EnhancedQueryLogger` [API docs][query_logger] for more information.

Secondly, you need to adjust your logging framework to accept log messages from the `EnhancedQueryLogger`. The `EnhancedQueryLogger`
uses 3 different loggers:

* `com.datastax.driver.core.QueryLogger.NORMAL` : Used to log normal queries, i.e., queries that completed successfully within a configurable threshold in milliseconds.
* `com.datastax.driver.core.QueryLogger.SLOW` : Used to log slow queries, i.e., queries that completed successfully but that took longer than a configurable threshold in milliseconds to complete.
* `com.datastax.driver.core.QueryLogger.ERROR`: Used to log unsuccessful queries, i.e., queries that did not complete normally and threw an exception. Note this this logger will also print the full stack trace of the reported exception.

You need to set the above loggers to DEBUG level to turn them on. E.g. to track queries
that take more than 300 ms to complete, configure your `EnhancedQueryLogger` with that threshold (see above), 
then set the `com.datastax.driver.core.QueryLogger.SLOW` logger to DEBUG, e.g. with Log4J:

```xml
  <logger name="com.datastax.driver.core.QueryLogger.SLOW">
    <level value="DEBUG"/>
  </logger>
```

The `EnhancedQueryLogger` would then print messages such as this for every slow query:

```
DEBUG [cluster1] [/127.0.0.1:9042] Query too slow, took 329 ms: BoundStatement@2a929abf [1 values]: SELECT * FROM users WHERE user_id=?;
```

As you can see, the query string is logged but actual bound values are not; if you want them to be logged as well, 
set the `com.datastax.driver.core.QueryLogger.SLOW` logger to TRACE instead, e.g. with Log4J:

```xml
  <logger name="com.datastax.driver.core.QueryLogger.SLOW">
    <level value="TRACE"/>
  </logger>
```

At this level, the `EnhancedQueryLogger` prints statements with maximum verbosity; 
it would then print messages such as this for every slow query:

```
TRACE [cluster1] [/127.0.0.1:9042] Query too slow, took 329 ms: BoundStatement@34d2ff60 [1 values]: SELECT * FROM users WHERE user_id=? { user_id : 42 }

```

#### Formatting statements

The `EnhancedQueryLogger` uses another component, namely the `StatementFormatter` class, 
to actually format and print executed statements.
 
`StatementFormatter` can format statements with different levels of verbosity, 
which in turn determines which elements to include in the formatted string 
(query string, bound values, custom payloads, inner statements for batches, etc.). 

`StatementFormatter` also provides safeguards to prevent overwhelming your logs with large query strings, 
queries with considerable amounts of parameters, batch queries with several inner statements, etc. 

With default settings, the `StatementFormatter` formats statements using a
common formatting pattern comprised of the following sections:

1. The actual statement class and the statement's hash code;
2. If verbosity is NORMAL or higher, a summary consisting of some of the statement's characteristics such as: 
    1. The number of bound values, if known.
    2. The number of inner statements, if known (only applicable for batch statements).
    3. Idempotence flag ("IDP"), if set and verbosity is EXTENDED. 
    4. Consistency level ("CL"), if set and verbosity is EXTENDED.
    5. Serial consistency level ("SCL"), if set and verbosity is EXTENDED.
    6. Default timestamp ("DTS"), if set and verbosity is EXTENDED.
    7. Read timeout in milliseconds ("RTM"), if set and verbosity is EXTENDED.
2. The statement's query string, if available and the verbosity is NORMAL or higher.
3. The statement's bound values, if available and the verbosity is EXTENDED.
4. The statement's outgoing payload, if available and the verbosity is EXTENDED.

Here is an example of how the same statements would be printed in different verbosity levels:

```java
Session session = ...;
StatementFormatter formatter = StatementFormatter.DEFAULT_INSTANCE;

// 1) regular statements
Statement stmt1 = new SimpleStatement("INSERT INTO t (c1, c2, c3) VALUES (?, ?, ?)", "foo", 42, null)
        .setIdempotent(true)
        .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
        .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
        .setDefaultTimestamp(new Date().getTime())
        .setReadTimeoutMillis(20000);

// 2) query builder statements
Statement stmt2 = insertInto("t").value("c1", "foo").value("c2", 42).value("c3", false);

// 3) bound statements
Statement stmt3 = session.prepare("INSERT INTO t (c1, c2, c3) VALUES (?, ?, ?)").bind("foo", null /*, unset */);

// 4) batch statements
BatchStatement stmt4 = new BatchStatement(BatchStatement.Type.UNLOGGED)
        .add(stmt1)
        .add(stmt2)
        .add(stmt3);
for (Statement stmt : Arrays.asList(stmt1, stmt2, stmt3, stmt4)) {
    System.out.println(formatter.format(stmt, ABRIDGED, version, codecRegistry));
    System.out.println(formatter.format(stmt, NORMAL, version, codecRegistry));
    System.out.println(formatter.format(stmt, EXTENDED, version, codecRegistry));
    System.out.println();
}
```

The above would produce an output similar to the one below:

```
SimpleStatement@258e2e41 [3 values]
SimpleStatement@258e2e41 [3 values]: INSERT INTO t (c1, c2, c3) VALUES (?, ?, ?)
SimpleStatement@258e2e41 [3 values, IDP : true, CL : LOCAL_ONE, SCL : LOCAL_SERIAL, DTS : 1487865254945, RTM : 20000]: INSERT INTO t (c1, c2, c3) VALUES (?, ?, ?) { 0 : 'foo', 1 : 42, 2 : <NULL> }

BuiltStatement@4fe767f3 [2 values]
BuiltStatement@4fe767f3 [2 values]: INSERT INTO t (c1,c2,c3) VALUES (?,42,?);
BuiltStatement@4fe767f3 [2 values, IDP : true]: INSERT INTO t (c1,c2,c3) VALUES (?,42,?); { 0 : 'foo', 1 : false }

BoundStatement@2805c96b [3 values]
BoundStatement@2805c96b [3 values]: INSERT INTO t (c1, c2, c3) VALUES (?, ?, ?)
BoundStatement@2805c96b [3 values, IDP : false]: INSERT INTO t (c1, c2, c3) VALUES (?, ?, ?) { c1 : 'foo', c2 : <NULL>, c3 : <?> }

BatchStatement@184cf7cf [UNLOGGED, 3 stmts]
BatchStatement@184cf7cf [UNLOGGED, 3 stmts] 1 : SimpleStatement@258e2e41 [3 values]: INSERT INTO t (c1, c2, c3) VALUES (?, ?, ?), 2 : BuiltStatement@4fe767f3 [2 values]: INSERT INTO t (c1,c2,c3) VALUES (?,42,?);, 3 : BoundStatement@2805c96b [3 values]: INSERT INTO t (c1, c2, c3) VALUES (?, ?, ?)
BatchStatement@184cf7cf [UNLOGGED, 3 stmts, 8 values] 1 : SimpleStatement@258e2e41 [3 values, IDP : true, CL : LOCAL_ONE, SCL : LOCAL_SERIAL, DTS : 1487865254945, RTM : 20000]: INSERT INTO t (c1, c2, c3) VALUES (?, ?, ?) { 0 : 'foo', 1 : 42, 2 : <NULL> }, 2 : BuiltStatement@4fe767f3 [2 values, IDP : true]: INSERT INTO t (c1,c2,c3) VALUES (?,42,?); { 0 : 'foo', 1 : false }, 3 : BoundStatement@2805c96b [3 values, IDP : false]: INSERT INTO t (c1, c2, c3) VALUES (?, ?, ?) { c1 : 'foo', c2 : <NULL>, c3 : <?> }
```

Note that null values are printed as `<NULL>` and unset values are printed as `<?>`.

And finally, `StatementFormatter` also allows you to implement your own formatting rules, should you need full control
over what needs to be formatted and how.

See the `StatementFormatter` [API docs][statement_formatter] for guidelines about how to customize `StatementFormatter`.

#### Constant vs Dynamic thresholds

Currently the `EnhancedQueryLogger` can be configured to track slow queries using either 
a constant threshold in milliseconds (which is the default behavior), or 
a dynamic threshold based on per-host latency percentiles, as computed by `PerHostPercentileTracker`.

Refer to the `EnhancedQueryLogger` [API docs][query_logger] for an example of usage.

### Performance Tips

* Use asynchronous appenders; both [Log4J](http://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/AsyncAppender.html) 
and [Logback](http://logback.qos.ch/manual/appenders.html#AsyncAppender) provide asynchronous appenders 
that can significantly boost latencies when writing log messages.
* While the driver does not provide such capability, it is possible for client applications to hot-reload the log configuration
without stopping the application. This usually involves JMX and is available for [Logback](http://logback.qos.ch/manual/jmxConfig.html);
Log4J provides a `configureAndWatch()` method but it is not recommended to use it inside J2EE containers (see [FAQ](https://logging.apache.org/log4j/1.2/faq.html#a3.6)).

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

[query_logger]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/EnhancedQueryLogger.html
[statement_formatter]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/StatementFormatter.html
