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

## Logging

### Quick overview

* based on SLF4J.
* config file examples for Logback and Log4J. 

**If you're looking for information about the request logger, see the [request
tracker](../request_tracker/#request-logger) page.**

-----

The driver uses [SLF4J] as a logging facade. This allows you to plug in your preferred logging
framework (java.util.logging, logback, log4j...) at deployment time.  

### Setup

To connect SLF4J to your logging framework, add a [binding] JAR in your classpath. If you use a
build tool such as Maven or Gradle, this usually involves adding a runtime dependency to your
application descriptor (`pom.xml` or `build.gradle`). For example, here is a Maven snippet for
[Logback]:

```xml
<dependency>
	<groupId>ch.qos.logback</groupId>
	<artifactId>logback-classic</artifactId>
	<version>...</version>
</dependency>
```

And the same for [Log4J]:

```xml
<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>slf4j-log4j12</artifactId>
  <version>...</version>
</dependency>
```
 
Check [SLF4J's documentation](http://www.slf4j.org/manual.html#projectDep) for examples for other
logging frameworks, and for troubleshooting dependency resolution problems.

Each logging framework has its own configuration rules, but all of them provide different levels
(DEBUG, INFO, WARN, ERROR...), different *loggers* or *categories* (messages from different
categories or loggers can be filtered out separately or printed out differently), and different
*appenders* (message receptacles such as the standard console, the error console, a file on disk, a
socket...).

Check your logging framework documentation for more information about how to properly configure it.
You can also find some configuration examples at the end of this page.

Performance tips:

* Use asynchronous appenders; both
  [Log4J](http://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/AsyncAppender.html) and
  [Logback](http://logback.qos.ch/manual/appenders.html#AsyncAppender) provide asynchronous
  appenders which reduce the impact of logging in latency-sensitive applications.
* While the driver does not provide such capability, it is possible for client applications to
  hot-reload the log configuration without stopping the application. This usually involves JMX and
  is available for [Logback](http://logback.qos.ch/manual/jmxConfig.html); Log4J provides a
  `configureAndWatch()` method but it is not recommended to use it inside J2EE containers (see
  [FAQ](https://logging.apache.org/log4j/1.2/faq.html#a3.6)).

### Taxonomy of driver logs

The driver has a well-defined use for each log level. As an application developer/administrator, you
should be focusing mostly on the `ERROR`, `WARN` and `INFO` levels.

#### ERROR

Something that renders the driver -- or a part of it -- completely unusable. An action is required
to fix it: bouncing the client, applying a patch, etc.

#### WARN

Something that the driver can recover from automatically, but indicates a configuration or
programming error that should be addressed. For example:

```
WARN  c.d.o.d.i.core.session.PoolManager - [s0] Detected a keyspace change at runtime (<none> =>
test). This is an anti-pattern that should be avoided in production (see
'request.warn-if-set-keyspace' in the configuration).

WARN  c.d.o.d.i.c.c.CqlPrepareHandlerBase - Re-preparing already prepared query. This is generally
an anti-pattern and will likely affect performance. The cached version of the PreparedStatement
will be returned, which may use different bound statement execution parameters (CL, timeout, etc.)
from the current session.prepare call. Consider preparing the statement only once. Query='...'
```
  
#### INFO
  
Something that is part of the normal operation of the driver, but might be useful to know for an
administrator. For example:

```
INFO  c.d.o.d.i.c.metadata.MetadataManager - [s0] No contact points provided, defaulting to
/127.0.0.1:9042

INFO  c.d.o.d.internal.core.time.Clock - Using native clock for microsecond precision

INFO  c.d.o.d.i.c.c.t.DefaultDriverConfigLoader - [s0] Detected a configuration change
```

#### DEBUG and TRACE

These levels are intended primarily for driver developers; we might ask you to enable them to
investigate an issue.

Keep in mind that they are quite verbose, in particular TRACE. It's a good idea to only enable them
on a limited set of categories.

### Configuration examples

#### Logback

Here is a sample configuration file for Logback.

It logs driver messages of level INFO and above, and all other libraries at level ERROR only.

The appenders send all messages of level INFO and above to the console, and all messages to a
rolling file (with the current configuration, the console and log file have the same contents, but
if you were to enable DEBUG logs for a category, those logs would go to the file but not the
console).

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
   
	<root level="ERROR">
      <appender-ref ref="async" />
	</root>
    <logger name="com.datastax.oss.driver" level= "INFO"/>
</configuration>
```

#### Log4J

Here is a sample configuration file for Log4J.

It logs driver messages of level INFO and above, and all other libraries at level ERROR only.

The appenders send all messages of level INFO and above to the console, and all messages to a
rolling file (with the current configuration, the console and log file have the same contents, but
if you were to enable DEBUG logs for a category, those logs would go to the file but not the
console).

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
  
  <root>
    <priority value="ERROR"/>
    <appender-ref ref="async"/>
  </root>
  <logger name="com.datastax.oss.driver">
    <level value="INFO"/>
  </logger>
  
</log4j:configuration>
```

[SLF4J]: https://www.slf4j.org/
[binding]: https://www.slf4j.org/manual.html#swapping
[Logback]: http://logback.qos.ch
[Log4J]: https://logging.apache.org/log4j
