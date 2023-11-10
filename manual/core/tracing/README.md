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

## Query tracing

### Quick overview

Detailed information about the server-side internals for a given query.

* disabled by default, must enable per statement with [Statement.setTracing()] or
  [StatementBuilder.setTracing()].
* retrieve with [ResultSet.getExecutionInfo().getTracingId()][ExecutionInfo.getTracingId()] and
  [getQueryTrace()][ExecutionInfo.getQueryTrace()].
* `advanced.request.trace` in the configuration: fine-grained control over how the driver fetches
  the trace data.

-----

To help troubleshooting performance, Cassandra offers the ability to *trace* a query, in other words
capture detailed information about the the internal operations performed by all nodes in the cluster
in order to build the response.

The driver provides a way to enable tracing on a particular statement, and an API to examine the
results. 

### Enabling tracing

Set the tracing flag on the `Statement` instance. There are various ways depending on how you build
it (see [statements](../statements/) for more details):

```java
// Setter-based:
Statement statement =
  SimpleStatement.newInstance("SELECT * FROM users WHERE id = 1234").setTracing(true);

// Builder-based:
Statement statement =
  SimpleStatement.builder("SELECT * FROM users WHERE id = 1234").setTracing().build();
```

Tracing is supposed to be run on a small percentage of requests only. Do not enable it on every
request, you would risk overwhelming your cluster.

### Retrieving tracing data

Once you've executed a statement with tracing enabled, tracing data is available through the
[ExecutionInfo]:

```java
ResultSet rs = session.execute(statement);
ExecutionInfo executionInfo = rs.getExecutionInfo();
```

#### Tracing id

Cassandra assigns a unique identifier to each query trace. It is returned with the query results,
and therefore available immediately: 

```java
UUID tracingId = executionInfo.getTracingId();
```

This is the primary key in the `system_traces.sessions` and `system_traces.events` tables where
Cassandra stores tracing data (you don't need to query those tables manually, see the next section).

If you call `getTracingId()` for a statement that didn't have tracing enabled, the resulting id will
be `null`. 

#### Tracing information

To get to the details of the trace, retrieve the [QueryTrace] instance:

```java
QueryTrace trace = executionInfo.getQueryTrace();

// Or asynchronous equivalent:
CompletionStage<QueryTrace> traceFuture = executionInfo.getQueryTraceAsync();
```

This triggers background queries to fetch the information from the `system_traces.sessions` and
`system_traces.events` tables. Because Cassandra writes that information asynchronously, it might
not be immediately available, therefore the driver will retry a few times if necessary. You can
control this behavior through the configuration:

```
# These options can be changed at runtime, the new values will be used for requests issued after
# the change. They can be overridden in a profile.
datastax-java-driver.advanced.request.trace {
  # How many times the driver will attempt to fetch the query if it is not ready yet.
  attempts = 5
  
  # The interval between each attempt.
  interval = 3 milliseconds
  
  # The consistency level to use for trace queries.
  # Note that the default replication strategy for the system_traces keyspace is SimpleStrategy
  # with RF=2, therefore LOCAL_ONE might not work if the local DC has no replicas for a given
  # trace id.
  consistency = ONE
}
```

Once you have the `QueryTrace` object, access its properties for relevant information, for example:

```java
System.out.printf(
    "'%s' to %s took %dμs%n",
    trace.getRequestType(), trace.getCoordinator(), trace.getDurationMicros());
for (TraceEvent event : trace.getEvents()) {
  System.out.printf(
      "  %d - %s - %s%n",
      event.getSourceElapsedMicros(), event.getSource(), event.getActivity());
}
```

If you call `getQueryTrace()` for a statement that didn't have tracing enabled, an exception is
thrown.

[ExecutionInfo]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/ExecutionInfo.html
[QueryTrace]:    https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/QueryTrace.html
[Statement.setTracing()]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/Statement.html#setTracing-boolean-
[StatementBuilder.setTracing()]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/StatementBuilder.html#setTracing--
[ExecutionInfo.getTracingId()]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/ExecutionInfo.html#getTracingId--
[ExecutionInfo.getQueryTrace()]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/ExecutionInfo.html#getQueryTrace--
