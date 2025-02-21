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

# OpenTelemetry
The driver provides support for session and node level traces using OpenTelemetry. 

## Including OpenTelemetry instrumentation in your code

You need to include the `java-driver-open-telemetry` module in your project's dependency.
```xml
<dependency>
    <groupId>org.apache.cassandra</groupId>
    <artifactId>java-driver-open-telemetry</artifactId>
</dependency>
```

You also need to instantiate an `OtelRequestTracker` and pass it to the `CqlSessionBuilder` when building the session.

```java
CqlSession session = CqlSession.builder()
    .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
    .withLocalDatacenter("datacenter1")
    .withRequestTracker(new OtelRequestTracker(initOpenTelemetry()))
    .build();
```

The constructor of `OtelRequestTracker` needs an argument of `OpenTelemetry` instance. This instance will contain the configuration for the resource and the exporter.
This is an example of how to initialize the `OpenTelemetry` instance with Jaeger exporter.

```java
static OpenTelemetry initOpenTelemetry() {

    ManagedChannel jaegerChannel =
            ManagedChannelBuilder.forAddress("localhost", 14250).usePlaintext().build();

    JaegerGrpcSpanExporter jaegerExporter =
            JaegerGrpcSpanExporter.builder()
                    .setChannel(jaegerChannel)
                    .setTimeout(30, TimeUnit.SECONDS)
                    .build();

    Resource serviceNameResource =
            Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "Demo App"));

    SdkTracerProvider tracerProvider =
            SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(jaegerExporter))
                    .setResource(Resource.getDefault().merge(serviceNameResource))
                    .build();
    OpenTelemetrySdk openTelemetry =
            OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();

    return openTelemetry;
}
```

You can also find an demo system [here](https://github.com/SiyaoIsHiding/java-driver-otel-example), which includes an Apache Cassandra database, a Spring server using the Apache Cassandra Java Driver, an HTTP client simulating browser behavior, and a Jaeger the Opentelemetry collector. 
The example demonstrates how to use OpenTelemetry to trace the queries and achieve context propagation between distributed components.

## Attributes

| Attribute                             | Description                                                                                                                                                                                   | Output Values                                                |
|---------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| db.system.name                        | Always "cassandra"                                                                                                                                                                            | "cassandra"                                                  |
| db.namespace                          | The keyspace associated with the session.                                                                                                                                                     | "mykeyspace"                                                 |
| db.operation.name                     | The name of the operation or command being executed. `Session_Request({RequestType})` for session level calls and Node_Request({RequestType}) for node level calls                            | "Node_Request(DefaultBoundStatement)"                        |
| error.type                            | Describes a class of error the operation ended with                                                                                                                                           | "NodeUnavailableException"                                   |
| server.port                           | Server port number.                                                                                                                                                                           | 9042                                                         |
| cassandra.consistency.level           | The consistency level of the query. Based on consistency values from [CQL](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html).                           | "QUORUM"                                                     |
| cassandra.coordinator.dc              | The data center of the coordinating node for a query.                                                                                                                                         | "datacenter1"                                                |
| cassandra.coordinator.id              | The ID of the coordinating node for a query.                                                                                                                                                  | "be13faa2-8574-4d71-926d-27f16cf8a7af"                       |
| cassandra.page.size                   | The fetch size used for paging, i.e. how many rows will be returned at once.                                                                                                                  | 5000                                                         |
| cassandra.query.idempotent            | Whether or not the query is idempotent.	                                                                                                                                                      | true                                                         |
| cassandra.query.id                    | The query ID to correlate with logs. `{sessionId}\|{sessionRequestId}` for a session request and `{sessionId}\|{sessionRequestId}\|{nodeRequestCount}` for a node request                     | "s0\|229540037\|0"                                           |
| cassandra.speculative_execution.count | The number of times a query was speculatively executed. Not set or 0 if the query was not executed speculatively.                                                                             | 0                                                            |
| db.operation.batch.size               | The number of queries included in the case of a batch operation.                                                                                                                              | 2                                                            |
| db.query.text                         | The database query being executed.                                                                                                                                                            | "SELECT * FROM ingredients WHERE id=? LIMIT 1 \[id='COTO'\]" |
| server.address                        | Name of the database host.                                                                                                                                                                    | "127.0.0.1"                                                  |
| db.operation.parameter.<key>          | In the case of a `BoundStatement`, this is a database operation parameter, with <key> being the parameter name, and the attribute value being a string representation of the parameter value. | "someval"                                                    |

## Apache Cassandra Internal Traces
When using `OtelRequestTracker`, If the statement has `setTracing()` enabled to turn on Apache Cassandra built-in Query Trace feature, 
the driver will retrieve the Query Trace information about the events happened in Apache Cassandra internally and export them to the OpenTelemetry collector, too.