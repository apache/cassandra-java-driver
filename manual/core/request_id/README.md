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

## Request Id

### Quick overview

Users can inject an identifier for each individual CQL request, and such ID can be written in to the [custom payload](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec) to 
correlate a request across the driver and the Apache Cassandra server.

A request ID generator needs to generate both:
- Session request ID: an identifier for an entire session.execute() call
- Node request ID: an identifier for the execution of a CQL statement against a particular node. There can be one or more node requests for a single session request, due to retries or speculative executions.

Usage:
* Inject ID generator: set the desired `RequestIdGenerator` in `advanced.request-id.generator.class`.
* Add ID to custom payload: the default behavior of a `RequestIdGenerator` is to add the request ID into the custom payload with the key `request-id`. Override `RequestIdGenerator.getDecoratedStatement` to customize the behavior.

### Request Id Generator Configuration

Request ID generator can be declared in the [configuration](../configuration/) as follows:

```
datastax-java-driver.advanced.request-id.generator {
  class = com.example.app.MyGenerator
}
```

To register your own request ID generator, specify the name of the class
that implements `RequestIdGenerator`.

The generated ID will be added to the log message of `CqlRequestHandler`, and propagated to other classes, e.g. the request trackers.