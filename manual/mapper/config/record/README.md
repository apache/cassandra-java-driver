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

## Java 14 Records

Java 14 introduced [Record] as a lightweight, immutable alternative to POJOs. You can map annotated
records as entities.

We have a full example at [DataStax-Examples/object-mapper-jvm/record].

Note: records are a **preview feature** of Java 14. As such the mapper's support for them is also
provided as a preview.

### Writing the model

Annotate your records like regular classes:

```java
@Entity
record Product(@PartitionKey int id, String description) {}
```

Records are immutable and use the [fluent getter style](../../entities#getter-style), but you don't
need to declare that explicitly with [@PropertyStrategy]: the mapper detects when it's processing a
record, and will assume `mutable = false, getterStyle = FLUENT` by default.

### Building

You need to build with Java 14, and pass the `--enable-preview` flag to both the compiler and the
runtime JVM. See [pom.xml] in the example.


[@PropertyStrategy]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/mapper/annotations/PropertyStrategy.html

[DataStax-Examples/object-mapper-jvm/record]: https://github.com/DataStax-Examples/object-mapper-jvm/tree/master/record
[pom.xml]: https://github.com/DataStax-Examples/object-mapper-jvm/blob/master/record/pom.xml

[Record]: https://docs.oracle.com/en/java/javase/14/docs/api/java.base/java/lang/Record.html
