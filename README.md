# Java Driver for Scylla and Apache Cassandra®

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.scylladb/java-driver-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.scylladb/java-driver-core)

*If you're reading this on github.com, please note that this is the readme for the development 
version and that some features described here might not yet have been released. You can find the
documentation for latest version through [Java Driver Docs](https://docs.scylladb.com/using-scylla/drivers/cql-drivers/scylla-java-driver/) or via the release tags, e.g. 
[4.17.0.0](https://github.com/scylladb/java-driver/tree/4.17.0.0).*

A modern, feature-rich and highly tunable Java client library for Scylla and [Apache Cassandra®] \(2.1+),
using exclusively Cassandra's binary protocol and Cassandra Query Language (CQL) v3.

[Java Driver Docs]: https://java-driver.docs.scylladb.com/
[Scylla]: https://scylladb.com/

## Getting the driver

The driver artifacts are published in Maven central, under the group id [com.scylladb]; there
are multiple modules, all prefixed with `java-driver-`.

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>${driver.version}</version>
</dependency>

<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>java-driver-query-builder</artifactId>
  <version>${driver.version}</version>
</dependency>

<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>java-driver-mapper-runtime</artifactId>
  <version>${driver.version}</version>
</dependency>
```

Note that the query builder is now published as a separate artifact, you'll need to add the
dependency if you plan to use it.

Refer to each module's manual for more details ([core](manual/core/), [query
builder](manual/query_builder/), [mapper](manual/mapper)).

[com.scylladb]: http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.scylladb%22

## Compatibility

The driver is compatible with Scylla and Apache Cassandra® 2.1 and higher.

It requires Java 8 or higher.

## Migrating from previous versions

Java driver 4 is **not binary compatible** with previous versions. However, most of the concepts
remain unchanged, and the new API will look very familiar to 2.x and 3.x users.

See the [upgrade guide](upgrade_guide/) for details.

## Useful links

* [Manual](manual/)
* [API docs]
* Bug tracking: [JIRA]
* [Mailing list]
* Training: [Scylla University]
* [Changelog]
* [FAQ]

[API docs]: https://java-driver.docs.scylladb.com/scylla-4.17.0.x/api/overview-summary.html
[Scylla University]: https://university.scylladb.com
[Changelog]: changelog/
[FAQ]: faq/

## Training:

The course [Using Scylla Drivers](https://university.scylladb.com/courses/using-scylla-drivers/) in [Scylla University](https://university.scylladb.com/) explains how to use drivers in different languages to interact 
with a Scylla cluster. The lesson, [Coding with Java Part 1](https://university.scylladb.com/courses/using-scylla-drivers/lessons/coding-with-java-part-1/), goes over a sample application that, 
using the Java driver,  interacts with a three-node Scylla cluster. It connects to a Scylla cluster, 
displays the contents of a  table, inserts and deletes data, and shows the contents of the table after each action. 
The course also includes more advanced lessons that demonstrate working with prepares statements and datatypes. 


## License

&copy; The Apache Software Foundation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

----

Apache Cassandra, Apache, Tomcat, Lucene, Solr, Hadoop, Spark, TinkerPop, and Cassandra are 
trademarks of the [Apache Software Foundation](http://www.apache.org/) or its subsidiaries in
Canada, the United States and/or other countries. 

Binary artifacts of this product bundle Java Native Runtime libraries, which is available under the Eclipse Public License version 2.0.