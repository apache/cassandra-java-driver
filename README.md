# Java Driver for Apache Cassandra®

:warning: The java-driver has recently been donated by Datastax to The Apache Software Foundation and the Apache Cassandra project.  Bear with us as we move assets and coordinates.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.cassandra/cassandra-driver-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.cassandra/cassandra-driver-core)

*If you're reading this on github.com, please note that this is the readme
for the development version and that some features described here might
not yet have been released. You can find the documentation for the latest
version through the [Java Driver
docs](http://docs.datastax.com/en/developer/java-driver/3.11/index.html) or via the release tags,
[e.g. 3.12.1](https://github.com/apache/cassandra-java-driver/tree/3.12.1).*

A modern, [feature-rich](manual/) and highly tunable Java client
library for Apache Cassandra (2.1+) and using exclusively Cassandra's binary protocol 
and Cassandra Query Language v3. _Use the [DataStax Enterprise Java Driver][dse-driver]
for better compatibility and support for DataStax Enterprise._

**Features:**

* [Sync](manual/) and [Async](manual/async/) API
* [Simple](manual/statements/simple/), [Prepared](manual/statements/prepared/), and [Batch](manual/statements/batch/)
  statements
* Asynchronous IO, parallel execution, request pipelining
* [Connection pooling](manual/pooling/)
* Auto node discovery
* Automatic reconnection
* Configurable [load balancing](manual/load_balancing/) and [retry policies](manual/retries/)
* Works with any cluster size
* [Query builder](manual/statements/built/)
* [Object mapper](manual/object_mapper/)

The driver architecture is based on layers. At the bottom lies the driver core.
This core handles everything related to the connections to a Cassandra
cluster (for example, connection pool, discovering new nodes, etc.) and exposes a simple,
relatively low-level API on top of which higher level layers can be built.

The driver contains the following modules:

- driver-core: the core layer.
- driver-mapping: the object mapper.
- driver-extras: optional features for the Java Driver.
- driver-examples: example applications using the other modules which are
  only meant for demonstration purposes.
- driver-tests: tests for the java-driver.

**Useful links:**

- JIRA (bug tracking): https://issues.apache.org/jira/projects/CASSJAVA
- MAILING LIST: https://cassandra.apache.org/_/community.html#discussions
- DOCS: the [manual](http://docs.datastax.com/en/developer/java-driver/3.11/manual/) has quick
  start material and technical details about the driver and its features.
- API: https://docs.datastax.com/en/drivers/java/3.11
- GITHUB REPOSITORY: https://github.com/apache/cassandra-java-driver
- [changelog](changelog/)

## Getting the driver

The last release of the driver is available on Maven Central. You can install
it in your application using the following Maven dependency (_if
using DataStax Enterprise, install the [DataStax Enterprise Java Driver][dse-driver] instead_):

```xml
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>3.12.1</version>
</dependency>
```

Note that the object mapper is published as a separate artifact:

```xml
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>cassandra-driver-mapping</artifactId>
  <version>3.12.1</version>
</dependency>
```

The 'extras' module is also published as a separate artifact:

```xml
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>cassandra-driver-extras</artifactId>
  <version>3.12.1</version>
</dependency>
```


We also provide a [shaded JAR](manual/shaded_jar/)
to avoid the explicit dependency to Netty.

## Compatibility

The Java client driver 3.13.0 ([branch 3.x](https://github.com/apache/cassandra-java-driver/tree/3.x)) has been tested against Apache
Cassandra 4.0.x, 4.1.x and 5.0.x.  The Java driver supports Java 8 and above.

## Upgrading from previous versions

If you are upgrading from a previous version of the driver, be sure to have a look at
the [upgrade guide](/upgrade_guide/).


----

Apache Cassandra, Apache, Tomcat, Lucene, Solr, Hadoop, Spark, TinkerPop, and Cassandra are 
trademarks of the [Apache Software Foundation](http://www.apache.org/) or its subsidiaries in
Canada, the United States and/or other countries. 


Binary artifacts of this product bundle Java Native Runtime libraries, which is available under the Eclipse Public License version 2.0.
