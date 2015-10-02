# Datastax Java Driver for Apache Cassandra

[![Build Status](https://travis-ci.org/datastax/java-driver.svg?branch=2.2)](https://travis-ci.org/datastax/java-driver)

*If you're reading this on github.com, please note that this is the readme
for the development version and that some features described here might
not yet have been released. You can find the documentation for latest
version through [Java driver
docs](http://datastax.github.io/java-driver/) or via the release tags,
[e.g.
2.2.0-rc3](https://github.com/datastax/java-driver/tree/2.2.0-rc3).*

A modern, [feature-rich](features/) and highly tunable Java client
library for Apache Cassandra (1.2+) and DataStax Enterprise (3.1+) using
exclusively Cassandra's binary protocol and Cassandra Query Language v3.

**Features:**

* [Sync][sync] and [Async][async] API
* [Simple][simple_st], [Prepared][prepared_st], and [Batch][batch_st] statements
* Asynchronous IO, parallel execution, request pipelining
* [Connection pooling][pool]
* Auto node discovery
* Automatic reconnection
* Configurable [load balancing][lbp] and [retry policies][retry_policy]
* Works with any cluster size
* [Query builder][query_builder]
* [Object mapper][mapper]


[sync]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/Session.html#execute(com.datastax.driver.core.Statement)
[async]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/Session.html#executeAsync(com.datastax.driver.core.Statement)
[simple_st]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/SimpleStatement.html
[prepared_st]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/Session.html#prepare(com.datastax.driver.core.RegularStatement)
[batch_st]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/BatchStatement.html
[pool]: features/pooling/
[lbp]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/policies/LoadBalancingPolicy.html
[retry_policy]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/policies/RetryPolicy.html
[query_builder]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/querybuilder/QueryBuilder.html
[mapper]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/mapping/MappingManager.html

The driver architecture is based on layers. At the bottom lies the driver core.
This core handles everything related to the connections to a Cassandra
cluster (for example, connection pool, discovering new nodes, etc.) and exposes a simple,
relatively low-level API on top of which higher level layers can be built.

The driver contains the following modules:

- driver-core: the core layer.
- driver-mapping: the object mapper.
- driver-examples: example applications using the other modules which are
  only meant for demonstration purposes.

**Community:**

- JIRA: https://datastax-oss.atlassian.net/browse/JAVA
- MAILING LIST: https://groups.google.com/a/lists.datastax.com/forum/#!forum/java-driver-user
- IRC: #datastax-drivers on [irc.freenode.net](http://freenode.net)
- TWITTER: [@dsJavaDriver](https://twitter.com/dsJavaDriver) tweets Java
  driver releases and important announcements (low frequency).
  [@DataStaxEng](https://twitter.com/datastaxeng) has more news including
  other drivers, Cassandra, and DSE.
- DOCS: the [user guide](http://docs.datastax.com/en/developer/java-driver/2.2/java-driver/whatsNew2.html)
  has introductory material. We are progressively migrating the doc
  [here](features/) with more technical details.
- API: http://www.datastax.com/drivers/java/2.2

**Feeback requested:** help us focus our efforts, provide your input on the [Platform and Runtime Survey](http://goo.gl/forms/qwUE6qnL7U) (we kept it short).  

## What's new in 2.2.0-rc3

* miscellaneous bug fixes on custom codecs.

See the [changelog](changelog/) for full details.

This version is **not binary compatible** with the 2.1 branch.
Refer to the [upgrade guide](upgrade_guide/) for more information.

## Maven

The last release of the driver is available on Maven Central. You can install
it in your application using the following Maven dependency:

```xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>2.2.0-rc3</version>
</dependency>
```

Note that the object mapper is published as a separate artifact:

```xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-mapping</artifactId>
  <version>2.2.0-rc3</version>
</dependency>
```

We also provide a [shaded JAR](features/shaded_jar/)
to avoid the explicit dependency to Netty.

## Compatibility

The Java client driver 2.2 ([branch 2.2](https://github.com/datastax/java-driver/tree/2.2)) is compatible with Apache
Cassandra 1.2, 2.0, 2.1 and 2.2.

UDT and tuple support is available only when using Apache Cassandra 2.1 (see [CQL improvements in Cassandra 2.1](http://www.datastax.com/dev/blog/cql-in-2-1)).

Other features are available only when using Apache Cassandra 2.0 or higher (e.g. result set paging,
[BatchStatement](https://github.com/datastax/java-driver/blob/2.1/driver-core/src/main/java/com/datastax/driver/core/BatchStatement.java), 
[lightweight transactions](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_ltweight_transaction_t.html) 
-- see [What's new in Cassandra 2.0](http://www.datastax.com/documentation/cassandra/2.0/cassandra/features/features_key_c.html)). 
Trying to use these with a cluster running Cassandra 1.2 will result in 
an [UnsupportedFeatureException](https://github.com/datastax/java-driver/blob/2.1/driver-core/src/main/java/com/datastax/driver/core/exceptions/UnsupportedFeatureException.java) being thrown.


## Upgrading from previous versions

If you are upgrading from a previous version of the driver, be sure to have a look at
the [upgrade guide](/upgrade_guide/).


### Troubleshooting

If you are having issues connecting to the cluster (seeing `NoHostAvailableConnection` exceptions) please check the 
[connection requirements](https://github.com/datastax/java-driver/wiki/Connection-requirements).


## License
Copyright 2012-2015, DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
