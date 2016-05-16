# Datastax Java Driver for Apache Cassandra

![Travis Build](https://travis-ci.org/datastax/java-driver.svg?branch=2.0)

*If you're reading this on github.com, please note that this is the readme
for the development version and that some features described here might
not yet have been released. You can find the documentation for latest
version through [Java driver
docs](http://datastax.github.io/java-driver/) or via the release tags,
[e.g.
2.0.12.2](https://github.com/datastax/java-driver/tree/2.0.12.2).*

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

[sync]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Session.html#execute(com.datastax.driver.core.Statement)
[async]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Session.html#executeAsync(com.datastax.driver.core.Statement)
[simple_st]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/SimpleStatement.html
[prepared_st]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Session.html#prepare(com.datastax.driver.core.RegularStatement)
[batch_st]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/BatchStatement.html
[pool]: features/pooling/
[lbp]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/policies/LoadBalancingPolicy.html
[retry_policy]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/policies/RetryPolicy.html
[query_builder]: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/querybuilder/QueryBuilder.html

The driver architecture is based on layers. At the bottom lies the driver core.
This core handles everything related to the connections to a Cassandra
cluster (for example, connection pool, discovering new nodes, etc.) and exposes a simple,
relatively low-level API on top of which higher level layers can be built.

The driver contains the following modules:

- driver-core: the core layer.
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
- DOCS: the [user guide](http://docs.datastax.com/en/developer/java-driver/2.0/java-driver/whatsNew2.html)
  has introductory material. We are progressively migrating the doc
  [here](features/) with more technical details.
- API: http://docs.datastax.com/en/drivers/java/2.0

**Feeback requested:** help us focus our efforts, provide your input on the [Platform and Runtime Survey](http://goo.gl/forms/qwUE6qnL7U) (we kept it short).


## Getting the driver

The latest release of the driver is available on Maven Central. You can install
it in your application using the following Maven dependency:

```xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>2.0.12.2</version>
</dependency>
```

We also provide a [shaded JAR](http://datastax.github.io/java-driver/features/shaded_jar/)
to avoid the explicit dependency to Netty.

If you can't use a dependency management tool, a
[binary tarball](http://downloads.datastax.com/java-driver/cassandra-java-driver-2.0.12.2.tar.gz)
is available for download.

## Compatibility

The Java client driver 2.0 ([branch 2.0](https://github.com/datastax/java-driver/tree/2.0)) is compatible with Apache
Cassandra 1.2 and 2.0, but some features are available only when using Apache Cassandra 2.0 (e.g. result set paging,
[BatchStatement](https://github.com/datastax/java-driver/blob/2.0/driver-core/src/main/java/com/datastax/driver/core/BatchStatement.java), 
[lightweight transactions](http://docs.datastax.com/en/cql/3.1/cql/cql_using/use_ltweight_transaction_t.html)
-- see [What's new in Cassandra 2.0](http://docs.datastax.com/en/cassandra/2.0/cassandra/features/features_key_c.html)).
Trying to use these with a cluster running Cassandra 1.2 will result in 
an [UnsupportedFeatureException](https://github.com/datastax/java-driver/blob/2.0/driver-core/src/main/java/com/datastax/driver/core/exceptions/UnsupportedFeatureException.java) being thrown.

## Upgrading from 1.x branch

If you are upgrading from the 1.x branch of the driver, be sure to have a look at
the [upgrade guide](upgrade_guide/).

We used the opportunity of a major version bump to incorporate your feedback and improve the API, 
to fix a number of inconsistencies and remove cruft. 
Unfortunately this means there are some breaking changes, but the new API should be both simpler and more complete.

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
