# Apache Cassandra® Java Driver

*If you're reading this on github.com, please note that this is the readme for the development 
version and that some features described here might not yet have been released. You can find the
documentation for latest version through [DataStax Docs] or via the release tags, e.g. 
[4.1.0](https://github.com/datastax/java-driver/tree/4.1.0).*

A modern, feature-rich and highly tunable Java client library for [Apache Cassandra®] \(2.1+) and 
[DataStax Enterprise] \(4.7+), using exclusively Cassandra's binary protocol and Cassandra Query
Language v3.

[DataStax Docs]: http://docs.datastax.com/en/developer/java-driver/
[Apache Cassandra®]: http://cassandra.apache.org/
[DataStax Enterprise]: http://www.datastax.com/products/datastax-enterprise

## Getting the driver

The driver artifacts are published in Maven central, under the group id [com.datastax.oss]; there
are multiple modules, all prefixed with `java-driver-`.

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>4.1.0</version>
</dependency>

<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-query-builder</artifactId>
  <version>4.1.0</version>
</dependency>
```

Note that the query builder is now published as a separate artifact, you'll need to add the
dependency if you plan to use it.

Refer to each module's manual for more details ([core](manual/core/), [query
builder](manual/query_builder/)).

[com.datastax.oss]: http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.datastax.oss%22

## Migrating from previous versions

Java Driver 4 is **not binary compatible** with previous versions. However, most of the concepts
remain unchanged, and the new API will look very familiar to 2.x and 3.x users.

See the [upgrade guide](upgrade_guide/) for details.

## Useful links

* [Manual](manual/)
* [API docs]
* Bug tracking: [JIRA]
* [Mailing list]
* Twitter: [@dsJavaDriver] tweets Java Driver releases and important announcements (low frequency).
    [@DataStaxEng] has more news, including other drivers, Cassandra, and DSE.
* [Changelog]
* [FAQ]

[API docs]: http://www.datastax.com/drivers/java/4.0
[JIRA]: https://datastax-oss.atlassian.net/browse/JAVA
[Mailing list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/java-driver-user
[@dsJavaDriver]: https://twitter.com/dsJavaDriver
[@DataStaxEng]: https://twitter.com/datastaxeng
[Changelog]: changelog/
[FAQ]: faq/

## License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

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
