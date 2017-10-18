# Datastax Java Driver for Apache Cassandra®

[![Build Status](https://travis-ci.org/datastax/java-driver.svg?branch=4.x)](https://travis-ci.org/datastax/java-driver)

*If you're reading this on github.com, please note that this is the readme for the development 
version and that some features described here might not yet have been released. You can find the
documentation for latest version through [DataStax Docs] or via the release tags, e.g. 
[4.0.0-alpha2](https://github.com/datastax/java-driver/tree/4.0.0-alpha2).*

A modern, feature-rich and highly tunable Java client library for [Apache Cassandra®] (2.1+) and 
[DataStax Enterprise] (4.7+), using exclusively Cassandra's binary protocol and Cassandra Query
Language v3.

[DataStax Docs]: http://docs.datastax.com/en/developer/java-driver/
[Apache Cassandra®]: http://cassandra.apache.org/
[DataStax Enterprise]: http://www.datastax.com/products/datastax-enterprise

## Getting the driver

The driver artifacts are published in Maven central, under the group id [com.datastax.oss]; there
are multiple modules, all prefixed with `java-driver-`. Refer to the [manual] for more details.

[com.datastax.oss]: http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.datastax.oss%22
[manual]: manual/

## Useful links

* [Manual][manual]
* [API docs]
* Bug tracking: [JIRA]
* [Mailing list]
* Twitter: [@dsJavaDriver] tweets Java driver releases and important announcements (low frequency).
    [@DataStaxEng] has more news, including other drivers, Cassandra, and DSE.
* [Changelog]
* [Upgrade guide]
* [FAQ]

[API docs]: http://www.datastax.com/drivers/java/4.0
[JIRA]: https://datastax-oss.atlassian.net/browse/JAVA
[Mailing list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/java-driver-user
[@dsJavaDriver]: https://twitter.com/dsJavaDriver
[@DataStaxEng]: https://twitter.com/datastaxeng
[Changelog]: changelog/
[Upgrade guide]: upgrade_guide/
[FAQ]: faq/

## License

Copyright 2017, DataStax

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

DataStax is a registered trademark of DataStax, Inc. and its subsidiaries in the United States 
and/or other countries.

Apache Cassandra, Apache, Tomcat, Lucene, Solr, Hadoop, Spark, TinkerPop, and Cassandra are 
trademarks of the [Apache Software Foundation](http://www.apache.org/) or its subsidiaries in
Canada, the United States and/or other countries. 
