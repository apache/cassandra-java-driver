DataStax Java Driver for Apache Cassandra
=========================================

A Java client driver for Apache Cassandra. This driver works exclusively with
the Cassandra Query Language version 3 (CQL3) and Cassandra's binary protocol.

- JIRA: https://datastax-oss.atlassian.net/browse/JAVA
- MAILING LIST: https://groups.google.com/a/lists.datastax.com/forum/#!forum/java-driver-user
- IRC: #datastax-drivers on `irc.freenode.net <http://freenode.net>`_
- TWITTER: Follow the latest news about DataStax Drivers - `@olim7t <http://twitter.com/olim7t>`_, `@mfiguiere <http://twitter.com/mfiguiere>`_ 
- DOCS: http://www.datastax.com/documentation/developer/java-driver/2.1/index.html
- API: http://www.datastax.com/drivers/java/2.1
- CHANGELOG: https://github.com/datastax/java-driver/blob/2.1/driver-core/CHANGELOG.rst


The driver architecture is based on layers. At the bottom lies the driver core.
This core handles everything related to the connections to a Cassandra
cluster (for example, connection pool, discovering new nodes, etc.) and exposes a simple,
relatively low-level API on top of which higher level layer can be built.

The driver contains the following modules:
 - driver-core: the core layer.
 - driver-mapping: the object mapper.
 - driver-examples: example applications using the other modules which are
   only meant for demonstration purposes.

Please refer to the README of each module for more information.


Maven
-----

The last release of the driver is available on Maven Central. You can install
it in your application using the following Maven dependency::

    <dependency>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-core</artifactId>
      <version>2.1.4</version>
    </dependency>

Note that the object mapper is published as a separate artifact::

    <dependency>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-mapping</artifactId>
      <version>2.1.4</version>
    </dependency>

Compatibility
-------------

The Java client driver 2.1 (`branch 2.1 <https://github.com/datastax/java-driver/tree/2.1>`_) is compatible with Apache
Cassandra 1.2, 2.0 and 2.1.

UDT and tuple support is available only when using Apache Cassandra 2.1 (see `CQL improvements in Cassandra 2.1 <http://www.datastax.com/dev/blog/cql-in-2-1>`_).

Other features are available only when using Apache Cassandra 2.0 or higher (e.g. result set paging,
`BatchStatement <https://github.com/datastax/java-driver/blob/2.0/driver-core/src/main/java/com/datastax/driver/core/BatchStatement.java>`_, 
`lightweight transactions <http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_ltweight_transaction_t.html>`_ 
-- see `What's new in Cassandra 2.0 <http://www.datastax.com/documentation/cassandra/2.0/cassandra/features/features_key_c.html>`_). 
Trying to use these with a cluster running Cassandra 1.2 will result in 
an `UnsupportedFeatureException <https://github.com/datastax/java-driver/blob/2.1/driver-core/src/main/java/com/datastax/driver/core/exceptions/UnsupportedFeatureException.java>`_ being thrown.


Upgrading from previous versions
--------------------------------

If you are upgrading from the 2.0.x branch of the driver, be sure to have a look at
the `upgrade guide <https://github.com/datastax/java-driver/blob/2.1/driver-core/Upgrade_guide_to_2.1.rst>`_.

If you are upgrading from the 1.x branch, follow the `upgrade guide to 2.0 <https://github.com/datastax/java-driver/blob/2.0/driver-core/Upgrade_guide_to_2.0.rst>`_,
and then the above document.


Troubleshooting
~~~~~~~~~~~~~~~

If you are having issues connecting to the cluster (seeing ``NoHostAvailableConnection`` exceptions) please check the 
`connection requirements <https://github.com/datastax/java-driver/wiki/Connection-requirements>`_.




License
-------
Copyright 2012-2014, DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
