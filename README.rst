DataStax Java Driver for Apache Cassandra
=========================================

A Java client driver for Apache Cassandra. This driver works exclusively with
the Cassandra Query Language version 3 (CQL3) and Cassandra's binary protocol.

- JIRA: https://datastax-oss.atlassian.net/browse/JAVA
- MAILING LIST: https://groups.google.com/a/lists.datastax.com/forum/#!forum/java-driver-user
- IRC: #datastax-drivers on `irc.freenode.net <http://freenode.net>`_
- DOCS: http://www.datastax.com/documentation/developer/java-driver/1.0/webhelp/index.html
- API: http://www.datastax.com/drivers/java/apidocs


The driver architecture is based on layers. At the bottom lies the driver core.
This core handles everything related to the connections to a Cassandra
cluster (for example, connection pool, discovering new nodes, etc.) and exposes a simple,
relatively low-level API on top of which higher level layer can be built.

The driver contains the following modules:
 - driver-core: the core layer.
 - driver-examples: example applications using the other modules which are
   only meant for demonstration purposes.

More modules including a simple object mapper will come shortly.

Please refer to the README of each module for more information.


Maven
-----

The last release of the driver is available on Maven Central. You can install
it in your application using the following Maven dependency::

    <dependency>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-core</artifactId>
      <version>1.0.3</version>
    </dependency>
    
**DSE users** should use the following Maven dependency (*note the version is set to 1.0.3-dse*)::

    <dependency>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-core</artifactId>
      <version>1.0.3-dse</version>
    </dependency>

Troubleshooting
~~~~~~~~~~~~~~~

If you are having issues connecting to the cluster (seeing ``NoHostAvailableConnection`` exceptions) please check the 
`connection requirements <https://github.com/datastax/java-driver/wiki/Connection-requirements>`_.

License
-------
Copyright 2013, DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
