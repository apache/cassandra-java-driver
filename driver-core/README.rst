..
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

Driver Core
===========

This is the core module of the Java Driver for Apache Cassandra (C*), 
which offers a simple (as in, not abstracted) but complete API to work with
CQL3. The main goal of the module is to handle all the functionality related
to managing connections to a Cassandra cluster (but leaving higher level
abstraction like object mapping to separate modules).


Features
--------

The features provided by the core module includes:
  - Asynchronous: the driver uses the new CQL binary protocol asynchronous
    capabilities. Only a relatively low number of connections per nodes needs to
    be maintained open to achieve good performance.
  - Nodes discovery: the driver automatically discovers and uses all nodes of the
    C* cluster, including newly bootstrapped ones.
  - Configurable load balancing: the driver allows for custom routing and load
    balancing of queries to C* nodes. Out of the box, round robin is provided
    with optional data-center awareness (only nodes from the local data-center
    are queried (and have connections maintained to)) and optional token
    awareness (that is, the ability to prefer a replica for the query as coordinator).
  - Transparent fail-over: if C* nodes fail or become unreachable, the driver
    automatically and transparently tries other nodes and schedules
    reconnection to the dead nodes in the background.
  - C* trace handling: tracing can be set on a per-query basis and the driver
    provides a convenient API to retrieve the trace.
  - Convenient schema access: the driver exposes a C* schema in a usable way.
  - Configurable retry policy: a retry policy can be set to define a precise
    behavior to adopt on query execution exceptions (for example, timeouts, 
    unavailability). This avoids polluting client code with retry-related code.


Prerequisite
------------

The driver uses the binary protocol that was introduced in Cassandra 1.2.
It only works with a version of Cassandra greater than or equal to 1.2. 
The beta2 1.2 works with the driver, but not the beta1 1.2 release.
Furthermore, the binary protocol server is not started with the default 
configuration file in Cassandra 1.2. You must edit the cassandra.yaml file:

    start_native_transport: true

If you want to run the (currently few) unit tests provided with this driver,
you will also need to have ccm installed (http://github.com/pcmanus/ccm) as the
tests use it. Also note that the first time you run the tests, ccm will
download/compile the source of C* under the hood, which may require some time
(that depends on your Internet connection or machine).

If you are having issues connecting to the cluster (seeing ``NoHostAvailableConnection`` exceptions) please check the 
`connection requirements <https://github.com/datastax/java-driver/wiki/Connection-requirements>`_.

Installing
----------

The last release of the driver is available on Maven Central. You can install
it in your application using the following Maven dependency::

    <dependency>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-core</artifactId>
      <version>1.0.8</version>
    </dependency>

**DSE users** should use the following Maven dependency (*note the version is set to 1.0.5-dse*)::

    <dependency>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-core</artifactId>
      <version>1.0.5-dse</version>
    </dependency>

**Cassandra 2.0.x users** should consider the following Maven dependency::

    <dependency>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-core</artifactId>
      <version>2.0.3</version>
    </dependency>


Getting Started
---------------

Suppose you have a Cassandra cluster running on 3 nodes whose hostnames are:
cass1, cass2 and cass3. A simple example using this core driver could be::

    Cluster cluster = Cluster.builder()
                        .addContactPoints("cass1", "cass2")
                        .build();
    Session session = cluster.connect("db1");

    for (Row row : session.execute("SELECT * FROM table1"))
        // do something ...

Please note that when we build the Cluster object, we only provide the address
to 2 Cassandra hosts. We could have provided only one host or the 3 of them,
this doesn't matter as long as the driver is able to contact one of the host
provided as "contact points". Even if only one host was provided, the driver
would use this host to discover the other ones and use the whole cluster
automatically. This is also true for new nodes joining the cluster.

For now, please refer to the API reference (http://www.datastax.com/drivers/java/apidocs/).
More informations and documentation will come later.
