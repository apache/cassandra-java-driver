Driver Core
===========

The core module of the Datastax Java Driver for Apache Cassandra (C*). This
module offers a simple (as in, not abstracted) but complete API to work with
CQL3. The main goal of this module is to handle all the functionality related
to managing connections to a Cassandra cluster (but leaving higher level
abstraction like object mapping to separate modules).


Features
--------

The features provided by this core module includes:
  - Asynchronous: the driver uses the new CQL binary protocol asynchronous
    capabilities. Only a relatively low number of connection per nodes needs to
    be maintained open to achieve good performance.
  - Nodes discovery: the driver automatically discover and use all nodes of the
    C* cluster, including newly bootstrapped ones.
  - Configurable load balancing: the driver allow for custom routing/load
    balancing of queries to C* nodes. Out of the box, round robin is provided
    with optional data-center awareness (only nodes from the local data-center
    are queried (and have connections maintained to)) and optional token
    awareness (i.e the ability to prefer a replica for the query as coordinator).
  - Transparent fail-over. If C* nodes fail (are not reachable), the driver
    automatically and transparently tries other nodes and schedule
    reconnection to the dead nodes in the background.
  - C* tracing handling. Tracing can be set on a per-query basis and the driver
    provides a convenient API to retrieve the trace.
  - Convenient schema access. The driver exposes the C* schema in a usable way.
  - Configurable retry policy. A retry policy can be set to define a precise
    comportment to adopt on query execution exceptions (timeouts, unavailable).
    This avoids having to litter client code with retry related code.


Prerequisite
------------

This driver uses the binary protocol that will be introduced in C* 1.2.
It will thus only work with a version of C* >= 1.2. Since at the time of this
writing C* 1.2 hasn't been released yet, at least the beta2 release needs to be
used (the beta1 is known to *not* work with this driver). Furthermore, the
binary protocol server is not started with the default configuration file
coming with Cassandra 1.2. In the cassandra.yaml file, you need to set::

    start_native_transport: true

If you want to run the (currently few) unit tests provided with this driver,
you will also need to have ccm installed (http://github.com/pcmanus/ccm) as the
tests uses it. Also note that the first time you run the tests, ccm will
download/compile the source of C* under the hood, which may require some time
(that depends on your internet connection/machine).


Installing
----------

The last release of the driver is available on Maven Central. You can install
it in your application using the following Maven dependency::

    <dependency>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-core</artifactId>
      <version>1.0.0-beta1</version>
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

For now, please refer to the JavaDoc (http://www.datastax.com/drivers/java/apidocs/)
for more informations, more documentation will come later.
