CHANGELOG
=========

* 1.0.0-RC1:
  - [new] Null values are now supported in BoundStatement (but you will need at
    least Cassandra 1.2.3 for it to work). The API of BoundStatement has been
    slightly changed so that not binding a variable is not an error anymore,
    the variable is simply considered null by default. The isReady() method has
    been removed (JAVA-79).
  - [improvement] The Cluster/Session shutdown methods now properly block until
    the shutdown is complete. A version with at timeout has been added (JAVA-75).
  - [bug] Fix use of CQL3 functions in the query builder (JAVA-44).
  - [bug] Fix case where multiple schema changes too quickly wouldn't work
    (only triggered when 0.0.0.0 was use for the rpc_address on the Cassandra
    nodes) (JAVA-77).
  - [bug] Fix IllegalStateException thrown due to a reconnection made on an I/O
    thread (JAVA-72).
  - [bug] Correctly reports errors during authentication phase (JAVA-82).

* 1.0.0-beta2:
  - [new] Support blob constants, BigInteger, BigDecimal and counter batches in
    the query builder (JAVA-51, JAVA-60, JAVA-58)
  - [new] Basic support for custom CQL3 types (JAVA-61)
  - [new] Add "execution infos" for a result set (this also move the query
    trace in the new ExecutionInfos object, so users of beta1 will have to
    update) (JAVA-65)
  - [bug] Fix failover bug in DCAwareRoundRobinPolicy (JAVA-62)
  - [bug] Fix use of bind markers for routing keys in the query builder
    (JAVA-66)


* 1.0.0-beta1:
  - initial release
