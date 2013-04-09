CHANGELOG
=========

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
