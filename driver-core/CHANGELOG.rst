CHANGELOG
=========

* 1.0.0-beta2:
    - [JAVA-51] Support blob constants in the query builder.
    - [JAVA-60] Support counter batches in the query builder.
    - [JAVA-61] Basic support for custom CQL3 types.
    - [JAVA-65] Add "execution infos" for a result set (this also move the
      query trace in the new ExecutionInfos object, so users of beta1 will have
      to update).
    - [JAVA-62] Fix failover bug in DCAwareRoundRobinPolicy.
    - [JAVA-66] Fix use of bind markers for routing keys in the query builder.


* 1.0.0-beta1:
    - initial release
