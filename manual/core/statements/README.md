<!--
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
-->

## Statements

### Quick overview

What you pass to `session.execute()`.

* three types: simple (textual query), bound (prepared) and batch.
* built-in implementations are **immutable**. Setters always return a new object, don't ignore the
  result.

-----

To execute a CQL query, you  create a [Statement] instance and pass it to
[Session#execute][execute] or [Session#executeAsync][executeAsync]. The driver provides various
implementations:

* [SimpleStatement](simple/): a simple implementation built directly from a character string. 
  Typically used for queries that are executed only once or a few times.
* [BoundStatement (from PreparedStatement)](prepared/): obtained by binding values to a prepared
  query. Typically used for queries that are executed often, with different values.
* [BatchStatement](batch/): a statement that groups multiple statements to be executed as a batch.

All statement types share a [common set of execution attributes][StatementBuilder], that can be set
through either setters or a builder:

* [execution profile](../configuration/) name, or the profile itself if it's been built dynamically.
* [idempotent flag](../idempotence/).
* [tracing flag](../tracing/).
* [query timestamp](../query_timestamps/).
* [page size and paging state](../paging/).
* [per-query keyspace](per_query_keyspace/) (Cassandra 4 or above).
* [token-aware routing](../load_balancing/#token-aware) information (keyspace and key/token).
* normal and serial consistency level.
* query timeout.
* custom payload to send arbitrary key/value pairs with the request (you should only need this if
  you have a custom query handler on the server).

When setting these attributes, keep in mind that statements are **immutable**, and every method
returns a different instance:

```java
SimpleStatement statement =
    SimpleStatement.newInstance("SELECT release_version FROM system.local");

// Won't work: statement isn't modified in place
statement.setConfigProfileName("oltp");
statement.setIdempotent(true);

// Instead, reassign the statement every time:
statement = statement.setConfigProfileName("oltp").setIdempotent(true);
```

All of these mutating methods are annotated with `@CheckReturnValue`. Some code analysis tools --
such as [ErrorProne](https://errorprone.info/) -- can check correct usage at build time, and report
mistakes as compiler errors.

Note that some attributes can either be set programmatically, or inherit a default value defined in
the [configuration](../configuration/). Namely, these are: idempotent flag, query timeout,
consistency levels and page size. We recommended the configuration approach whenever possible (you
can create execution profiles to capture common combinations of those options).

[Statement]:        https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/Statement.html
[StatementBuilder]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/StatementBuilder.html
[execute]:          https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/Session.html#execute-com.datastax.oss.driver.api.core.cql.Statement-
[executeAsync]:     https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/Session.html#executeAsync-com.datastax.oss.driver.api.core.cql.Statement-

```eval_rst
.. toctree::
   :hidden:
   :glob:
   
   batch/*
   per_query_keyspace/*
   prepared/*
   simple/*
```
