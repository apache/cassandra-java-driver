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

## Query idempotence

### Quick overview

A request is *idempotent* if executing it multiple times leaves the database in the same state as
executing it only once.

* `basic.request.default-idempotence` in the configuration (defaults to false).
* can be overridden per statement [Statement.setIdempotent] or [StatementBuilder.setIdempotence].
* retries and speculative executions only happen for idempotent statements.

-----

For example:

* `update my_table set list_col = [1] where pk = 1` is idempotent: no matter how many times it gets
  executed, `list_col`  will always end up with the value `[1]`;
* `update my_table set list_col = [1] + list_col where pk = 1` is not idempotent: if `list_col` was
  initially empty, it will contain `[1]` after the first execution, `[1, 1]` after the second, etc.
  
Idempotence matters because the driver sometimes re-runs requests automatically:

* [retries](../retries): if we're waiting for a response from a node and the connection gets
  dropped, the default retry policy automatically retries on another node. But we can't know what
  went wrong with the first node: maybe it went down, or maybe it was just a network issue; in any
  case, it might have applied the changes already. Therefore non-idempotent requests are never
  retried.

* [speculative executions](../speculative_execution): if they are enabled and a node takes too long
  to respond, the driver queries another node to get the response faster. But maybe both nodes will
  eventually apply the changes. Therefore non-idempotent requests are never speculatively executed.

In most cases, you need to flag your statements manually:

```java
SimpleStatement statement =
    SimpleStatement.newInstance("SELECT first_name FROM user WHERE id=1")
        .setIdempotent(true);

// Or with a builder:
SimpleStatement statement =
    SimpleStatement.builder("SELECT first_name FROM user WHERE id=1")
        .setIdempotence(true)
        .build();
```

If you don't, they default to the value defined in the [configuration](../configuration/) by the
`basic.request.default-idempotence` option; out of the box, it is set to `false`.

When you prepare a statement, its idempotence carries over to bound statements:

```java
PreparedStatement pst = session.prepare(
    SimpleStatement.newInstance("SELECT first_name FROM user WHERE id=?")
        .setIdempotent(true));
BoundStatement bs = pst.bind(1);
assert bs.isIdempotent();
```

The query builder tries to infer idempotence automatically; refer to
[its manual](../../query_builder/idempotence/) for more details.

[Statement.setIdempotent]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/Statement.html#setIdempotent-java.lang.Boolean-
[StatementBuilder.setIdempotence]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/StatementBuilder.html#setIdempotence-java.lang.Boolean-
