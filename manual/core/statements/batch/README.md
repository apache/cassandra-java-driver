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

## Batch statements

### Quick overview

Group a set of statements into an atomic operation.

* create with [BatchStatement.newInstance()] or [BatchStatement.builder()].
* built-in implementation is **immutable**. Setters always return a new object, don't ignore the
  result.

-----

Use [BatchStatement] to execute a set of queries as an atomic operation (refer to 
[Batching inserts, updates and deletes][batch_dse] to understand how to use batching effectively):

```java
PreparedStatement preparedInsertExpense =
    session.prepare(
        "INSERT INTO cyclist_expenses (cyclist_name, expense_id, amount, description, paid) "
            + "VALUES (:name, :id, :amount, :description, :paid)");
SimpleStatement simpleInsertBalance =
    SimpleStatement.newInstance(
        "INSERT INTO cyclist_expenses (cyclist_name, balance) VALUES (?, 0) IF NOT EXISTS",
        "Vera ADRIAN");

BatchStatement batch =
  BatchStatement.newInstance(
      DefaultBatchType.LOGGED,
      simpleInsertBalance,
      preparedInsertExpense.bind("Vera ADRIAN", 1, 7.95f, "Breakfast", false));

session.execute(batch);
```

To create a new batch statement, use one of the static factory methods (as demonstrated above), or a
builder: 

```java
BatchStatement batch =
    BatchStatement.builder(DefaultBatchType.LOGGED)
        .addStatement(simpleInsertBalance)
        .addStatement(preparedInsertExpense.bind("Vera ADRIAN", 1, 7.95f, "Breakfast", false))
        .build();
```

Keep in mind that batch statements are **immutable**, and every method returns a different instance:

```java
// Won't work: the object is not modified in place:
batch.setExecutionProfileName("oltp");

// Instead, reassign the statement every time:
batch = batch.setExecutionProfileName("oltp");
```

As shown in the examples above, batches can contain any combination of simple statements and bound 
statements. A given batch can contain at most 65536 statements. Past this limit, addition methods
throw an `IllegalStateException`.

In addition, simple statements with named parameters are currently not supported in batches (this is
due to a [protocol limitation][CASSANDRA-10246] that will be fixed in a future version). If you try
to execute such a batch, an `IllegalArgumentException` is thrown.

[BatchStatement]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/BatchStatement.html
[BatchStatement.newInstance()]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/BatchStatement.html#newInstance-com.datastax.oss.driver.api.core.cql.BatchType-
[BatchStatement.builder()]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/BatchStatement.html#builder-com.datastax.oss.driver.api.core.cql.BatchType-
[batch_dse]: http://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/useBatch.html
[CASSANDRA-10246]: https://issues.apache.org/jira/browse/CASSANDRA-10246
