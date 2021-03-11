## Batch statements

Use [BatchStatement] to execute a set of queries as a single operation (refer to
[Batching inserts, updates and deletes][batch_dse] to understand how to use batching effectively):

```java
PreparedStatement preparedInsertExpense =
    session.prepare(
        "INSERT INTO cyclist_expenses (cyclist_name, expense_id, amount, description, paid) "
            + "VALUES (:name, :id, :amount, :description, :paid)");
SimpleStatement simpleInsertBalance =
    new SimpleStatement("INSERT INTO cyclist_expenses (cyclist_name, balance) VALUES (?, 0) IF NOT EXISTS",
        "Vera ADRIAN");

BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
    .add(simpleInsertBalance)
    .add(preparedInsertExpense.bind("Vera ADRIAN", 1, 7.95f, "Breakfast", false));

session.execute(batch);
```

As shown in the examples above, batches can contain any combination of simple statements and bound
statements. A given batch can contain at most 65536 statements. Past this limit, addition methods
throw an `IllegalStateException`.

By default, batches are configured as [LOGGED]. This ensures that if any statement in the batch
succeeds, all will eventually succeed.  Ensuring all queries in a batch succeed has a 
performance cost.  Consider using [UNLOGGED] as shown above if you do not need this capability.

Please note that the size of a batch is subject to the [batch_size_fail_threshold] configuration
option on the server.

In addition, simple statements with named parameters are currently not supported in batches (this is
due to a [protocol limitation][CASSANDRA-10246] that will be fixed in a future version). If you try
to execute such a batch, an `IllegalArgumentException` is thrown.

[BatchStatement]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/BatchStatement.html
[batch_dse]: http://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/useBatch.html
[LOGGED]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/BatchStatement.Type.html#LOGGED
[UNLOGGED]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/BatchStatement.Type.html#UNLOGGED
[batch_size_fail_threshold]: https://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html#configCassandra_yaml__batch_size_fail_threshold_in_kb
[CASSANDRA-10246]: https://issues.apache.org/jira/browse/CASSANDRA-10246
