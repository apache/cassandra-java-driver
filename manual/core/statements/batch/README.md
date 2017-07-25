## Batch statements

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
      BatchType.LOGGED,
      simpleInsertBalance,
      preparedInsertExpense.bind("Vera ADRIAN", 1, 7.95f, "Breakfast", false));

session.execute(batch);
```

To create a new batch statement, use one of the static factory methods (as demonstrated above), or a
builder: 

```java
BatchStatement batch =
    BatchStatement.builder(BatchType.LOGGED)
        .addStatement(simpleInsertBalance)
        .addStatement(preparedInsertExpense.bind("Vera ADRIAN", 1, 7.95f, "Breakfast", false))
        .build();
```

Keep in mind that batch statements are immutable, and every method returns a different instance:

```java
// Won't work: the object is not modified in place:
batch.setConfigProfileName("oltp");

// Do this instead:
batch = batch.setConfigProfileName("oltp");
```

As shown in the examples above, batches can contain any combination of simple statements and bound 
statements. A given batch can contain at most 65536 statements. Past this limit, addition methods
throw an `IllegalStateException`.

In addition, simple statements with named parameters are currently not supported in batches (this is
due to a [protocol limitation][CASSANDRA-10246] that will be fixed in a future version). If you try
to execute such a batch, an `IllegalArgumentException` is thrown.

[BatchStatement]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/BatchStatement.html
[batch_dse]: http://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/useBatch.html
[CASSANDRA-10246]: https://issues.apache.org/jira/browse/CASSANDRA-10246