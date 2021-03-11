## Query idempotence

A query is *idempotent* if it can be applied multiple times without changing the result of the initial application. For
example:

* `update my_table set list_col = [1] where pk = 1` is idempotent: no matter how many times it gets executed, `list_col`
  will always end up with the value `[1]`;
* `update my_table set list_col = [1] + list_col where pk = 1` is not idempotent: if `list_col` was initially empty,
  it will contain `[1]` after the first execution, `[1, 1]` after the second, etc.

Idempotence matters for [retries](../retries/) and [speculative query executions](../speculative_execution/). The driver
will bypass those features if the [Statement#isIdempotent()][isIdempotent] flag is set to `false`, to ensure that the
statement does not get executed more than once.

In most cases, you must set that flag manually. The driver does not parse query strings, so it can't infer it
automatically (except for statements coming from the query builder, see below).

Statements start out as non-idempotent by default. You can override the flag on each statement:

```java
Statement s = new SimpleStatement("SELECT * FROM users WHERE id = 1");
s.setIdempotent(true);
```

The default is also configurable: if you want all statements to start out as idempotent, do this:

```java
// Make all statements idempotent by default:
cluster.getConfiguration().getQueryOptions().setDefaultIdempotence(true);
```

Any statement on which you didn't call `setIdempotent` gets this default value.

Bound statements inherit the flag from the prepared statement they were created from:

```java
PreparedStatement pst = session.prepare("SELECT * FROM users WHERE id = ?");
// This cast is for backward-compatibility reasons. On 3.0+, you can do pst.setIdempotent(true) directly
((IdempotenceAwarePreparedStatement) pst).setIdempotent(true);

BoundStatement bst = pst.bind();
assert bst.isIdempotent();
```

### Idempotence in the query builder

The [QueryBuilder] DSL tries to infer the `isIdempotent` flag on the statements it generates. The following statements
will be marked **non-idempotent**:

* counter updates:

    ```java
    update("mytable").with(incr("c")).where(eq("k", 1));
    ```
* prepend, append or deletion operations on lists:

    ```java
    update("mytable").with(append("l", 1)).where(eq("k", 1));
    delete().listElt("l", 1).from("mytable").where(eq("k", 1));
    ```
* queries that insert the result of a function call or a "raw" string in a column (or as an element in a collection
  column):

    ```java
    update("mytable").with(set("v", now())).where(eq("k", 1));
    update("mytable").with(set("v", fcall("myCustomFunc"))).where(eq("k", 1));
    update("mytable").with(set("v", raw("myCustomFunc()"))).where(eq("k", 1));
    ```

    This is a conservative approach, since the driver can't guess whether a function is idempotent, or what a raw string
    contains. It might yield false negatives, that you'll have to fix manually.

* lightweight transactions (see the next section for a detailed explanation):

    ```java
    insertInto("mytable").value("k", 1).value("v", 2).ifNotExists();
    ```

If these rules produce a false negative, you can manually override the flag on the built statement:

```java
BuiltStatement s = update("mytable").with(set("v", fcall("anIdempotentFunc"))).where(eq("k", 1));

// False negative because the driver can't guess that anIdempotentFunc() is safe
assert !s.isIdempotent();

// Fix it
s.setIdempotent(true);
```


### Idempotence and lightweight transactions

As explained in the previous section, the query builder considers lightweight transactions as non-idempotent. This might
sound counter-intuitive, as these queries can sometimes be safe to execute multiple times. For example, consider the
following query:

```
UPDATE mytable SET v = 4 WHERE k = 1 IF v = 1
```

If we execute it twice, the `IF` condition will fail the second time, so the second execution will do nothing and `v`
will still have the value 4.

However, the problem appears when we consider multiple clients executing the query with retries:

1. `v` has the value 1;
2. client 1 executes the query above, performing a a CAS (compare and set) from 1 to 4;
3. client 1's connection drops, but the query completes successfully. `v` now has the value 4;
4. client 2 executes a CAS from 4 to 2;
5. client 2's transaction succeeds. `v` now has the value 2;
6. since client 1 lost its connection, it considers the query as failed, and transparently retries the CAS from 1 to 4.
   But since the column now has value 2, it receives a "not applied" response.

One important aspect of lightweight transactions is [linearizability]: given a set of concurrent operations on a column
from different clients, there must be a way to reorder them to yield a sequential history that is correct. From our
clients' point of view, there were two operations:

* client 1 executed a CAS from 1 to 4, that was not applied;
* client 2 executed a CAS from 4 to 2, that was applied.

But overall the column changed from 1 to 2. There is no ordering of the two operations that can explain that change. We
broke linearizability by doing a transparent retry at step 6.

If linearizability is important for you, you should ensure that lightweight transactions are appropriately flagged as
not idempotent.

[isIdempotent]:          https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/Statement.html#isIdempotent--
[setDefaultIdempotence]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/QueryOptions.html#setDefaultIdempotence-boolean-
[QueryBuilder]:          https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/querybuilder/QueryBuilder.html

[linearizability]: https://en.wikipedia.org/wiki/Linearizability#Definition_of_linearizability