## Idempotence in the query builder

When you generate a statement (or a statement builder) from the query builder, it automatically
infers the [isIdempotent](../../core/idempotence/) flag:

```java
SimpleStatement statement =
    selectFrom("user").all()
        .whereColumn("id").isEqualTo(literal(1))
        .build();
// SELECT * FROM user WHERE id=1
assert statement.isIdempotent();
```

This can't always be determined accurately; when in doubt, the builder is pessimistic and marks the
statement as not idempotent. If you know otherwise, you can fix it manually:

```java
Delete delete =
    deleteFrom("product")
        .element("features", literal("color"))
        .whereColumn("sku").isEqualTo(bindMarker());
assert !delete.build().isIdempotent(); // see below for why
SimpleStatement statement = delete.builder()
    .withIdempotence(true)
    .build();
```

The remaining sections describe the rules that are applied to compute the flag.

### SELECT statements

SELECT statements don't modify the contents of the database. They're always considered idempotent,
regardless of the other rules below.

### Unsafe terms

If you use the result of a user-defined function in an INSERT or UPDATE statement, there is no way
of knowing if that function is idempotent:  

```java
Statement statement = insertInto("foo").value("k", function("generate_id")).build();
// INSERT INTO foo (k) VALUES (generate_id())
assert !statement.isIdempotent();
```

This extends to arithmetic operations using such terms:

```java
Statement statement =
    insertInto("foo").value("k", add(function("generate_id"), literal(1))).build();
// INSERT INTO foo (k) VALUES (generate_id()+1)
assert !statement.isIdempotent();
```

Raw terms could be anything, so they are also considered unsafe by default: 

```java
Statement statement =
    insertInto("foo").value("k", raw("generate_id()+1")).build();
// INSERT INTO foo (k) VALUES (generate_id()+1)
assert !statement.isIdempotent();
```

### Unsafe WHERE clauses

If a WHERE clause in an UPDATE or DELETE statement uses a comparison with an unsafe term, it could
potentially apply to different rows for each execution:

```java
Statement statement =
    update("foo")
        .setColumn("v", bindMarker())
        .whereColumn("k").isEqualTo(function("non_idempotent_func"))
        .build();
// UPDATE foo SET v=? WHERE k=non_idempotent_func()
assert !statement.isIdempotent();
```

### Unsafe updates

Counter updates are never idempotent:

```java
Statement statement =
    update("foo")
        .increment("c")
        .whereColumn("k").isEqualTo(bindMarker())
        .build();
// UPDATE foo SET c+=1 WHERE k=?
assert !statement.isIdempotent();
```

Nor is appending or prepending an element to a list:

```java
Statement statement =
    update("foo")
        .appendListElement("l", literal(1))
        .whereColumn("k").isEqualTo(bindMarker())
        .build();
// UPDATE foo SET l+=[1] WHERE k=?
assert !statement.isIdempotent();
```

The generic `append` and `prepend` methods apply to any kind of collection, so we have to consider
them unsafe by default too:

```java
Statement statement =
    update("foo")
        .prepend("l", literal(Arrays.asList(1, 2, 3)))
        .whereColumn("k").isEqualTo(bindMarker())
        .build();
// UPDATE foo SET l=[1,2,3]+l WHERE k=?
assert !statement.isIdempotent();
```

### Unsafe deletions

Deleting from a list is not idempotent:

```java
Statement statement =
    deleteFrom("foo")
        .element("l", literal(0))
        .whereColumn("k").isEqualTo(bindMarker())
        .build();
// DELETE l[0] FROM foo WHERE k=?
assert !statement.isIdempotent();
```

### Conditional statements

All conditional statements are considered non-idempotent:

* INSERT with IF NOT EXISTS;
* UPDATE and DELETE with IF EXISTS or IF conditions on columns.

This might seem counter-intuitive, as these queries can sometimes be safe to execute multiple times.
For example, consider the following query:

```java
update("foo")
    .setColumn("v", literal(4))
    .whereColumn("k").isEqualTo(literal(1))
    .ifColumn("v").isEqualTo(literal(1));
// UPDATE foo SET v=4 WHERE k=1 IF v=1
```

If we execute it twice, the IF condition will fail the second time, so the second execution will do
nothing and `v` will still have the value 4.

However, the problem appears when we consider multiple clients executing the query with retries:

1. `v` has the value 1;
2. client 1 executes the query above, performing a a CAS (compare and set) from 1 to 4;
3. client 1's connection drops, but the query completes successfully. `v` now has the value 4;
4. client 2 executes a CAS from 4 to 2;
5. client 2's transaction succeeds. `v` now has the value 2;
6. since client 1 lost its connection, it considers the query as failed, and transparently retries
   the CAS from 1 to 4. But since the column now has value 2, it receives a "not applied" response.

One important aspect of lightweight transactions is [linearizability]: given a set of concurrent
operations on a column from different clients, there must be a way to reorder them to yield a
sequential history that is correct. From our clients' point of view, there were two operations:

* client 1 executed a CAS from 1 to 4, that was not applied;
* client 2 executed a CAS from 4 to 2, that was applied.

But overall the column changed from 1 to 2. There is no ordering of the two operations that can
explain that change. We broke linearizability by doing a transparent retry at step 6.

[linearizability]: https://en.wikipedia.org/wiki/Linearizability#Definition_of_linearizability