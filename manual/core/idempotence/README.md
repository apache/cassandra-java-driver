## Query idempotence

A request is *idempotent* if executing it multiple times leaves the database in the same state as
executing it only once.

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
`request.default-idempotence` option; out of the box, it is set to `false`.

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
