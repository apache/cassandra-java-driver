## Conditions

A condition is a clause that appears after the IF keyword in a conditional [UPDATE](../update/) or
[DELETE](../delete/) statement.

The easiest way to add a condition is with an `ifXxx` method in the fluent API:

```java
deleteFrom("user")
    .whereColumn("k").isEqualTo(bindMarker())
    .ifColumn("v1").isEqualTo(literal(1))
    .ifColumn("v2").isEqualTo(literal(2));
// DELETE FROM user WHERE k=? IF v1=1 AND v2=2    
```

You can also create it manually with one of the factory methods in [Condition], and then pass it to
`if_()`:

```java
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

Condition vCondition = Condition.column("v").isEqualTo(literal(1));
deleteFrom("user")
    .whereColumn("k").isEqualTo(bindMarker())
    .if_(vCondition);
// DELETE FROM user WHERE k=? IF v=1
```

If you call `if_()` multiple times, the clauses will be joined with the AND keyword. You can also
add multiple conditions in a single call. This is a bit more efficient since it creates less
temporary objects:

```java
deleteFrom("user")
    .whereColumn("k").isEqualTo(bindMarker())
    .if_(
        Condition.column("v1").isEqualTo(literal(1)), 
        Condition.column("v2").isEqualTo(literal(2)));
// DELETE FROM user WHERE k=? IF v1=1 AND v2=2
```

Conditions are composed of a left operand, an operator, and a right-hand-side
[term](../term/).

### Simple columns

`ifColumn` operates on a single column. It supports basic arithmetic comparison operators:

| Comparison operator | Method name              |
|---------------------|--------------------------|
| `=`                 | `isEqualTo`              |
| `<`                 | `isLessThan`             |
| `<=`                | `isLessThanOrEqualTo`    |
| `>`                 | `isGreaterThan`          |
| `>=`                | `isGreaterThanOrEqualTo` |
| `!=`                | `isNotEqualTo`           |

*Note: we support `!=` because it is present in the CQL grammar but, as of Cassandra 4, it is not
implemented yet.*

In addition, `in()` can test for equality with various alternatives. You can either provide each
alternative as a term:

```java
deleteFrom("user")
    .whereColumn("k").isEqualTo(bindMarker())
    .ifColumn("v").in(bindMarker(), bindMarker(), bindMarker());
// DELETE FROM user WHERE k=? IF v IN (?,?,?)
```

Or bind the whole list of alternatives as a single variable:

```java
deleteFrom("user")
    .whereColumn("k").isEqualTo(bindMarker())
    .ifColumn("v").in(bindMarker());
// DELETE FROM user WHERE k=? IF v IN ?
```

### UDT fields

`ifField` tests a field in a top-level UDT (nested UDTs are not allowed):

```java
deleteFrom("user")
    .whereColumn("k").isEqualTo(bindMarker())
    .ifField("address", "zip").isEqualTo(literal(94040));
// DELETE FROM user WHERE k=? IF address.zip=94040
```

It supports the same set of operators as simple columns.

### Collection elements

`ifElement` tests an element in a top-level collection (nested collections are not allowed):

```java
deleteFrom("product")
    .whereColumn("sku").isEqualTo(bindMarker())
    .ifElement("features", literal("color")).in(literal("red"), literal("blue"));
// DELETE FROM product WHERE sku=? IF features['color'] IN ('red','blue')
```

It supports the same set of operators as simple columns.

### Raw snippets

You can also provide a condition as a raw CQL snippet, that will get appended to the query as-is,
without any syntax checking or escaping:

```java
deleteFrom("product")
    .whereColumn("sku").isEqualTo(bindMarker())
    .ifRaw("features['color'] IN ('red', 'blue') /*some random comment*/");
// DELETE FROM product WHERE sku=? IF features['color'] IN ('red', 'blue') /*some random comment*/
```

This should be used with caution, as it's possible to generate invalid CQL that will fail at
execution time; on the other hand, it can be used as a workaround to handle new CQL features that
are not yet covered by the query builder.

### IF EXISTS

Finally, you can specify an IF EXISTS clause:

```java
deleteFrom("product").whereColumn("sku").isEqualTo(bindMarker()).ifExists();
// DELETE FROM product WHERE sku=? IF EXISTS
```

It is mutually exclusive with column conditions: if you previously specified column conditions on
the statement, they will be ignored; conversely, adding a column condition cancels a previous IF
EXISTS clause.

[Condition]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/querybuilder/condition/Condition.html
