## SELECT

Start your SELECT with the `selectFrom` method in [QueryBuilder]. There are several variants
depending on whether your table name is qualified, and whether you use
[identifiers](../../case_sensitivity/) or raw strings:

```java
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

SelectFrom selectUser = selectFrom("user");
```

Note that, at this stage, the query can't be built yet. You need at least one selector.

### Selectors

A selector is something that appears after the `SELECT` keyword, and will become a column in the
result set. Its simplest form is a column identifier, but it can be a more complex expression.

The easiest way to add a selector is with one of the fluent shortcuts:

```java
selectFrom("user")
    .column("first_name")
    .column("last_name");
// SELECT first_name,last_name FROM user
```

You can also create it manually with one of the factory methods in [Selector], and then pass it to
`selector()`:

```java
selectFrom("user").selector(
    Selector.column("first_name"));
// SELECT first_name FROM user
```

If you have multiple selectors, you can also use `selectors()` to add them all in a single call.
This is a bit more efficient since it creates less temporary objects: 

```java
selectFrom("user").selectors(
    Selector.column("first_name"),
    Selector.column("last_name"));
// SELECT first_name,last_name FROM user
```

Use an alias to give a selector a different name in the result set:

```java
selectFrom("user").column("first_name").as("first");
// SELECT first_name AS first FROM user

selectFrom("user").selector(
    Selector.column("first_name").as("first"));
// SELECT first_name AS first FROM user
```

The query builder provides many kinds of selectors. Some of them only work with newer Cassandra
versions, always check what your target platform supports.

#### Star selector and count

`all` is the classic "star" selector that returns all columns. It cannot be aliased, and must be the
only selector:

```java
selectFrom("user").all();
// SELECT * FROM user

selectFrom("user").all().as("everything");
// throws IllegalStateException: Can't alias the * selector
```

If you add it to a query that already had other selectors, they will get removed:

```java
selectFrom("user").column("first_name").all();
// SELECT * FROM user
```

If you add other selectors to a query that already had the star selector, the star selector gets
removed:

```java
selectFrom("user").all().column("first_name");
// SELECT first_name FROM user
```

If you add multiple selectors at once, and one of them is the star selector, an exception is thrown: 

```java
selectFrom("user").selectors(
    Selector.column("first_name"),
    Selector.all(),
    Selector.column("last_name"));
// throws IllegalArgumentException: Can't pass the * selector to selectors()
```

`countAll` counts the number of rows:

```java
selectFrom("user").countAll();
// SELECT count(*) FROM user
```

#### Columns

We've already shown how `column` works:

```java
selectFrom("user")
    .column("first_name")
    .column("last_name");
// SELECT first_name,last_name FROM user
```

When all your selectors are simple columns, there is a convenience shortcut to add them in one call:

```java
selectFrom("user").columns("first_name", "last_name");
// SELECT first_name,last_name FROM user
```

#### Arithmetic operations

Selectors can be combined with arithmetic operations. 

| CQL Operator | Selector name |
|--------------|---------------|
| `a+b`        | `add`         |
| `a-b`        | `subtract`    |
| `-a`         | `negate`      |
| `a*b`        | `multiply`    |
| `a/b`        | `divide`      |
| `a%b`        | `remainder`   |

```java
selectFrom("rooms")
    .multiply(Selector.column("length"), Selector.column("width"))
    .as("surface");
// SELECT length*width AS surface FROM rooms
```

Operations can be nested, and will get parenthesized according to the usual precedence rules:

```java
selectFrom("foo")
    .multiply(
        Selector.negate(Selector.column("a")),
        Selector.add(Selector.column("b"), Selector.column("c")));
// SELECT -a*(b+c) FROM foo
```

Note: as shown in the examples above, arithmetic operations can get verbose very quickly. If you
have common expressions that get reused throughout your application code, consider writing your own
shortcuts:

```java
public static Selector multiplyColumns(String left, String right) {
  return Selector.multiply(Selector.column(left), Selector.column(right));
}

selectFrom("rooms")
    .selector(multiplyColumns("length", "width"))
    .as("surface");
// SELECT length*width AS surface FROM rooms
```

#### Casts

Casting is closely related to arithmetic operations; it allows you to coerce a selector to a
different data type. For example, if `height` and `weight` are two `int` columns, the following
expression uses integer division and returns an `int`:

```java
selectFrom("user")
    .divide(
        Selector.multiply(Selector.column("weight"), literal(10_000)),
        Selector.multiply(Selector.column("height"), Selector.column("height")))
    .as("bmi");
// SELECT weight*10000/(height*height) AS bmi FROM user
```

What if you want a floating-point result instead? You have to introduce a cast:

```java
selectFrom("user")
    .divide(
        Selector.multiply(
            Selector.cast(Selector.column("weight"), DataTypes.DOUBLE),
            literal(10_000)),
        Selector.multiply(Selector.column("height"), Selector.column("height")))
    .as("bmi");
// SELECT CAST(weight AS double)*10000/(height*height) AS bmi FROM user
```

Type hints are similar to casts, with a subtle difference: a cast applies to an expression with an
already well-established type, whereas a hint is used with a literal, where the type can be
ambiguous.

```java
selectFrom("foo").divide(
    // A literal 1 can be any numeric type (int, bigint, double, etc.)
    // It defaults to int, which is wrong here if we want a floating-point result.
    Selector.typeHint(literal(1), DataTypes.DOUBLE),
    Selector.column("a"));
// SELECT (double)1/a FROM foo
```

#### Sub-elements

These selectors extract an element from a complex column, for example:

* a field from a user-defined type:

  ```java
  selectFrom("user").field("address", "street");
  // SELECT address.street FROM user
  ```

* an element, or range of elements, in a set or a map:

  ```java
  selectFrom("product").element("features", literal("color"));
  // SELECT features['color'] FROM product
  
  selectFrom("movie").range("ratings", literal(3), literal(4));
  // SELECT ratings[3..4] FROM movie
  
  selectFrom("movie").range("ratings", literal(3), null);
  // SELECT ratings[3..] FROM movie
  
  selectFrom("movie").range("ratings", null, literal(3));
  // SELECT ratings[..3] FROM movie
  ```

#### Collections of selectors

Groups of selectors can be extracted as a single collection, such as:
 
* a list or set. All inner selectors must return the same CQL type:

  ```java
  selectFrom("user").listOf(
      Selector.column("first_name"),
      Selector.column("last_name"));
  // SELECT [first_name,last_name] FROM user
  
  selectFrom("user").setOf(
      Selector.column("first_name"),
      Selector.column("last_name"));
  // SELECT {first_name,last_name} FROM user
  ```

* a map. All key and value selectors must have consistent types. In most cases, Cassandra will
  require a type hint for the outer map, so the query builder can generate that for you if you
  provide the key and value types:

  ```java
  Map<Selector, Selector> mapSelector = new HashMap<>();
  mapSelector.put(literal("first"), Selector.column("first_name"));
  mapSelector.put(literal("last"), Selector.column("last_name"));
  
  selectFrom("user").mapOf(mapSelector, DataTypes.TEXT, DataTypes.TEXT);
  // SELECT (map<text,text>){'first':first_name,'last':last_name} FROM user
  ```

* a tuple. This time the types can be heterogeneous:

  ```java
  selectFrom("user").tupleOf(
      Selector.column("first_name"),
      Selector.column("birth_date"));
  // SELECT (first_name,birth_date) FROM user
  ```

#### Functions

Function calls take a function name (optionally qualified with a keyspace), and a list of selectors
that will be passed as arguments:

```java
selectFrom("user").function("utils", "bmi", Selector.column("weight"), Selector.column("height"));
// SELECT utils.bmi(weight,height) FROM user
```

The built-in functions `ttl` and `writetime` have convenience shortcuts:

```java
selectFrom("user").writeTime("first_name").ttl("last_name");
// SELECT writetime(first_name),ttl(last_name) FROM user
```

#### Literals

Occasionally, you'll need to inline a CQL literal in your query; this is not very useful as a
top-level selector, but could happen as part of an arithmetic operation:

```java
selectFrom("foo").quotient(literal(1), Selector.column("a"));
// SELECT 1/a FROM foo
```

See the [terms](../term/#literals) section for more details on literals.

#### Raw snippets

Lastly, a selector can be expressed as a raw CQL snippet, that will get appended to the query as-is,
without any syntax checking or escaping:

```java
selectFrom("user").raw("first_name, last_name /*some random comment*/");
// SELECT first_name, last_name /*some random comment*/ FROM user
```

This should be used with caution, as it's possible to generate invalid CQL that will fail at
execution time; on the other hand, it can be used as a workaround to handle new CQL features that
are not yet covered by the query builder.

### Relations

Relations get added with the `whereXxx()` methods:

```java
selectFrom("user").all().whereColumn("id").isEqualTo(literal(1));
// SELECT * FROM user WHERE id=1
```

You can also create and add them manually:

```java
selectFrom("user").all().where(
    Relation.column("id").isEqualTo(literal(1)));
// SELECT * FROM user WHERE id=1
```

Like selectors, they also have fluent shortcuts to build and add in a single call:


Relations are a common feature used by many types of statements, so they have a
[dedicated page](../relation) in this manual.

### Other clauses

The remaining SELECT clauses have a straightforward syntax. Refer to the javadocs for the fine
print.

Groupings:

```java
selectFrom("sensor_data")
    .function("max", Selector.column("reading"))
    .whereColumn("id").isEqualTo(bindMarker())
    .groupBy("date");
// SELECT max(reading) FROM sensor_data WHERE id=? GROUP BY date
```

Orderings:

```java
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;

selectFrom("sensor_data")
    .column("reading")
    .whereColumn("id").isEqualTo(bindMarker())
    .orderBy("date", ClusteringOrder.DESC);
// SELECT reading FROM sensor_data WHERE id=? ORDER BY date DESC
```

Limits:

```java
selectFrom("sensor_data")
    .column("reading")
    .whereColumn("id").isEqualTo(bindMarker())
    .limit(10);
// SELECT reading FROM sensor_data WHERE id=? LIMIT 10

selectFrom("sensor_data")
    .column("reading")
    .whereColumn("id").isEqualTo(bindMarker())
    .perPartitionLimit(bindMarker("l"));
// SELECT reading FROM sensor_data WHERE id IN ? PER PARTITION LIMIT :l
```

Filtering:

```java
selectFrom("user").all().allowFiltering();
// SELECT * FROM user ALLOW FILTERING
```

[QueryBuilder]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/querybuilder/QueryBuilder.html
[Selector]:     https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/querybuilder/select/Selector.html
