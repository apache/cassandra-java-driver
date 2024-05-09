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

## Relations

A relation is a clause that appears after the WHERE keyword, and restricts the rows that the
statement operates on.

Relations are used by the following statements:

* [SELECT](../select/) 
* [UPDATE](../update/)
* [DELETE](../delete/)
* [CREATE MATERIALIZED VIEW](../schema/materialized_view/)

The easiest way to add a relation is with a `whereXxx` method in the fluent API:

```java
selectFrom("sensor_data").all()
    .whereColumn("id").isEqualTo(bindMarker())
    .whereColumn("date").isGreaterThan(bindMarker());
// SELECT * FROM sensor_data WHERE id=? AND date>?
```

You can also create it manually with one of the factory methods in [Relation], and then pass it to
`where()`:

```java
selectFrom("user").all().where(
    Relation.column("id").isEqualTo(bindMarker()));
// SELECT * FROM user WHERE id=?
```

If you call `where()` multiple times, the clauses will be joined with the AND keyword. You can also
add multiple relations in a single call. This is a bit more efficient since it creates less
temporary objects: 

```java
selectFrom("sensor_data").all()
    .where(
        Relation.column("id").isEqualTo(bindMarker()),
        Relation.column("date").isGreaterThan(bindMarker()));
// SELECT * FROM sensor_data WHERE id=? AND date>?
```

Relations are generally composed of a left operand, an operator, and an optional right-hand-side
[term](../term/). The type of relation determines which operators are available.
 
### Simple columns

`whereColumn` operates on a single column. It supports basic arithmetic comparison operators:

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

See above for comparison operator examples.

If you're using SASI indices, you can also use `like()` for wildcard comparisons:

```java
selectFrom("user").all().whereColumn("last_name").like(literal("M%"));
// SELECT * FROM user WHERE last_name LIKE 'M%'
```

`in()` is like `isEqualTo()`, but with various alternatives. You can either provide each alternative as a
term:

```java
selectFrom("user").all().whereColumn("id").in(literal(1), literal(2), literal(3));
// SELECT * FROM user WHERE id IN (1,2,3)

selectFrom("user").all().whereColumn("id").in(bindMarker(), bindMarker(), bindMarker());
// SELECT * FROM user WHERE id IN (?,?,?)
```

Or bind the whole list of alternatives as a single variable:

```java
selectFrom("user").all().whereColumn("id").in(bindMarker());
// SELECT * FROM user WHERE id IN ?
```

For collection columns, you can check for the presence of an element with `contains()` and
`containsKey()`:

```java
selectFrom("sensor_data")
    .all()
    .whereColumn("id").isEqualTo(bindMarker())
    .whereColumn("date").isEqualTo(bindMarker())
    .whereColumn("readings").containsKey(literal("temperature"))
    .allowFiltering();
// SELECT * FROM sensor_data WHERE id=? AND date=? AND readings CONTAINS KEY 'temperature' ALLOW FILTERING
```

Finally, `isNotNull()` generates an `IS NOT NULL` check. *Note: we support `IS NOT NULL` because it
is present in the CQL grammar but, as of Cassandra 4, it is not implemented yet.*

### Column components

`whereMapValue` operates on an value inside of a map:

```java
selectFrom("sensor_data")
    .all()
    .whereColumn("id").isEqualTo(bindMarker())
    .whereColumn("date").isEqualTo(bindMarker())
    .whereMapValue("readings", literal("temperature")).isGreaterThan(literal(65))
    .allowFiltering();
// SELECT * FROM sensor_data WHERE id=? AND date=? AND readings['temperature']>65 ALLOW FILTERING
```

Column components support the six basic arithmetic comparison operators.

### Tokens

`whereToken` hashes one or more columns into a token. It is generally used to perform range queries:

```java
selectFrom("user")
    .all()
    .whereToken("id").isGreaterThan(bindMarker())
    .whereToken("id").isLessThanOrEqualTo(bindMarker());
// SELECT * FROM user WHERE token(id)>? AND token(id)<=?
```

It supports the six basic arithmetic comparison operators.

### Multi-column relations

`whereColumns` compares a set of columns to tuple terms of the same arity. It supports the six basic
arithmetic comparison operators (using lexicographical order):

```java
selectFrom("sensor_data")
    .all()
    .whereColumn("id").isEqualTo(bindMarker())
    .whereColumns("date", "hour").isGreaterThan(tuple(bindMarker(), bindMarker()));
// SELECT * FROM sensor_data WHERE id=? AND (date,hour)>(?,?)
```

In addition, tuples support the `in()` operator. Like with regular columns, bind markers can operate
at different levels:

```java
// Bind the whole list of alternatives (two-element tuples) as a single value:
selectFrom("test")
    .all()
    .whereColumn("k").isEqualTo(literal(1))
    .whereColumns("c1", "c2").in(bindMarker());
// SELECT * FROM test WHERE k=1 AND (c1,c2) IN ?

// Bind each alternative as a value:
selectFrom("test")
    .all()
    .whereColumn("k").isEqualTo(literal(1))
    .whereColumns("c1", "c2").in(bindMarker(), bindMarker(), bindMarker());
// SELECT * FROM test WHERE k=1 AND (c1,c2) IN (?,?,?)

// Bind each element in the alternatives as a value:
selectFrom("test")
    .all()
    .whereColumn("k").isEqualTo(literal(1))
    .whereColumns("c1", "c2").in(
        tuple(bindMarker(), bindMarker()),
        tuple(bindMarker(), bindMarker()),
        tuple(bindMarker(), bindMarker()));
// SELECT * FROM test WHERE k=1 AND (c1,c2) IN ((?,?),(?,?),(?,?))
```

### Custom index expressions

`whereCustomIndex` evaluates a custom index. The argument is a free-form term (what is a legal value
depends on your index implementation):

```java
selectFrom("foo")
    .all()
    .whereColumn("k").isEqualTo(literal(1))
    .whereCustomIndex("my_custom_index", literal("a text expression"));
// SELECT * FROM foo WHERE k=1 AND expr(my_custom_index,'a text expression')
```

### Raw snippets

Finally, it is possible to provide a raw CQL snippet with `whereRaw()`; it will get appended to the
query as-is, without any syntax checking or escaping:

```java
selectFrom("foo").all().whereRaw("k = 1 /*some custom comment*/ AND c<2");
// SELECT * FROM foo WHERE k = 1 /*some custom comment*/ AND c<2
```

This should be used with caution, as it's possible to generate invalid CQL that will fail at
execution time; on the other hand, it can be used as a workaround to handle new CQL features that
are not yet covered by the query builder.

[QueryBuilder]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/QueryBuilder.html
[Relation]:     https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/relation/Relation.html
