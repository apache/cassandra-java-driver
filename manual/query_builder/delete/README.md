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

## DELETE

To start a DELETE query, use one of the `deleteFrom` methods in [QueryBuilder]. There are several
variants depending on whether your table name is qualified, and whether you use
[identifiers](../../case_sensitivity/) or raw strings:

```java
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

DeleteSelection delete = deleteFrom("user");
```

Note that, at this stage, the query can't be built yet. You need at least one
[relation](#relations).

### Selectors

A selector is something that appears after the `DELETE` keyword, and will be removed from the
affected row(s).

Selectors are optional; if you don't provide any, the whole row will be deleted.

The easiest way to add a selector is with a fluent API method:

```java
deleteFrom("user").column("v1").column("v2");
// DELETE v1,v2 FROM user...
```

You can also create it manually with one of the factory methods in [Selector], and then pass it to
`selector()`:

```java
deleteFrom("user").selector(Selector.getColumn("v"))
// DELETE v FROM user ...
```

If you have multiple selectors, you can also use `selectors()` to add them all in a single call.
This is a bit more efficient since it creates less temporary objects: 

```java
deleteFrom("user").selectors(getColumn("v1"), getColumn("v2"));
// DELETE v1,v2 FROM user...
```

Only 3 types of selectors can be used in DELETE statements:

* simple columns (as illustrated in the previous examples);
* fields in non-nested UDT columns:

  ```java
  deleteFrom("user").field("address", "street");
  // DELETE address.street FROM user ...
  ```
  
* elements in non-nested collection columns:

  ```java
  deleteFrom("product").element("features", literal("color"));
  // DELETE features['color'] FROM product ...
  ```
  
You can also pass a raw CQL snippet, that will get appended to the query as-is, without any syntax
checking or escaping:

```java
deleteFrom("user").raw("v /*some random comment*/")
// DELETE v /*some random comment*/ FROM user ...
```

This should be used with caution, as it's possible to generate invalid CQL that will fail at
execution time; on the other hand, it can be used as a workaround to handle new CQL features that
are not yet covered by the query builder.

### Timestamp

The USING TIMESTAMP clause specifies the timestamp at which the mutation will be applied. You can
pass either a literal value:

```java
deleteFrom("user").column("v").usingTimestamp(1234)
// DELETE v FROM user USING TIMESTAMP 1234
```

Or a bind marker:

```java
deleteFrom("user").column("v").usingTimestamp(bindMarker())
// DELETE v FROM user USING TIMESTAMP ?
```

If you call the method multiple times, the last value will be used.

### Relations

Relations get added with the fluent `whereXxx()` methods:

```java
deleteFrom("user").whereColumn("k").isEqualTo(bindMarker());
// DELETE FROM user WHERE k=?
```

Or you can build and add them manually:

```java
deleteFrom("user").where(
    Relation.column("k").isEqualTo(bindMarker()));
// DELETE FROM user WHERE k=?
```

Once there is at least one relation, the statement can be built:

```java
SimpleStatement statement = deleteFrom("user").whereColumn("k").isEqualTo(bindMarker()).build();
```

Relations are a common feature used by many types of statements, so they have a
[dedicated page](../relation) in this manual.

### Conditions

Conditions get added with the fluent `ifXxx()` methods:

```java
deleteFrom("user")
    .whereColumn("k").isEqualTo(bindMarker())
    .ifColumn("v").isEqualTo(literal(1));
// DELETE FROM user WHERE k=? IF v=1
```

Or you can build and add them manually:

```java
deleteFrom("user")
    .whereColumn("k").isEqualTo(bindMarker())
    .if_(
        Condition.Column("v").isEqualTo(literal(1)));
// DELETE FROM user WHERE k=? IF v=1
```

Conditions are a common feature used by UPDATE and DELETE, so they have a
[dedicated page](../condition) in this manual.

[QueryBuilder]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/QueryBuilder.html
[Selector]:     https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/select/Selector.html
