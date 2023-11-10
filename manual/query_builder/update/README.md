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

## UPDATE

To start an UPDATE query, use one of the `update` methods in [QueryBuilder]. There are several
variants depending on whether your table name is qualified, and whether you use
[identifiers](../../case_sensitivity/) or raw strings:

```java
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

UpdateStart update = update("user");
```

Note that, at this stage, the query can't be built yet. You need at least one
[assignment](#assignments) and one [relation](#relations).

### Timestamp

The USING TIMESTAMP clause comes right after the table, and specifies the timestamp at which the
mutation will be applied. You can pass either a literal value:

```java
update("user").usingTimestamp(1234);
// UPDATE user USING TIMESTAMP 1234...
```

Or a bind marker:

```java
update("user").usingTimestamp(bindMarker());
// UPDATE user USING TIMESTAMP ?...
```

If you call the method multiple times, the last value will be used.

### Time To Live (TTL)

You can generate a USING TTL clause that will cause column values to be deleted (marked with a
tombstone) after the specified time (in seconds) has expired. This can be done with a literal:

```java
update("user").usingTtl(60).setColumn("v", bindMarker()).whereColumn("k").isEqualTo(bindMarker());
// UPDATE user USING TTL 60 SET v=? WHERE k=?
```

Or a bind marker:

```java
update("user").usingTtl(bindMarker()).setColumn("v", bindMarker()).whereColumn("k").isEqualTo(bindMarker());
// UPDATE user USING TTL ? SET v=? WHERE k=?
```

You can clear a previously set TTL by setting the value to 0:

```java
update("user").usingTtl(0).setColumn("v", bindMarker()).whereColumn("k").isEqualTo(bindMarker());
// UPDATE user USING TTL 0 SET v=? WHERE k=?
```

Setting the value to 0 will result in removing the TTL from the column in Cassandra when the query
is executed. This is distinctly different than setting the value to null. Passing a null value to
this method will only remove the USING TTL clause from the query, which will not alter the TTL (if
one is set) in Cassandra.

### Assignments

An assignment is an operation that appears after the SET keyword. You need at least one for a valid
update query.

The easiest way to add an assignment is with one of the fluent methods:

```java
update("user").setColumn("v", bindMarker());
// UPDATE user SET v=?...
```

You can also create it manually with one of the factory methods in [Assignment], and then pass it to
`set()`:

```java
update("user").set(
    Assignment.setColumn("v", bindMarker()));
// UPDATE user SET v=?...
```

If you have multiple assignments, you can add them all in a single call. This is a bit more
efficient since it creates less temporary objects:

```java
update("user").set(
    Assignment.setColumn("v1", bindMarker()),
    Assignment.setColumn("v2", bindMarker()))
// UPDATE user SET v1=?,v2=?...
```

#### Simple columns

As shown already, `setColumn` changes the value of a column. It can take a bind marker or a literal
(which must have the same CQL type as the column):

```java
update("user").setColumn("last_name", literal("Doe"));
// UPDATE user SET last_name='Doe'...
```

#### UDT fields

`setField` modifies a field inside of a UDT column:

```java
update("user").setField("address", "street", bindMarker());
// UPDATE user SET address.street=?...
```

#### Counters

Counter columns can be incremented by a given amount:

```java
update("foo").increment("c", bindMarker());
// UPDATE foo SET c+=?...

update("foo").increment("c", literal(4));
// UPDATE foo SET c+=4...
```

There is a shortcut to increment by 1:

```java
update("foo").increment("c");
// UPDATE foo SET c+=1...
```

All those methods have a `decrement` counterpart:

```java
update("foo").decrement("c");
// UPDATE foo SET c-=1...
```

#### Collections

`mapValue` changes a value in a map. The key is expressed as a term (here a literal value) :

```java
update("product").setMapValue("features", literal("color"), bindMarker())
// UPDATE product SET features['color']=?...
```

`append` operates on any CQL collection type (list, set or map). If you pass a literal, it must also
be a collection, with the same CQL type of elements:

```java
update("foo").append("l", bindMarker());
// UPDATE foo SET l+=?...

List<Integer> value = Arrays.asList(1, 2, 3);
update("foo").append("l", literal(value));
// UPDATE foo SET l+=[1,2,3]...

Set<Integer> value = new HashSet<>(Arrays.asList(1, 2, 3));
update("foo").append("s", literal(value))
// UPDATE foo SET s+={1,2,3}...

Map<Integer, String> value = new HashMap<>();
value.put(1, "bar");
value.put(2, "baz");
update("foo").append("m", literal(value));
// UPDATE foo SET m+={1:'bar',2:'baz'}...
```

If you only have one element to append, there are shortcuts to avoid creating a collection in your
code:

```java
update("foo").appendListElement("l", literal(1));
// UPDATE foo SET l+=[1]...

update("foo").appendSetElement("s", literal(1));
// UPDATE foo SET s+={1}...

update("foo").appendMapEntry("m", literal(1), literal("bar"));
// UPDATE foo SET m+={1:'bar'}...
```

All those methods have a `prepend` counterpart:

```java
update("foo").prepend("l", bindMarker());
// UPDATE foo SET l=?+l...
```

As well as a `remove` counterpart:

```java
update("foo").remove("l", bindMarker());
// UPDATE foo SET l-=?...
```

### Relations

Once you have at least one assignment, relations can be added with the fluent `whereXxx()` methods:

```java
update("foo").setColumn("v", bindMarker()).whereColumn("k").isEqualTo(bindMarker());
// UPDATE foo SET v=? WHERE k=?
```

Or you can build and add them manually:

```java
update("foo").setColumn("v", bindMarker()).where(
    Relation.column("k").isEqualTo(bindMarker()));
// UPDATE foo SET v=? WHERE k=?
```

Once there is at least one assignment and one relation, the statement can be built:

```java
SimpleStatement statement = update("foo")
    .setColumn("k", bindMarker())
    .whereColumn("k").isEqualTo(bindMarker())
    .build();
```

Relations are a common feature used by many types of statements, so they have a
[dedicated page](../relation) in this manual.

### Conditions

Conditions get added with the fluent `ifXxx()` methods:

```java
update("foo")
    .setColumn("v", bindMarker())
    .whereColumn("k").isEqualTo(bindMarker())
    .ifColumn("v").isEqualTo(bindMarker());
// UPDATE foo SET v=? WHERE k=? IF v=?
```

Or you can build and add them manually:

```java
update("foo")
    .setColumn("v", bindMarker())
    .whereColumn("k").isEqualTo(bindMarker())
    .if_(
        Condition.column("v").isEqualTo(bindMarker()));
// UPDATE foo SET v=? WHERE k=? IF v=?
```

Conditions are a common feature used by UPDATE and DELETE, so they have a
[dedicated page](../condition) in this manual.

[QueryBuilder]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/QueryBuilder.html
[Assignment]:   https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/update/Assignment.html
