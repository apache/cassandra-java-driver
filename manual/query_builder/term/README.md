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

## Terms

A term is an expression that does not involve the value of a column. It is used:

* as an argument to some selectors, for example the indices of [sub-element](../select/#sub-element)
  selectors;
* as the right operand of [relations](../relation).

To create a term, call one of the factory methods in [QueryBuilder]:

### Literals

`literal()` takes a Java object and inlines it as a CQL literal:

```java
selectFrom("user").all().whereColumn("id").isEqualTo(literal(1));
// SELECT * FROM user WHERE id=1
```

The argument is converted according to the driver's
[default type mappings](../../core/#cql-to-java-type-mapping). If there is no default mapping, you
will get a `CodecNotFoundException`.

If you use [custom codecs](../../core/custom_codecs), you might need to inline a custom Java type.
You can pass a [CodecRegistry] as the second argument (most likely, this will be the registry of
your session):

```java
MyCustomId myCustomId = ...;
CodecRegistry registry = session.getContext().getCodecRegistry();
selectFrom("user").all().whereColumn("id").isEqualTo(literal(myCustomId, registry));
```

Alternatively, you can pass a codec directly:

```java
TypeCodec<MyCustomId> codec = ...;
selectFrom("user").all().whereColumn("id").isEqualTo(literal(myCustomId, codec));
```

### Function calls

`function()` invokes a built-in or user-defined function. It takes a function name (optionally
qualified with a keyspace), and a list of terms that will be passed as arguments:

```java
selectFrom("sensor_data")
    .all()
    .whereColumn("id").isEqualTo(bindMarker())
    .whereColumn("date").isEqualTo(function("system", "currentDate"));
// SELECT * FROM sensor_data WHERE id=? AND date=system.currentdate()
```

### Arithmetic operations

Terms can be combined with arithmetic operations.

| CQL Operator | Selector name |
|--------------|---------------|
| `a+b`        | `add`         |
| `a-b`        | `subtract`    |
| `-a`         | `negate`      |
| `a*b`        | `multiply`    |
| `a/b`        | `divide`      |
| `a%b`        | `remainder`   |

```java
selectFrom("sensor_data")
    .all()
    .whereColumn("id").isEqualTo(bindMarker())
    .whereColumn("unix_timestamp").isGreaterThan(
        subtract(function("toUnixTimestamp", function("now")),
        literal(3600)));
// SELECT * FROM sensor_data WHERE id=? AND unix_timestamp>tounixtimestamp(now())-3600
```

Operations can be nested, and will get parenthesized according to the usual precedence rules.

### Type hints

`typeHint` forces a term to a particular CQL type. For instance, it could be used to ensure that an
expression uses floating-point division:

```java
selectFrom("test")
    .all()
    .whereColumn("k").isEqualTo(literal(1))
    .whereColumn("c").isGreaterThan(divide(
            typeHint(literal(1), DataTypes.DOUBLE), 
            literal(3)));
// SELECT * FROM test WHERE k=1 AND c>(double)1/3
```

### Raw CQL snippets

Finally, it is possible to provide a raw CQL snippet with `raw()`; it will get appended to the query
as-is, without any syntax checking or escaping:

```java
selectFrom("sensor_data").all().whereColumn("id").isEqualTo(raw("  1 /*some random comment*/"));
// SELECT * FROM sensor_data WHERE id=  1 /*some random comment*/
```

This should be used with caution, as it's possible to generate invalid CQL that will fail at
execution time; on the other hand, it can be used as a workaround to handle new CQL features that
are not yet covered by the query builder.

[QueryBuilder]:  https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/QueryBuilder.html
[CodecRegistry]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/type/codec/registry/CodecRegistry.html
