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

## Tuples

### Quick overview

Ordered set of anonymous, typed fields, e.g. `tuple<int, text, float>`, `(1, 'a', 1.0)`. 

* `row.getTupleValue()` / `boundStatement.setTupleValue()`.
* positional getters and setters: `tupleValue.getInt(0)`, `tupleValue.setString(1, "a")`...
* getting hold of the [TupleType]: statement or session metadata, `tupleValue.getType()`, or
  `DataTypes.tupleOf()`.
* creating a value from a type: `tupleType.newValue()`.

-----

[CQL tuples][cql_doc] are ordered sets of anonymous, typed fields. They can be used as a column type
in tables, or a field type in [user-defined types](../udts/):

```
CREATE TABLE ks.collect_things (
  pk int,
  ck1 text,
  ck2 text,
  v tuple<int, text, float>,
  PRIMARY KEY (pk, ck1, ck2)
);
```

### Fetching tuples from results

The driver maps tuple columns to the [TupleValue] class, which exposes getters and setters to access
individual fields by index:

```java
Row row = session.execute("SELECT v FROM ks.collect_things WHERE pk = 1").one();

TupleValue tupleValue = row.getTupleValue("v");
int field0 = tupleValue.getInt(0);
String field1 = tupleValue.getString(1);
Float field2 = tupleValue.getFloat(2);
```

### Using tuples as parameters

Statements may contain tuples as bound values:

```java
PreparedStatement ps =
  session.prepare(
      "INSERT INTO ks.collect_things (pk, ck1, ck2, v) VALUES (:pk, :ck1, :ck2, :v)");
```

To create a new tuple value, you must first have a reference to its [TupleType]. There are various
ways to get it:
  
* from the statement's metadata

    ```java
    TupleType tupleType = (TupleType) ps.getVariableDefinitions().get("v").getType();
    ```

* from the driver's [schema metadata](../metadata/schema/):

    ```java
    TupleType tupleType =
        (TupleType)
            session
                .getMetadata()
                .getKeyspace("ks")
                .getTable("collect_things")
                .getColumn("v")
                .getType();
    ```

* from another tuple value:

    ```java
    TupleType tupleType = tupleValue.getType();
    ``` 
  
* or creating it from scratch:

    ```java
    TupleType tupleType = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT, DataTypes.FLOAT);
    ```

    Note that the resulting type is [detached](../detachable_types).
  
Once you have the type, call `newValue()` and set the fields:

```java
TupleValue tupleValue =
    tupleType.newValue().setInt(0, 1).setString(1, "hello").setFloat(2, 2.3f);

// Or as a one-liner for convenience:
TupleValue tupleValue = tupleType.newValue(1, "hello", 2.3f);
```

And bind your tuple value like any other type:

```java
BoundStatement bs =
    ps.boundStatementBuilder()
        .setInt("pk", 1)
        .setString("ck1", "1")
        .setString("ck2", "1")
        .setTupleValue("v", tupleValue)
        .build();
session.execute(bs);
```

Tuples are also used for multi-column `IN` restrictions (usually for tables with composite
clustering keys):

```java
PreparedStatement ps =
    session.prepare("SELECT * FROM ks.collect_things WHERE pk = 1 and (ck1, ck2) IN (:choice1, :choice2)");

TupleType tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.TEXT);
BoundStatement bs = ps.boundStatementBuilder()
    .setTupleValue("choice1", tupleType.newValue("a", "b"))
    .setTupleValue("choice2", tupleType.newValue("c", "d"))
    .build();
```

If you bind the whole list of choices as a single variable, a list of tuple values is expected:

```java
PreparedStatement ps =
    // Note the absence of parentheses around ':choices'
    session.prepare("SELECT * FROM ks.collect_things WHERE pk = 1 and (ck1, ck2) IN :choices");

TupleType tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.TEXT);
List<TupleValue> choices = new ArrayList<>();
choices.add(tupleType.newValue("a", "b"));
choices.add(tupleType.newValue("c", "d"));
BoundStatement bs =
    ps.boundStatementBuilder().setList("choices", choices, TupleValue.class).build();
```

[cql_doc]: https://docs.datastax.com/en/cql/3.3/cql/cql_reference/tupleType.html

[TupleType]:  https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/type/TupleType.html
[TupleValue]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/data/TupleValue.html
