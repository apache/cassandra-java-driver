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

## User-defined types

### Quick overview

Ordered set of named, typed fields, e.g. `{ street: '1 Main St', zip: 12345}`.

* `row.getUdtValue()` / `boundStatement.setUdtValue()`.
* positional or named getters and setters: `udtValue.getString("street")`,
  `udtValue.setInt(1, 12345)`...
* getting hold of the [UserDefinedType]:
  * statement or session metadata, or `udtValue.getType()`.
  * `UserDefinedTypeBuilder` (not recommended, dangerous if you build a type that doesn't match the
    database schema).
* creating a value from a type: `userDefinedType.newValue()`.

-----


[CQL user-defined types][cql_doc] are ordered sets of named, typed fields. They must be defined in a
keyspace:

```
CREATE TYPE ks.type1 (
  a int,
  b text,
  c float);
```

And can then be used as a column type in tables, or a field type in other user-defined types in that
keyspace:

```
CREATE TABLE ks.collect_things (
  pk int,
  ck1 text,
  ck2 text,
  v frozen<type1>,
  PRIMARY KEY (pk, ck1, ck2)
);

CREATE TYPE ks.type2 (v frozen<type1>);
```

### Fetching UDTs from results

The driver maps UDT columns to the [UdtValue] class, which exposes getters and setters to access
individual fields by index or name:

```java
Row row = session.execute("SELECT v FROM ks.collect_things WHERE pk = 1").one();

UdtValue udtValue = row.getUdtValue("v");
int a = udtValue.getInt(0);
String b = udtValue.getString("b");
Float c = udtValue.getFloat(2);
```

### Using UDTs as parameters

Statements may contain UDTs as bound values:

```java
PreparedStatement ps =
  session.prepare(
      "INSERT INTO ks.collect_things (pk, ck1, ck2, v) VALUES (:pk, :ck1, :ck2, :v)");
```

To create a new UDT value, you must first have a reference to its [UserDefinedType]. There are
various ways to get it:

* from the statement's metadata

    ```java
    UserDefinedType udt = (UserDefinedType) ps.getVariableDefinitions().get("v").getType();
    ```

* from the driver's [schema metadata](../metadata/schema/):

    ```java
    UserDefinedType udt =
        session.getMetadata()
            .getKeyspace("ks")
            .flatMap(ks -> ks.getUserDefinedType("type1"))
            .orElseThrow(() -> new IllegalArgumentException("Missing UDT definition"));
    ```

* from another UDT value:

    ```java
    UserDefinedType udt = udtValue.getType();
    ```
  
Note that the driver's official API does not expose a way to build [UserDefinedType] instances
manually. This is because the type's internal definition must precisely match the database schema;
if it doesn't (for example if the fields are not in the same order), you run the risk of inserting
corrupt data, that you won't be able to read back. There is still a way to do it with the driver,
but it's part of the [internal API](../../api_conventions/):

```java
// Advanced usage: make sure you understand the risks
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;

UserDefinedType udt =
    new UserDefinedTypeBuilder("ks", "type1")
        .withField("a", DataTypes.INT)
        .withField("b", DataTypes.TEXT)
        .withField("c", DataTypes.FLOAT)
        .build();
```

Note that a manually created type is [detached](../detachable_types).

 
Once you have the type, call `newValue()` and set the fields:

```java
UdtValue udtValue = udt.newValue().setInt(0, 1).setString(1, "hello").setFloat(2, 2.3f);

// Or as a one-liner for convenience:
UdtValue udtValue = udt.newValue(1, "hello", 2.3f);
```

And bind your UDT value like any other type:

```java
BoundStatement bs =
    ps.boundStatementBuilder()
        .setInt("pk", 1)
        .setString("ck1", "1")
        .setString("ck2", "1")
        .setUdtValue("v", udtValue)
        .build();
session.execute(bs);
```

[cql_doc]: https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlRefUDType.html

[UdtValue]:        https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/data/UdtValue.html
[UserDefinedType]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/type/UserDefinedType.html
