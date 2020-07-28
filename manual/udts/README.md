## User-defined types

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

The driver maps UDT columns to the [UDTValue] class, which exposes getters and setters to access
individual fields by index or name:

```java
Row row = session.execute("SELECT v FROM ks.collect_things WHERE pk = 1").one();

UDTValue udtValue = row.getUDTValue("v");
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

To create a new UDT value, you must first have a reference to its [UserType]. There are
various ways to get it:

* from the statement's metadata

    ```java
    UserType udt = (UserType) ps.getVariables().getType("v");
    ```

* from the driver's [schema metadata](../metadata/#schema-metadata):

    ```java
    UserType udt = session.getCluster().getMetadata().getKeyspace("ks").getUserType("type1");
    ```

* from another UDT value:

    ```java
    UserType udt = udtValue.getType();
    ```
  
Note that the driver's official API does not expose a way to build [UserType] instances manually.
This is because the type's internal definition must precisely match the database schema;
if it doesn't (for example if the fields are not in the same order), you run the risk of inserting
corrupt data, that you won't be able to read back.
 
Once you have the type, call `newValue()` and set the fields:

```java
UDTValue udtValue = udt.newValue().setInt(0, 1).setString(1, "hello").setFloat(2, 2.3f);
```

And bind your UDT value like any other type:

```java
BoundStatement bs =
    ps.bind()
        .setInt("pk", 1)
        .setString("ck1", "1")
        .setString("ck2", "1")
        .setUDTValue("v", udtValue);
session.execute(bs);
```

[cql_doc]: https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlRefUDType.html

[UDTValue]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/UDTValue.html
[UserType]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/UserType.html
