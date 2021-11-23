## INSERT

To start an INSERT query, use one of the `insertInto` methods in [QueryBuilder]. There are
several variants depending on whether your table name is qualified, and whether you use
[identifiers](../../case_sensitivity/) or raw strings:

```java
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

InsertInto insert = insertInto("user");
```

Note that, at this stage, the query can't be built yet. You need to set at least one value.

### Setting values

#### Regular insert

A regular insert (as opposed to a JSON insert, covered in the next section) specifies values for a
set of columns. In the Query Builder DSL, this is expressed with the `value` method:

```java
insertInto("user")
    .value("id", bindMarker())
    .value("first_name", literal("John"))
    .value("last_name", literal("Doe"));
// INSERT INTO user (id,first_name,last_name) VALUES (?,'John','Doe')
```

The column names can only be simple identifiers. The values are [terms](../term).

#### JSON insert

To start a JSON insert, use the `json` method instead. It takes the payload as a raw string, that
will get inlined as a CQL literal:

```java
insertInto("user").json("{\"id\":1, \"first_name\":\"John\", \"last_name\":\"Doe\"}");
// INSERT INTO user JSON '{"id":1, "first_name":"John", "last_name":"Doe"}'
```

In a real application, you'll probably obtain the string from a JSON library such as Jackson.

You can also bind it as a value:

```java
insertInto("user").json(bindMarker());
// INSERT INTO user JSON ?
```

JSON inserts have extra options to indicate how missing fields should be handled:

```java
insertInto("user").json("{\"id\":1}").defaultUnset();
// INSERT INTO user JSON '{"id":1}' DEFAULT UNSET

insertInto("user").json("{\"id\":1}").defaultNull();
// INSERT INTO user JSON '{"id":1}' DEFAULT NULL
```

### Conditions

For INSERT queries, there is only one possible condition: IF NOT EXISTS. It applies to both regular
and JSON inserts:

```java
insertInto("user").json(bindMarker()).ifNotExists();
// INSERT INTO user JSON ? IF NOT EXISTS
```

### Timestamp

The USING TIMESTAMP clause specifies the timestamp at which the mutation will be applied. You can
pass either a literal value:

```java
insertInto("user").json(bindMarker()).usingTimestamp(1234)
// INSERT INTO user JSON ? USING TIMESTAMP 1234
```

Or a bind marker:

```java
insertInto("user").json(bindMarker()).usingTimestamp(bindMarker())
// INSERT INTO user JSON ? USING TIMESTAMP ?
```

If you call the method multiple times, the last value will be used.

### Time To Live (TTL)

You can generate a USING TTL clause that will cause column values to be deleted (marked with a
tombstone) after the specified time (in seconds) has expired. This can be done with a literal:

```java
insertInto("user").value("a", bindMarker()).usingTtl(60)
// INSERT INTO user (a) VALUES (?) USING TTL 60
```

Or a bind marker:

```java
insertInto("user").value("a", bindMarker()).usingTtl(bindMarker())
// INSERT INTO user (a) VALUES (?) USING TTL ?
```

If you call the method multiple times, the last value will be used.

The TTL value applies only to the inserted data, not the entire column. Any subsequent updates to
the column resets the TTL.

Setting the value to 0 will result in removing the TTL for the inserted data in Cassandra when the query
is executed. This is distinctly different than setting the value to null. Passing a null value to
this method will only remove the USING TTL clause from the query, which will not alter the TTL (if
one is set) in Cassandra.

[QueryBuilder]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/querybuilder/QueryBuilder.html