## INSERT

To start an INSERT query, use one of the `insertInto` methods in [QueryBuilderDsl]. There are
several variants depending on whether your table name is qualified, and whether you use
case-sensitive identifiers or case-insensitive strings:

```java
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.*;

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

[QueryBuilderDsl]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/query-builder/QueryBuilderDsl.html
