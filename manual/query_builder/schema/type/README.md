## Type

User-defined types are special types that can associate multiple named fields to a single column. 
[SchemaBuilder] offers API methods for creating, altering, and dropping types.

### Creating a Type (CREATE TYPE)

To start a `CREATE TYPE` query, use `createType` in [SchemaBuilder]:

```java
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

CreateTypeStart create = createType("mykeyspace", "address");
```

Like all other `CREATE` queries, one may supply `ifNotExists()` to require that the type should only
be created if it doesn't already exist, i.e.:

```java
CreateTypeStart create = createType("address").ifNotExists();
```

Note that, at this stage, the query cannot be completed yet.  You need to provide at least one field
using `withField()`, i.e.:

```java
CreateType create = createType("mykeyspace", "address").withField("street", DataTypes.TEXT);
// CREATE TYPE mykeyspace.address (street text)
```

A type with only one field is not entirely useful.  You may continue to make successive calls to
`withField` to specify additional fields, i.e.:

```java
CreateType create = createType("mykeyspace", "address")
    .withField("street", DataTypes.TEXT)
    .withField("city", DataTypes.TEXT)
    .withField("zip_code", DataTypes.INT)
    .withField("phones", DataTypes.setOf(DataTypes.TEXT));
// CREATE TYPE mykeyspace.address (street text,city text,zip_code int,phones set<text>)
```

### Using a created Type in Schema Builder API

After creating a UDT, one may wonder how to use it in other schema statements.  To do so, utilize
`udt(name,frozen)` from [SchemaBuilder], i.e:

```java
CreateTable users = createTable("mykeyspace", "users")
    .withPartitionKey("id", DataTypes.UUID)
    .withColumn("name", udt("fullname", true))
    .withColumn("name", DataTypes.setOf(udt("direct_reports", true)))
    .withColumn("addresses", DataTypes.mapOf(DataTypes.TEXT, udt("address", true)));
// CREATE TABLE mykeyspace.users (id uuid PRIMARY KEY,name set<frozen<direct_reports>>,addresses map<text, frozen<address>>)
```

### Altering a Type (ALTER TYPE)

To start an `ALTER TYPE` query, use `alterType`:

```java
alterTable("mykeyspace", "address");
```

From here, you can modify the type in the following ways:

* `addField(fieldName, dataType)`: Adds a new field to the type.
* `alterField(fieldName, dataType)`: Changes the type of an existing field.  This is not
  recommended.
* `renameField(from, to)`: Renames a field.

Invoking any of these methods returns a complete query.  You may make successive calls to
`renameField`, but not the other methods.

### Dropping a Type (DROP TYPE)

To create a `DROP TYPE` query, use `dropType`:

```java
dropType("mykeyspace", "address");
// DROP TYPE mykeyspace.address
```

You may also specify `ifExists`:

```java
dropTable("address").ifExists();
// DROP TYPE IF EXISTS address
```

[SchemaBuilder]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/querybuilder/SchemaBuilder.html
