# Schema builder

The schema builder is an additional API provided by [java-driver-query-builder](../) that enables
one to *generate CQL DDL queries programmatically**.  For example it could be used to:

* based on application configuration, generate schema queries instead of building CQL strings by
  hand.
* given a Java class that represents a table, view, or user defined type, generate representative
  schema DDL `CREATE` queries.

Here is an example that demonstrates creating a keyspace and a table using [SchemaBuilder]:

```java
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

try (CqlSession session = CqlSession.builder().build()) {
  CreateKeyspace createKs = createKeyspace("cycling").withSimpleStrategy(1);
  session.execute(createKs.build());

  CreateTable createTable =
      createTable("cycling", "cyclist_name")
          .withPartitionKey("id", DataTypes.UUID)
          .withColumn("lastname", DataTypes.TEXT)
          .withColumn("firstname", DataTypes.TEXT);

  session.execute(createTable.build());
}
```

The [general concepts](../#general-concepts) and [non goals](../#non-goals) defined for the query
builder also apply for the schema builder.

### Building DDL Queries

The schema builder offers functionality for creating, altering and dropping elements of a CQL
schema.  For a complete tour of the API, browse the child pages in the manual for each schema
element type:

* [keyspace](keyspace/)
* [table](table/)
* [index](index/)
* [materialized view](materialized_view/)
* [type](type/)
* [function](function/)
* [aggregate](aggregate/)

[SchemaBuilder]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/querybuilder/SchemaBuilder.html
