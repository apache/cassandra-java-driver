## Materialized View

Materialized Views are an experimental feature introduced in Apache Cassandra 3.0 that provide a
mechanism for server-side denormalization from a base table into a view that is updated when the
base table is updated. [SchemaBuilder] offers API methods for creating, altering and dropping
materialized views.

### Creating a Materialized View (CREATE MATERIALIZED VIEW)

To start a `CREATE MATERIALIZED VIEW` query, use `createMaterializedView` in [SchemaBuilder]:

```java
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

CreateMaterializedViewStart create = createMaterializedView("cycling", "cyclist_by_age");
```

Like all other `CREATE` queries, one may supply `ifNotExists()` to require that the view should only
be created if it doesn't already exist, i.e.:

```java
CreateMaterializedViewStart create = createMaterializedView("cycling", "cyclist_by_age").ifNotExists();
```

There are a number of steps that must be executed to complete a materialized view:

* Specify the base table using `asSelectFrom`
* Specify the columns to include in the view via `column` or `columns` 
* Specify the where clause using [relations](../../relation)
* Specify the partition key columns using `withPartitionKey` and `withClusteringColumn`

For example, the following defines a complete `CREATE MATERIALIZED VIEW` statement:

```java
createMaterializedView("cycling", "cyclist_by_age")
    .asSelectFrom("cycling", "cyclist")
    .columns("age", "name", "country")
    .whereColumn("age")
    .isNotNull()
    .whereColumn("cid")
    .isNotNull()
    .withPartitionKey("age")
    .withClusteringColumn("cid");
// CREATE MATERIALIZED VIEW cycling.cyclist_by_age AS
// SELECT age,name,country FROM cycling.cyclist WHERE age IS NOT NULL AND cid IS NOT NULL PRIMARY KEY(age,cid) 
```

Please note that not all WHERE clause relations may be compatible with materialized views.

Like a [table](../table), one may additionally provide configuration such as clustering order,
compaction options and so on.  Refer to [RelationStructure] for documentation on additional
configuration that may be provided for a view.

### Altering a Materialized View (ALTER MATERIALIZED VIEW)

To start a `ALTER MATERIALIZED VIEW` query, use `alterMaterializedView`:

```java
alterMaterializedView("cycling", "cyclist_by_age");
```

Unlike a table, you may not alter, drop or rename columns on a materialized view.  Instead, one may
only alter the options defined in [RelationStructure].  For example:

```java
alterMaterializedView("cycling", "cyclist_by_age")
    .withGcGraceSeconds(0)
    .withCaching(true, RowsPerPartition.NONE);
// ALTER MATERIALIZED VIEW cycling.cyclist_by_age WITH gc_grace_seconds=0 AND caching={'keys':'ALL','rows_per_partition':'NONE'}
```

### Dropping a View (DROP MATERIALIZED VIEW)

To create a `DROP MATERIALIZED VIEW` query, use `dropMaterializedView`:

```java
dropMaterializedView("cycling", "cyclist_by_age");
// DROP MATERIALIZED VIEW cycling.cyclist_by_age
```

You may also specify `ifExists`:

```java
dropTable("cyclist_by_age").ifExists();
// DROP MATERIALIZED VIEW IF EXISTS cyclist_by_age
```

[SchemaBuilder]:     https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/querybuilder/SchemaBuilder.html
[RelationStructure]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/querybuilder/schema/RelationStructure.html
