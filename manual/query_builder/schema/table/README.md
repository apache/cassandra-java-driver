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

## Table

Data in Apache Cassandra is stored in tables.  [SchemaBuilder] offers API methods for creating,
altering, and dropping tables.

### Creating a Table (CREATE TABLE)

To start a `CREATE TABLE` query, use `createTable` in [SchemaBuilder]:

```java
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

CreateTableStart create = createTable("cycling", "cyclist_name");
```

Like all other `CREATE` queries, one may supply `ifNotExists()` to require that the table should
only be created if it doesn't already exist, i.e.:

```java
CreateTableStart create = createTable("cycling", "cyclist_name").ifNotExists();
```

Note that, at this stage, the query cannot be completed yet.  You need to provide at least one
partition key column using `withPartitionKey()`, i.e.:

```java
CreateTable create = createTable("cycling", "cyclist_name").withPartitionKey("id", DataTypes.UUID);
// CREATE TABLE cycling.cyclist_name (id UUID PRIMARY KEY)
```

A table with only one column is not so typical however.  At this point you may provide partition,
clustering, regular and static columns using any of the following API methods:

* `withPartitionKey(name, dataType)`
* `withClusteringColumn(name, dataType)`
* `withColumn(name, dataType)`
* `withStaticColumn(name, dataType)`

Primary key precedence is driven by the order of `withPartitionKey` and `withClusteringKey`
invocations, for example:


```java
CreateTable create = createTable("cycling", "cyclist_by_year_and_name")
    .withPartitionKey("race_year", DataTypes.INT)
    .withPartitionKey("race_name", DataTypes.TEXT)
    .withClusteringColumn("rank", DataTypes.INT)
    .withColumn("cyclist_name", DataTypes.TEXT);
// CREATE TABLE cycling.cyclist_by_year_and_name (race_year int,race_name text,rank int,cyclist_name text,PRIMARY KEY((race_year,race_name),rank))
```

After providing the column specification, clustering order and many table options may be provided. 
Refer to [CreateTableWithOptions] for the variety of configuration options available.

The following configures compaction and compression options and includes a clustering order.

```java
CreateTableWithOptions create = createTable("cycling", "cyclist_by_year_and_name")
    .withPartitionKey("race_year", DataTypes.INT)
    .withPartitionKey("race_name", DataTypes.TEXT)
    .withClusteringColumn("rank", DataTypes.INT)
    .withColumn("cyclist_name", DataTypes.TEXT)
    .withCompaction(leveledCompactionStrategy())
    .withSnappyCompression()
    .withClusteringOrder("rank", ClusteringOrder.DESC);
// CREATE TABLE cycling.cyclist_by_year_and_name (race_year int,race_name text,rank int,cyclist_name text,PRIMARY KEY((race_year,race_name),rank))
// WITH CLUSTERING ORDER BY (rank DESC)
// AND compaction={'class':'LeveledCompactionStrategy'}
// AND compression={'class':'SnappyCompressor'}
```

### Altering a Table (ALTER TABLE)

To start an `ALTER TABLE` query, use `alterTable`:

```java
alterTable("cycling", "cyclist_name");
```

From here, you can modify the table in the following ways:

* `dropCompactStorage()`: Drops `COMPACT STORAGE` from a table, removing thrift compatibility mode
  and migrates to a CQL-compatible format.
* `addColumn(columnName, dataType)`: Adds a new column to the table.
* `alterColumn(columnName, dataType)`: Changes the type of an existing column.  This is not
  recommended.
* `dropColumn(columnName)`: Removes an existing column from the table.
* `renameColumn(from, to)`: Renames a column.
* API methods from [AlterTableWithOptions]

Invoking any of these methods returns a complete query and you may make successive calls to the same
API methods, with exception to alter column, which may only be invoked once.

### Dropping a Table (DROP TABLE)

To create a `DROP TABLE` query, use `dropTable`:

```java
dropTable("cycling", "cyclist_name");
// DROP TABLE cycling.cyclist_name
```

You may also specify `ifExists`:

```java
dropTable("cyclist_name").ifExists();
// DROP TABLE IF EXISTS cyclist_name
```

[SchemaBuilder]:          https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/SchemaBuilder.html
[CreateTableWithOptions]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/schema/CreateTableWithOptions.html
[AlterTableWithOptions]:  https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/schema/AlterTableWithOptions.html
