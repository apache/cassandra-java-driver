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

# Index

An index provides a means of expanding the query capabilities of a table.  [SchemaBuilder] offers
API methods for creating and dropping indices.  Unlike other schema members, there is no mechanism
to alter an index.

### Creating an Index (CREATE INDEX)

To start a `CREATE INDEX` query, use `createIndex` in [SchemaBuilder]:

```java
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

// an index name is not required
CreateIndexStart create = createIndex();

create = createIndex("my_idx");
```

Unlike other keyspace elements, there is not option to provide the keyspace name, instead it is
implied from the indexed table's keyspace.

Like all other `CREATE` queries, one may supply `ifNotExists()` to require that the index should
only be created if it doesn't already exist, i.e.:

```java
CreateIndexStart create = createIndex("my_idx").ifNotExists();
```

Note one small difference with `IF NOT EXISTS` with indices is that the criteria also applies to
whether or not the table and column specification has an index already, not just the name of the
index.

At this stage, the query cannot be completed yet.  You need to provide at least:

* The table the index applies to using `onTable`
* The column the index applies to using an `andColumn*` implementation.

For example:

```java
createIndex().onTable("tbl").andColumnKeys("addresses");
// CREATE INDEX ON tbl (KEYS(addresses))
```

#### Custom Indices

Cassandra supports indices with a custom implementation, specified by an input class name.  The
class implementation may be specified using `custom(className)`, for example:

```java
createIndex()
    .custom("org.apache.cassandra.index.MyCustomIndex")
    .onTable("tbl")
    .andColumn("x");
// CREATE CUSTOM INDEX ON tbl (x) USING 'org.apache.cassandra.index.MyCustomIndex'
```

One popular custom index implementation is SASI (SSTable Attached Secondary Index).  To use SASI,
use `usingSASI` and optionally `withSASIOptions`:

```java
createIndex()
    .usingSASI()
    .onTable("tbl")
    .andColumn("x")
    .withSASIOptions(ImmutableMap.of("mode", "CONTAINS", "tokenization_locale", "en"));
// CREATE CUSTOM INDEX ON tbl (x) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS={'mode':'CONTAINS','tokenization_locale':'en'}
```

#### Column Index Types

When indexing columns, one may simply use `andColumn`.   However, when indexing collection columns
there are several additional options available:

* `andColumnKeys`: Creates an index on a map column's keys.
* `andColumnValues`: Creates an index on a map column's values.
* `andColumnEntries`: Creates an index on a map column's entries.
* `andColumnFull`:  Creates an index of a frozen collection's full value.

#### Index options

After specifying the columns for the index, you may use `withOption` to provide free-form options on
the index.  These are really only applicable to custom index implementations.

### Dropping an Index (DROP INDEX)

To create a `DROP INDEX` query, use `dropIndex`:

```java
dropIndex("ks", "my_idx");
// DROP INDEX ks.my_idx
```

You may also specify `ifExists`:

```java
dropIndex("my_idx").ifExists();
// DROP INDEX IF EXISTS my_idx
```

[SchemaBuilder]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/SchemaBuilder.html
