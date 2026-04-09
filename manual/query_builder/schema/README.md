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

# Schema builder

The schema builder is an additional API provided by [java-driver-query-builder](../README.md) that enables
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

The [general concepts](../README.md#general-concepts) and [non goals](../README.md#non-goals) defined for the query
builder also apply for the schema builder.

### Building DDL Queries

The schema builder offers functionality for creating, altering and dropping elements of a CQL
schema.  For a complete tour of the API, browse the child pages in the manual for each schema
element type:

* [keyspace](keyspace/README.md)
* [table](table/README.md)
* [index](index/README.md)
* [materialized view](materialized_view/README.md)
* [type](type/README.md)
* [function](function/README.md)
* [aggregate](aggregate/README.md)

[SchemaBuilder]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/SchemaBuilder.html
