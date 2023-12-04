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

## Query builder

The query builder is a utility to **generate CQL queries programmatically**. For example, it could
be used to:

* given a set of optional search parameters, build a search query dynamically depending on which
  parameters are provided;
* given a Java class, generate the CRUD queries that map instances of that class to a Cassandra
  table.

To use it in your application, add the following dependency:

```xml
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>java-driver-query-builder</artifactId>
  <version>${driver.version}</version>
</dependency>
```

Here is our canonical example rewritten with the query builder:

```java
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

try (CqlSession session = CqlSession.builder().build()) {
  
  Select query = selectFrom("system", "local").column("release_version"); // SELECT release_version FROM system.local
  SimpleStatement statement = query.build();
  
  ResultSet rs = session.execute(statement);
  Row row = rs.one();
  System.out.println(row.getString("release_version"));
}
```

### General concepts

#### Fluent API

All the starting methods are centralized in the [QueryBuilder] class. To get started, add the
following import:

```java
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
```

Choose the method matching your desired statement, for example `selectFrom`. Then use your IDE's
completion and the javadocs to add query parts:

```java
Select select =
    selectFrom("ks", "user")
          .column("first_name")
          .column("last_name")
          .whereColumn("id").isEqualTo(bindMarker());
// SELECT first_name,last_name FROM ks.user WHERE id=?
```

When your query is complete, you can either extract a raw query string, or turn it into a
[simple statement](../core/statements/simple) (or its builder):

```java
String cql = select.asCql();
SimpleStatement statement = select.build();
SimpleStatementBuilder builder = select.builder();
```

#### Immutability

All types in the fluent API are immutable. This means that every step creates a new object:

```java
SelectFrom selectFrom = selectFrom("ks", "user");

Select select1 = selectFrom.column("first_name"); // SELECT first_name FROM ks.user
Select select2 = selectFrom.column("last_name"); // SELECT last_name FROM ks.user

assert select1 != select2;
```

Immutability has great benefits:

* **thread safety**: you can share built queries across threads, without any race condition or
  badly published state.
* **zero sharing**: when you build multiple queries from a shared "base" (as in the example above),
  all the queries are totally independent, changes to one query will never "pollute" another.
  
On the downside, immutability means that the query builder creates lots of short-lived objects.
Modern garbage collectors are good at handling that, but still we recommend that you **avoid using
the query builder in your hot path**:

* favor [bound statements](../core/statements/prepared) for queries that are used often. You can
  still use the query builder and prepare the result:
  
  ```java
  // During application initialization:
  Select selectUser = selectFrom("user").all().whereColumn("id").isEqualTo(bindMarker());
  // SELECT * FROM user WHERE id=?
  PreparedStatement preparedSelectUser = session.prepare(selectUser.build());

  // At runtime:
  session.execute(preparedSelectUser.bind(userId));
  ```
* for queries that never change, build them when your application initializes, and store them in a
  field or constant for later.
* for queries that are built dynamically, consider using a cache.

#### Identifiers

All fluent API methods use [CqlIdentifier] for schema element names (keyspaces, tables, columns...).
But, for convenience, there are also `String` overloads that take the CQL form (as see [Case
sensitivity](../case_sensitivity) for more explanations).

For conciseness, we'll use the string-based versions for the examples in this manual.

### Non-goals

The query builder is **NOT**:

#### A crutch to learn CQL

While the fluent API guides you, it does not encode every rule of the CQL grammar. Also, it supports
a wide range of Cassandra versions, some of which may be more recent than your production target, or
not even released yet. It's still possible to generate invalid CQL syntax if you don't know what
you're doing.

You should always start with a clear idea of the CQL query, and write the builder code that produces
it, not the other way around.

#### A better way to write static queries

The primary use case of the query builder is dynamic generation. You will get the most value out of
it when you do things like: 

```java
// The columns to select are only known at runtime:
for (String columnName : columnNames) {
  select = select.column(columnName)
}

// If a search parameter is present, add the corresponding WHERE clause:
if (name != null) {
  select = select.whereColumn("name").isEqualTo(name);
}
```

If all of your queries could also be written as compile-time string constants, ask yourself what the
query builder is really buying you:  

```java
// Built version:
private static final Statement SELECT_USERS =
    selectFrom("user").all().limit(10).build();

// String version:
private static final Statement SELECT_USERS =
    SimpleStatement.newInstance("SELECT * FROM user LIMIT 10");
```

The built version:

* is slightly more expensive to build (admittedly, that is not really an issue for constants);
* is not more readable;
* is not necessarily less error-prone (see the previous section).

It eventually boils down to personal taste, but for simple cases you should consider raw strings as
a better alternative. 

### Building queries

For a complete tour of the API, browse the child pages in this manual:

* statement types:
  * [SELECT](select/)
  * [INSERT](insert/)
  * [UPDATE](update/)
  * [DELETE](delete/)
  * [TRUNCATE](truncate/)
  * [Schema builder](schema/) (for DDL statements such as CREATE TABLE, etc.)
* common topics:
  * [Relations](relation/)
  * [Conditions](condition/)
  * [Terms](term/)
  * [Idempotence](idempotence/)
  
[QueryBuilder]:  https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/querybuilder/QueryBuilder.html
[SchemaBuilder]: https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/querybuilder/SchemaBuilder.html
[CqlIdentifier]: https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/CqlIdentifier.html
