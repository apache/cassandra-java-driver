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

## Case sensitivity

### In Cassandra

Cassandra identifiers, such as keyspace, table and column names, are case-insensitive by default.
For example, if you create the following table:

```
cqlsh> CREATE TABLE test.FooBar(k int PRIMARY KEY);
```

Cassandra actually stores the table name as lower-case:

```
cqlsh> SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'test';

 table_name
------------
     foobar
```

And you can use whatever case you want in your queries:

```
cqlsh> SELECT * FROM test.FooBar;
cqlsh> SELECT * FROM test.foobar;
cqlsh> SELECT * FROM test.FoObAr;
```

However, if you enclose an identifier in double quotes, it becomes case-sensitive:

```
cqlsh> CREATE TABLE test."FooBar"(k int PRIMARY KEY);
cqlsh> SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'test';

 table_name
------------
     FooBar
```

You now have to use the exact, quoted form in your queries:

```
cqlsh> SELECT * FROM test."FooBar";
```

If you forget to quote, or use the wrong case, you'll get an error: 

```
cqlsh> SELECT * FROM test.Foobar;
InvalidRequest: Error from server: code=2200 [Invalid query] message="table foobar does not exist"

cqlsh> SELECT * FROM test."FOOBAR";
InvalidRequest: Error from server: code=2200 [Invalid query] message="table FOOBAR does not exist"
```

### In the driver

When we deal with identifiers, we use the following definitions:

* **CQL form**: how you would type it in a CQL query. In other words, case-sensitive if it's quoted,
  case-insensitive otherwise; 
* **internal form**: how it is stored in system tables. In other words, never quoted and always in
  its exact case.

In previous driver versions, identifiers were represented as raw strings. The problem is that this
does not capture the form; when a method processed an identifier, it always had to know where it
came from and what form it was in, and possibly convert it. This led a lot of internal complexity,
and recurring bugs.

To address this issue, driver 4+ uses a wrapper: [CqlIdentifier]. Its API methods are always
explicit about the form:

```java
CqlIdentifier caseInsensitiveId = CqlIdentifier.fromCql("FooBar");
System.out.println(caseInsensitiveId.asInternal());             // foobar
System.out.println(caseInsensitiveId.asCql(/*pretty=*/ false)); // "foobar"
System.out.println(caseInsensitiveId.asCql(true));              // foobar

// Double-quotes need to be escaped inside Java strings
CqlIdentifier caseSensitiveId = CqlIdentifier.fromCql("\"FooBar\"");
System.out.println(caseSensitiveId.asInternal());               // FooBar
System.out.println(caseSensitiveId.asCql(true));                // "FooBar"
System.out.println(caseSensitiveId.asCql(false));               // "FooBar"

CqlIdentifier caseSensitiveId2 = CqlIdentifier.fromInternal("FooBar");
assert caseSensitiveId.equals(caseSensitiveId2);
```

*Side note: as shown above, `asCql` has a pretty-printing option that omits the quotes if they are
not necessary. This looks nicer, but is slightly more expensive because it requires parsing the
string.*

The driver API uses `CqlIdentifier` whenever it produces or consumes an identifier. For example:

* getting the keyspace from a table's metadata: `CqlIdentifier keyspaceId =
  tableMetadata.getKeyspace()`;
* setting the keyspace when building a session: `CqlSession.builder().withKeyspace(keyspaceId)`.

For "consuming" methods, string overloads are also provided for convenience, for example
`SessionBuilder.withKeyspace(String)`.

* getters and setters of "data container" types [Row], [UdtValue], and [BoundStatement] follow
  special rules described [here][AccessibleByName] (these methods are treated apart because they are
  generally invoked very often, and therefore avoid to create `CqlIdentifier` instances internally); 
* in other cases, the string is always assumed to be in CQL form, and converted on the fly with
  `CqlIdentifier.fromCql`. 

[CqlIdentifier]:    https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/CqlIdentifier.html
[Row]:              https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/Row.html
[UdtValue]:         https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/data/UdtValue.html
[BoundStatement]:   https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/BoundStatement.html
[AccessibleByName]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/data/AccessibleByName.html

### Good practices

As should be clear by now, case sensitivity introduces a lot of extra (and arguably unnecessary)
complexity.

The Java Driver team's recommendation is:

> **Always use case-insensitive identifiers in your data model.**

You'll never have to create `CqlIdentifier` instances in your application code, nor think about
CQL/internal forms. When you pass an identifier to the driver, use the string-based methods. When
the driver returns an identifier and you need to convert it into a string, use `asInternal()`.

If you worry about readability, use snake case (`shopping_cart`), or simply stick to camel case
(`ShoppingCart`) and ignore the fact that Cassandra lower-cases everything internally.

The only reason to use case sensitivity should be if you don't control the data model. In that
case, either pass quoted strings to the driver, or use `CqlIdentifier` instances (stored as
constants to avoid creating them over and over).
