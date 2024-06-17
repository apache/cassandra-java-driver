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

## Function

User-defined functions (UDF) enable users to create user code written in JSR-232 compliant scripting
languages that can be evaluated in CQL queries.  [SchemaBuilder] offers API methods for creating
and dropping UDFs.

### Creating a Function (CREATE FUNCTION)

To start a `CREATE FUNCTION` query, use `createFunction` in [SchemaBuilder]:

```java
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

CreateFunctionStart create = createFunction("log");
```

Like all other `CREATE` queries, one may supply `ifNotExists()` to require that the UDF should only
be created if it doesn't already exist, i.e.:

```java
CreateFunctionStart create = createFunction("cycling", "log").ifNotExists();
```

You may also specify that you would like to replace an existing function by the same signature if it
exists.  In this case, use `orReplace`:

```java
CreateFunctionStart create = createFunction("cycling", "log").orReplace();
```

One may also specify the parameters of a function using `withParameter`:

```
createFunction("cycling", "left")
    .withParameter("colName", DataTypes.TEXT)
    .withParameter("num", DataTypes.DOUBLE)
```

There are a number of steps that must be executed to complete a function:

* Specify whether the function is called on null input (`calledOnNull`) or if it should simply
  return null (`returnsNullOnNull`).
* Specify the return type of the function using `returnsType`
* Specify language of the function body using `withJavaLanguage`, `withJavaScriptLanguage`, or
  `withLanguage`
* Specify the function body with `as` or `asQuoted`

For example, the following defines a complete `CREATE FUNCTION` statement:

```java
createFunction("cycling", "log")
    .withParameter("input", DataTypes.DOUBLE)
    .calledOnNull()
    .returnsType(DataTypes.DOUBLE)
    .withJavaLanguage()
    .asQuoted("return Double.valueOf(Math.log(input.doubleValue()));");

// CREATE FUNCTION cycling.log (columnname text,num int) CALLED ON NULL INPUT RETURNS double LANGUAGE java
// AS 'return Double.valueOf(Math.log(input.doubleValue()));'
```

Note that when providing a function body, the `as` method does not implicitly quote your function
body.  If you would like to have the API handle this for you, use `asQuoted`.  This will surround
your function body in single quotes if the body itself does not contain a single quote, otherwise it
will surround your function body in two dollar signs mimicking a postgres-style string
literal, i.e.:

```java
createFunction("sayhi")
    .withParameter("input", DataTypes.TEXT)
    .returnsNullOnNull()
    .returnsType(DataTypes.TEXT)
    .withJavaScriptLanguage()
    .asQuoted("'hi ' + input;");
// CREATE FUNCTION sayhi (input text) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE javascript AS $$ 'hi ' + input; $$
```


### Dropping a Function (DROP FUNCTION)

To create a `DROP FUNCTION` query, use `dropFunction`:

```java
dropFunction("cycling", "log");
// DROP FUNCTION cycling.log
```

You may also specify `ifExists`:

```java
dropFunction("log").ifExists();
// DROP FUNCTION IF EXISTS log
```

[SchemaBuilder]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/SchemaBuilder.html
