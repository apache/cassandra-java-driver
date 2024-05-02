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

## Aggregate

Aggregates enable users to apply User-defined functions (UDF) to rows in a data set and combine
their values into a final result, for example average or standard deviation.  [SchemaBuilder]
offers API methods for creating and dropping aggregates.

### Creating an aggregate (CREATE AGGREGATE)

To start a `CREATE AGGREGATE` query, use `createAggregate` in [SchemaBuilder]:

```java
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

CreateAggregateStart create = createAggregate("average");
```

Like all other `CREATE` queries, one may supply `ifNotExists()` to require that the aggregate should
only be created if it doesn't already exist, i.e.:

```java
CreateAggregateStart create = createAggregate("cycling", "average").ifNotExists();
```

You may also specify that you would like to replace an existing aggregate by the same signature if
it exists.  In this case, use `orReplace`:

```java
CreateAggregateStart create = createAggregate("cycling", "average").orReplace();
```

One may also specify the parameters of an aggregate using `withParameter`:

```java
CreateAggregateStart create = createAggregate("cycling", "average")
    .withParameter(DataTypes.INT);
```

To complete an aggregate, one must then provide the following:

* The state function (`withSFunc`) to execute on each row
* The type of the value returned by the state function (`withSType`)

In addition, while optional, it is typical that the following is also provided:

* The final function to be executed after the state function is evaluated against all rows
  (`withFinalFunc`)
* The initial condition (`withInitCond`) which defines the initial value(s) to be passed in to the
  first parameter of the state function.

For example, the following defines a complete `CREATE AGGREGATE` statement:

```java
createAggregate("cycling", "average")
    .withParameter(DataTypes.INT)
    .withSFunc("avgstate")
    .withSType(DataTypes.tupleOf(DataTypes.INT, DataTypes.BIGINT))
    .withFinalFunc("avgfinal")
    .withInitCond(tuple(literal(0), literal(0)));

// CREATE AGGREGATE cycling.average (int) SFUNC avgstate STYPE tuple<int, bigint> FINALFUNC avgfinal INITCOND (0,0)
```

### Dropping an aggregate (DROP AGGREGATE)

To create a `DROP AGGREGATE` query, use `dropAggregate`:

```java
dropAggregate("cycling", "average");
// DROP AGGREGATE cycling.average
```

You may also specify `ifExists`:

```java
dropAggregate("average").ifExists();
// DROP AGGREGATE IF EXISTS average
```

[SchemaBuilder]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/SchemaBuilder.html
