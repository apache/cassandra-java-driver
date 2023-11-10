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

## Query methods

Annotate a DAO method with [@Query] to provide your own query string:

```java
@Dao
public interface SensorReadingDao {
  @Query("SELECT count(*) FROM sensor_readings WHERE id = :id")
  long countById(int id);
}
```

This is the equivalent of what was called "accessor methods" in the driver 3 mapper.

### Parameters

The query string will typically contain CQL placeholders. The method's parameters must match those
placeholders: same name and a compatible Java type.

```java
@Query("SELECT count(*) FROM sensor_readings WHERE id = :id AND year = :year")
long countByIdAndYear(int id, int year);
```

The annotation can define a [null saving strategy](../null_saving/) that applies to the method
parameters.

A `Function<BoundStatementBuilder, BoundStatementBuilder>` or `UnaryOperator<BoundStatementBuilder>`
can be added as the **last** parameter. It will be applied to the statement before execution. This
allows you to customize certain aspects of the request (page size, timeout, etc) at runtime. See
[statement attributes](../statement_attributes/).

### Return type

The method can return:

* `void`.

* a `boolean` or `Boolean`, which will be mapped to [ResultSet#wasApplied()]. This is intended for
  conditional queries.
  
* a `long` or `Long`, which will be mapped to the first column of the first row, expecting CQL type
  `BIGINT`. This is intended for count queries. The method will fail if the result set is empty, or
  does not match the expected format.
  
* a [Row]. This means the result is not converted, the mapper only extracts the first row of the
  result set and returns it. The method will return `null` if the result set is empty.

* a single instance of an [Entity](../../entities/) class. The method will extract the first row and
  convert it, or return `null` if the result set is empty.
  
* an [Optional] of an entity class. The method will extract the first row and convert
    it, or return `Optional.empty()` if the result set is empty.

* a [ResultSet]. The method will return the raw query result, without any conversion.

* a [BoundStatement]. This is intended for queries where you will execute this statement later
  or in a batch.

* a [PagingIterable]. The method will convert each row into an entity instance.

* a [Stream]. The method will convert each row into an entity instance. For details about the
  stream's characteristics, see [PagingIterable.spliterator].

* a [CompletionStage] or [CompletableFuture] of any of the above. The method will execute the query
  asynchronously. Note that for result sets and iterables, you need to switch to the asynchronous
  equivalent [AsyncResultSet] and [MappedAsyncPagingIterable] respectively.
  
* a [ReactiveResultSet], or a [MappedReactiveResultSet] of the entity class.

* a [custom type](../custom_types).

### Target keyspace and table

To avoid hard-coding the keyspace and table name, the query string supports 3 additional
placeholders: `${keyspaceId}`, `${tableId}` and `${qualifiedTableId}`. They get substituted at DAO
initialization time, with the [keyspace and table that the DAO was built
with](../../mapper/#dao-factory-methods).

For example, given the following:

```java
@Dao
public interface TestDao {
  @Query("SELECT * FROM ${keyspaceId}.${tableId}")
  ResultSet queryFromKeyspaceAndTable();

  @Query("SELECT * FROM ${qualifiedTableId}")
  ResultSet queryFromQualifiedTable();
}

@Mapper
public interface TestMapper {
  @DaoFactory
  TestDao dao(@DaoKeyspace String keyspace, @DaoTable String table);

  @DaoFactory
  TestDao dao(@DaoTable String table);
}

TestDao dao1 = mapper.dao("ks", "t");
TestDao dao2 = mapper.dao("t");
```

Then:

* `dao1.queryFromKeyspaceAndTable()` and `dao1.queryFromQualifiedTable()` both execute `SELECT *
  FROM ks.t`.
* `dao2.queryFromKeyspaceAndTable()` fails: no keyspace was specified for this DAO, so
  `${keyspaceId}` can't be substituted.
* `dao1.queryFromQualifiedTable()` executes `SELECT * FROM t`. In other words, `${qualifiedTableId}`
  uses the keyspace if it is available, but resolves to the table name only if it isn't. Whether the
  query succeeds or not depends on whether the session that the mapper was built with has a [default
  keyspace].

[default keyspace]:          https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withKeyspace-com.datastax.oss.driver.api.core.CqlIdentifier-
[@Query]:                    https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/mapper/annotations/Query.html
[AsyncResultSet]:            https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/AsyncResultSet.html
[ResultSet]:                 https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/ResultSet.html
[ResultSet#wasApplied()]:    https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/ResultSet.html#wasApplied--
[MappedAsyncPagingIterable]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/MappedAsyncPagingIterable.html
[PagingIterable]:            https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/PagingIterable.html
[PagingIterable.spliterator]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/PagingIterable.html#spliterator--
[Row]:                       https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/Row.html
[BoundStatement]:            https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/BoundStatement.html
[ReactiveResultSet]:         https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/cql/reactive/ReactiveResultSet.html
[MappedReactiveResultSet]:   https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/mapper/reactive/MappedReactiveResultSet.html

[CompletionStage]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html
[CompletableFuture]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html
[Optional]: https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html
[Stream]: https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html
