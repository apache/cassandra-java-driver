## Select methods

Annotate a DAO method with [@Select] to generate a query that selects one or more rows, and maps
them to [Entities](../../entities):

```java
@Dao
public interface ProductDao {
  @Select
  Product findById(UUID productId);
}
```

### Parameters

If the annotation doesn't have a [customWhereClause()], the mapper defaults to a selection by
primary key (partition key + clustering columns). The method's parameters must match the types of
the [primary key columns](../../entities/#primary-key-columns), in the exact order (as defined by
the [@PartitionKey] and [@ClusteringColumn] annotations). The parameter names don't necessarily need
to match the names of the columns.

To select more than one entity within a partition, a subset of primary key components may be 
specified as long as enough parameters are provided to account for the partition key.

```java
// given: PRIMARY KEY ((product_id, day), customer_id, ts)
public interface ProductSaleDao {
  @Select
  PagingIterable<ProductSale> findByDay(UUID productId, LocalDate day);
  
  @Select
  PagingIterable<ProductSale> findByDayForCustomer(UUID productId, LocalDate day, UUID customerID);
  
  /* Note that the clustering columns in your primary key definition are significant. All
   * preceding clustering columns must be provided if any are.
   *
   * For example, the following is *NOT VALID* because ts is provided, but customer_id is
   * not. */
  @Select
  PagingIterable<ProductSale> findByDayForTs(UUID productId, LocalDate day, long ts);
}
```

To select all rows within a table, you may also provide no parameters.

```java
@Dao
public interface ProductDao {
  @Select
  PagingIterable<Product> all();
}
```

If the annotation has a [customWhereClause()], it completely replaces the WHERE clause. The provided
string can contain named placeholders. In that case, the method must have a corresponding parameter
for each, with the same name and a compatible Java type.

```java
@Select(customWhereClause = "description LIKE :searchString")
PagingIterable<Product> findByDescription(String searchString);
```

The generated SELECT query can be further customized with [limit()], [perPartitionLimit()],
[orderBy()], [groupBy()] and [allowFiltering()]. Some of these clauses can also contain placeholders
whose values will be provided through additional method parameters. Note that it is sometimes not
possible to determine if a parameter is a primary key component or a placeholder value; therefore
the rule is that **if your method takes a partial primary key, the first parameter that is not a
primary key component must be explicitly annotated with
[@CqlName](../../entities/#user-provided-names)**. For example if the primary key is `((day int,
hour int, minute int), ts timestamp)`:

```java
// Annotate 'l' so that it's not mistaken for the second PK component
@Select(limit = ":l")
PagingIterable<Sale> findDailySales(int day, @CqlName("l") int l);
```

A `Function<BoundStatementBuilder, BoundStatementBuilder>` or `UnaryOperator<BoundStatementBuilder>`
can be added as the **last** parameter. It will be applied to the statement before execution. This
allows you to customize certain aspects of the request (page size, timeout, etc) at runtime. See
[statement attributes](../statement_attributes/).

### Return type

In all cases, the method can return:

* the entity class itself. If the query returns no rows, the method will return `null`. If it
  returns more than one row, subsequent rows will be discarded.

    ```java
    @Select
    Product findById(UUID productId);
    ```

* an [Optional] of the entity class. If the query returns no rows, the method will return
  `Optional.empty()`. If it returns more than one row, subsequent rows will be discarded.

    ```java
    @Select
    Optional<Product> findById(UUID productId);
    ```

* a [PagingIterable] of the entity class. It behaves like a result set, except that each element is
  a mapped entity instead of a row.

    ```java
    @Select(customWhereClause = "description LIKE :searchString")
    PagingIterable<Product> findByDescription(String searchString);
    ```

* a [Stream] of the entity class. It behaves like a result set, except that each element is a mapped
  entity instead of a row.
  
    Note: even if streams are lazily evaluated, the query will be executed synchronously; also, as
    the returned stream is traversed, more blocking calls may occur, as more results are fetched
    from the server in the background. For details about the stream's characteristics, see
    [PagingIterable.spliterator].

    ```java
    @Select(customWhereClause = "description LIKE :searchString")
    Stream<Product> findByDescription(String searchString);
    ```

* a [CompletionStage] or [CompletableFuture] of any of the above. The method will execute the query
  asynchronously. Note that for iterables, you need to switch to the asynchronous equivalent
  [MappedAsyncPagingIterable].

    ```java
    @Select
    CompletionStage<Product> findByIdAsync(UUID productId);
    
    @Select
    CompletionStage<Optional<Product>> findByIdAsync(UUID productId);
    
    @Select(customWhereClause = "description LIKE :searchString")
    CompletionStage<MappedAsyncPagingIterable<Product>> findByDescriptionAsync(String searchString);
    ```

    For streams, even if the initial query is executed asynchronously, traversing the returned
    stream may block the traversing thread. Blocking calls can indeed be required as more results
    are fetched from the server in the background. For this reason, _the usage of
    `CompletionStage<Stream<T>>` cannot be considered as a fully asynchronous execution method_.
  
* a [MappedReactiveResultSet] of the entity class.

    ```java
    @Select(customWhereClause = "description LIKE :searchString")
    MappedReactiveResultSet<Product> findByDescriptionReactive(String searchString);
    ```

* a [custom type](../custom_types).

### Target keyspace and table

If a keyspace was specified [when creating the DAO](../../mapper/#dao-factory-methods), then the
generated query targets that keyspace. Otherwise, it doesn't specify a keyspace, and will only work
if the mapper was built from a session that has a [default keyspace] set.

If a table was specified when creating the DAO, then the generated query targets that table.
Otherwise, it uses the default table name for the entity (which is determined by the name of the
entity class and the [naming strategy](../../entities/#naming-strategy)).

[default keyspace]:          https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withKeyspace-com.datastax.oss.driver.api.core.CqlIdentifier-
[@ClusteringColumn]:         https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/ClusteringColumn.html
[@PartitionKey]:             https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/PartitionKey.html
[@Select]:                   https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Select.html
[allowFiltering()]:          https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Select.html#allowFiltering--
[customWhereClause()]:       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Select.html#customWhereClause--
[groupBy()]:                 https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Select.html#groupBy--
[limit()]:                   https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Select.html#limit--
[orderBy()]:                 https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Select.html#orderBy--
[perPartitionLimit()]:       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Select.html#perPartitionLimit--
[MappedAsyncPagingIterable]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/MappedAsyncPagingIterable.html
[PagingIterable]:            https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/PagingIterable.html
[PagingIterable.spliterator]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/PagingIterable.html#spliterator--
[MappedReactiveResultSet]:   https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/mapper/reactive/MappedReactiveResultSet.html

[CompletionStage]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html
[CompletableFuture]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html
[Optional]: https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html
[Stream]: https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html
