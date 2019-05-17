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

If the annotation doesn't have a `customWhereClause`, the mapper defaults to a selection by primary
key (partition key + clustering columns). The method's parameters must match the types of the
[primary key columns](../../entities/#primary-key-columns), in the exact order (as defined by the
[@PartitionKey] and [@ClusteringColumn] annotations). The parameter names don't necessarily need to
match the names of the columns.

If the annotation has a `customWhereClause`, it completely replaces the WHERE clause. The provided
string can contain named placeholders. In that case, the method must have a corresponding parameter
for each, with the same name and a compatible Java type.

```java
@Select(customWhereClause = "description LIKE :searchString")
PagingIterable<Product> findByDescription(String searchString);
```

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

### Target keyspace and table

If a keyspace was specified [when creating the DAO](../../mapper/#dao-factory-methods), then the
generated query targets that keyspace. Otherwise, it doesn't specify a keyspace, and will only work
if the mapper was built from a session that has a [default keyspace] set.

If a table was specified when creating the DAO, then the generated query targets that table.
Otherwise, it uses the default table name for the entity (which is determined by the name of the
entity class and the naming convention).

[default keyspace]:          https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withKeyspace-com.datastax.oss.driver.api.core.CqlIdentifier-
[@ClusteringColumn]:         https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/ClusteringColumn.html
[@PartitionKey]:             https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/PartitionKey.html
[@Select]:                   https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/Select.html
[MappedAsyncPagingIterable]: https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/MappedAsyncPagingIterable.html
[PagingIterable]:            https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/PagingIterable.html

[CompletionStage]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html
[CompletableFuture]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html
[Optional]: https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html
