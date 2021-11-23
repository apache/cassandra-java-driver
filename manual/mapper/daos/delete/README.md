## Delete methods

Annotate a DAO method with [@Delete] to generate a query that deletes an [Entity](../../entities):

```java
@Dao
public interface ProductDao {
  @Delete
  void delete(Product product);
}
```

### Parameters

The method can operate on:

* an entity instance:

    ```java
    @Delete
    void delete(Product product);
    ```
    
* a primary key (partition key + clustering columns):

    ```java
    @Delete(entityClass = Product.class)
    void deleteById(UUID productId);
    ```
    
    In this case, the parameters must match the types of the [primary key
    columns](../../entities/#primary-key-columns), in the exact order (as defined by the
    [@PartitionKey] and [@ClusteringColumn] annotations). The parameter names don't necessarily need
    to match the names of the columns.
    
    In addition, because the entity class can't be inferred from the method signature, it must be
    specified via the annotation's `entityClass` element.

* a subset of the primary key.  As in the partition key, or partition key + subset of clustering 
  columns:

    ```java
    // given: PRIMARY KEY ((product_id, day), customer_id, ts)
    // delete all rows in partition
    @Delete(entityClass = ProductSale.class)
    void deleteByIdForDay(UUID productId, LocalDate day);

    // delete by partition key and partial clustering key
    @Delete(entityClass = ProductSale.class)
    void deleteByIdForCustomer(UUID productId, LocalDate day, UUID customerId);
   
    /* Note that the clustering columns in your primary key definition are significant. All
     * preceding clustering columns must be provided if any are.
     *
     * For example, the following is *NOT VALID* because ts is provided, but customer_id is
     * not. */
    @Delete(entityClass = ProductSale.class)
    void deleteByIdForTs(UUID productId, LocalDate day, long ts);
    ```

* a number of parameters matching the placeholder markers in `customWhereClause`, for which
  the parameters match the name and compatible java type of the markers:

    ```java
    @Delete(
        entityClass = ProductSale.class,
        customWhereClause =
            "id = :id and day = :day and customer_id = :customerId and ts >= :startTs and ts < :endTs")
    ResultSet deleteInTimeRange(UUID id, String day, int customerId, UUID startTs, UUID endTs);
    ```
    
An optional IF clause can be added to the generated query. Like `customWhereClause` it can contain 
placeholders:

```java
@Delete(entityClass = Product.class, customIfClause = "description = :expectedDescription")
void deleteIfDescriptionMatches(UUID productId, String expectedDescription);
```

A `Function<BoundStatementBuilder, BoundStatementBuilder>` or `UnaryOperator<BoundStatementBuilder>`
can be added as the **last** parameter. It will be applied to the statement before execution. This
allows you to customize certain aspects of the request (page size, timeout, etc) at runtime. See
[statement attributes](../statement_attributes/).

### Return type

The method can return:

* `void`.

* a `boolean` or `Boolean`, which will be mapped to [ResultSet#wasApplied()]. This is intended for
  IF EXISTS queries:

    ```java
    /** @return true if the product did exist */
    @Delete(ifExists = true)
    boolean deleteIfExists(Product product);
    ```
    
* a [ResultSet]. This is intended for queries with custom IF clauses; when those queries are not
  applied, they return the actual values of the tested columns.
  
    ```java
    @Delete(entityClass = Product.class, customIfClause = "description = :expectedDescription")
    ResultSet deleteIfDescriptionMatches(UUID productId, String expectedDescription);
    // if the condition fails, the result set will contain columns '[applied]' and 'description'
    ```
  
* a [BoundStatement]. This is intended for queries where you will execute this statement later
  or in a batch.
  
    ```java
    @Delete
    BoundStatement delete(Product product);
    ```
    
* a [CompletionStage] or [CompletableFuture] of any of the above. The method will execute the query
  asynchronously. Note that for result sets, you need to switch to [AsyncResultSet].
  
    ```java
    @Delete
    CompletableFuture<Void> deleteAsync(Product product);    
    
    @Delete(ifExists = true)
    CompletionStage<Boolean> deleteIfExistsAsync(Product product);

    @Delete(entityClass = Product.class, customIfClause = "description = :expectedDescription")
    CompletionStage<AsyncResultSet> deleteIfDescriptionMatchesAsync(UUID productId, String expectedDescription);
    ```
  
* a [ReactiveResultSet].

    ```java
    @Delete
    ReactiveResultSet deleteReactive(Product product);
    ```

* a [custom type](../custom_types).

Note that you can also return a boolean or result set for non-conditional queries, but there's no
practical purpose for that since those queries always return `wasApplied = true` and an empty result
set.

### Target keyspace and table

If a keyspace was specified [when creating the DAO](../../mapper/#dao-factory-methods), then the
generated query targets that keyspace. Otherwise, it doesn't specify a keyspace, and will only work
if the mapper was built from a session that has a [default keyspace] set.

If a table was specified when creating the DAO, then the generated query targets that table.
Otherwise, it uses the default table name for the entity (which is determined by the name of the
entity class and the [naming strategy](../../entities/#naming-strategy)).

[default keyspace]:       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withKeyspace-com.datastax.oss.driver.api.core.CqlIdentifier-
[AsyncResultSet]:         https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/AsyncResultSet.html
[@ClusteringColumn]:      https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/ClusteringColumn.html
[@Delete]:                https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Delete.html
[@PartitionKey]:          https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/PartitionKey.html
[ResultSet]:              https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/ResultSet.html
[ResultSet#wasApplied()]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/ResultSet.html#wasApplied--
[BoundStatement]:         https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/BoundStatement.html
[ReactiveResultSet]:      https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveResultSet.html


[CompletionStage]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html
[CompletableFuture]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html