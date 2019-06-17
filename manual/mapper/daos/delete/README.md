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

An optional IF clause can be added to the generated query. It can contain placeholders, for which
the method must have corresponding parameters (same name, and a compatible Java type):

```java
@Delete(entityClass = Product.class, customIfClause = "description = :expectedDescription")
void deleteIfDescriptionMatches(UUID productId, String expectedDescription);
```

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

Note that you can also return a boolean or result set for non-conditional queries, but there's no
practical purpose for that since those queries always return `wasApplied = true` and an empty result
set.

### Target keyspace and table

If a keyspace was specified [when creating the DAO](../../mapper/#dao-factory-methods), then the
generated query targets that keyspace. Otherwise, it doesn't specify a keyspace, and will only work
if the mapper was built from a session that has a [default keyspace] set.

If a table was specified when creating the DAO, then the generated query targets that table.
Otherwise, it uses the default table name for the entity (which is determined by the name of the
entity class and the naming convention).

[default keyspace]:       https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withKeyspace-com.datastax.oss.driver.api.core.CqlIdentifier-
[AsyncResultSet]:         https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/AsyncResultSet.html
[@ClusteringColumn]:      https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/ClusteringColumn.html
[@Delete]:                https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/Delete.html
[@PartitionKey]:          https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/PartitionKey.html
[ResultSet]:              https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/ResultSet.html
[ResultSet#wasApplied()]: https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/ResultSet.html#wasApplied--

[CompletionStage]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html
[CompletableFuture]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html