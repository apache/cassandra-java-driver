## Insert methods

Annotate a DAO method with [@Insert] to generate a query that inserts an [Entity](../../entities):

```java
@Dao
public interface ProductDao {
  @Insert
  void insert(Product product);
}
```

### Parameters

The first parameter must be the entity to insert.

If the annotation defines a TTL and/or timestamp with placeholders, the method must have
corresponding additional parameters (same name, and a compatible Java type):

```java
@Insert(ttl = ":ttl")
void insertWithTtl(Product product, int ttl);
```

The annotation can define a [null saving strategy](../null_saving/) that applies to the properties
of the entity to insert.

A `Function<BoundStatementBuilder, BoundStatementBuilder>` or `UnaryOperator<BoundStatementBuilder>`
can be added as the **last** parameter. It will be applied to the statement before execution. This
allows you to customize certain aspects of the request (page size, timeout, etc) at runtime. See
[statement attributes](../statement_attributes/).

### Return type

The method can return:

* `void`.

* the entity class. This is intended for `INSERT ... IF NOT EXISTS` queries. The method will return
  `null` if the insertion succeeded, or the existing entity if it failed.

    ```java
    @Insert(ifNotExists = true)
    Product insertIfNotExists(Product product);
    ```

* an [Optional] of the entity class, as a null-safe alternative for `INSERT ... IF NOT EXISTS`
  queries.

    ```java
    @Insert(ifNotExists = true)
    Optional<Product> insertIfNotExists(Product product);
    ```

* a `boolean` or `Boolean`, which will be mapped to [ResultSet#wasApplied()]. This is intended for
  IF NOT EXISTS queries:

    ```java
    /** @return true if the product did not exist */
    @Insert(ifNotExists = true)
    boolean saveIfNotExists(Product product);
    ```
    
* a [ResultSet]. This is intended for cases where you intend to inspect data associated with the
  result, such as [ResultSet#getExecutionInfo()]:
  
    ```java
    @Insert
    ResultSet save(Product product);
    ```
* a [BoundStatement]. This is intended for cases where you intend to execute this statement later or in a batch:
  
    ```java
    @Insert
    BoundStatement save(Product product);
    ```    
    
* a [CompletionStage] or [CompletableFuture] of any of the above. The mapper will execute the query
  asynchronously.

    ```java
    @Insert
    CompletionStage<Void> insert(Product product);

    @Insert(ifNotExists = true)
    CompletableFuture<Product> insertIfNotExists(Product product);

    @Insert(ifNotExists = true)
    CompletableFuture<Optional<Product>> insertIfNotExists(Product product);
    ```

* a [ReactiveResultSet].

    ```java
    @Insert
    ReactiveResultSet insertReactive(Product product);
    ```

* a [custom type](../custom_types).

### Target keyspace and table

If a keyspace was specified [when creating the DAO](../../mapper/#dao-factory-methods), then the
generated query targets that keyspace. Otherwise, it doesn't specify a keyspace, and will only work
if the mapper was built from a session that has a [default keyspace] set.

If a table was specified when creating the DAO, then the generated query targets that table.
Otherwise, it uses the default table name for the entity (which is determined by the name of the
entity class and the [naming strategy](../../entities/#naming-strategy)).

[default keyspace]:             https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withKeyspace-com.datastax.oss.driver.api.core.CqlIdentifier-
[@Insert]:                      https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Insert.html
[ResultSet]:                    https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/ResultSet.html
[ResultSet#wasApplied()]:       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/ResultSet.html#wasApplied--
[ResultSet#getExecutionInfo()]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/ResultSet.html#getExecutionInfo--
[BoundStatement]:               https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/BoundStatement.html
[ReactiveResultSet]:            https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveResultSet.html

[CompletionStage]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html
[CompletableFuture]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html
[Optional]: https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html
