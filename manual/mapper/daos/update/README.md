## Update methods

Annotate a DAO method with [@Update] to generate a query that updates one or more
[entities](../../entities):

```java
@Dao
public interface ProductDao {
  @Update
  void update(Product product);
}
```

### Parameters

The first parameter must be an entity instance. All of its non-PK properties will be interpreted as
values to update.

* If the annotation doesn't have a `customWhereClause`, the mapper defaults to an update by primary
  key (partition key + clustering columns). The WHERE clause is generated automatically, and bound
  with the PK components of the provided entity instance. The query will update at most one row.
  
* If the annotation has a `customWhereClause`, it completely replaces the WHERE clause. If the
  provided string contains placeholders, the method must have corresponding additional parameters
  (same name, and a compatible Java type):

    ```java
    @Update(customWhereClause = "description LIKE :searchString")
    void updateIfDescriptionMatches(Product product, String searchString);
    ```
    
    The PK components of the provided entity are ignored. Multiple rows may be updated.

If the query has a custom timestamp or TTL with placeholders, the method must have corresponding
additional parameters (same name, and a compatible Java type):

```java
@Update(timestamp = ":timestamp")
void updateWithTimestamp(Product product, long timestamp);

@Update(ttl = ":ttl")
void updateWithTtl(Product product, int ttl);
```

An optional IF clause can be appended to the generated query. It can contain placeholders, for which
the method must have corresponding parameters (same name, and a compatible Java type):

```java
@Update(customIfClause = "description = :expectedDescription")
ResultSet updateIfDescriptionMatches(Product product, String expectedDescription);
```

An optional IF EXISTS clause at the end of the generated UPDATE query. This is mutually exclusive
with customIfClause (if both are set, the mapper processor will generate a compile-time error):

```java
@Update(ifExists = true)
boolean updateIfExists(Product product);
```


### Return type

The method can return:

* `void`.

* a `boolean` or [Boolean], which will be mapped to `ResultSet#wasApplied()`. This is intended for
  conditional queries.
  
    ```java
    @Update(ifExists = true)
    boolean updateIfExists(Product product);
    ```
    
* a [ResultSet]. The method will return the raw query result, without any conversion. This is
  intended for queries with custom IF clauses; when those queries are not applied, they return the
  actual values of the tested columns.
  
    ```java
    @Update(customIfClause = "description = :expectedDescription")
    ResultSet updateIfExists(Product product);
    // if the condition fails, the result set will contain columns '[applied]' and 'description'
    ```

* a [CompletionStage] or [CompletableFuture] of any of the above. The mapper will execute the query
  asynchronously. 
  Note that for result sets, you need to switch to the asynchronous equivalent [AsyncResultSet].

    ```java
    @Update
    CompletionStage<Void> update(Product product);

    @Update(ifExists = true)
    CompletableFuture<Boolean> updateIfExists(Product product);

    @Update(customIfClause = "description = :expectedDescription")
    CompletableFuture<AsyncResultSet> updateIfDescriptionMatches(Product product, String expectedDescription);
    ```

### Target keyspace and table

If a keyspace was specified [when creating the DAO](../../mapper/#dao-factory-methods), then the
generated query targets that keyspace. Otherwise, it doesn't specify a keyspace, and will only work
if the mapper was built from a session that has a [default keyspace] set.

If a table was specified when creating the DAO, then the generated query targets that table.
Otherwise, it uses the default table name for the entity (which is determined by the name of the
entity class and the naming convention).

[default keyspace]: https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withKeyspace-com.datastax.oss.driver.api.core.CqlIdentifier-
[@Update]:          https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/Update.html

[AsyncResultSet]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/AsyncResultSet.html
[Boolean]: https://docs.oracle.com/javase/8/docs/api/index.html?java/lang/Boolean.html
[CompletionStage]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html
[CompletableFuture]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html
[ResultSet]:            http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/ResultSet.html
