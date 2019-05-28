## Null saving strategies

The [@Insert](../insert/), [@Query](../query/), [@SetEntity](../setentity/) and
[@Update](../update/) annotations allow you to choose how null entity properties should be handled:

```java
import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.SET_TO_NULL;

@Update(nullSavingStrategy = SET_TO_NULL)
void update(Product product);
```

Two strategies are available:

* `DO_NOT_SET`: the mapper won't call the corresponding setter on the [BoundStatement]. The
  generated code looks approximately like this:

    ```java
    if (entity.getDescription() != null) {
      boundStatement = boundStatement.setString("description", entity.getDescription());
    }
    ```
    
    This avoids inserting tombstones for null properties. On the other hand, if the query is an
    update and the column previously had another value, it won't be overwritten.

    Note that unset values ([CASSANDRA-7304]) are only supported with [native
    protocol](../../../core/native_protocol/) v4 (Cassandra 2.2) or above . If you try to use this
    strategy with a lower Cassandra version, the mapper will throw an `IllegalArgumentException`
    when you try to build the corresponding DAO.

* `SET_TO_NULL`: the mapper will always call the setter, even with a null value. The generated code
  looks approximately like this:
  
    ```java
    // Called even if entity.getDescription() == null
    boundStatement = boundStatement.setString("description", entity.getDescription());
    ```
    
If you don't specify an explicit strategy for a method, the mapper defaults to `SET_TO_NULL`.

[BoundStatement]: https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/BoundStatement.html
      
[CASSANDRA-7304]: https://issues.apache.org/jira/browse/CASSANDRA-7304
