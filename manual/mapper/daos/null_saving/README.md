## Null saving strategy

The null saving strategy controls how null entity properties are handled when writing to the
database. It can be configured either for each method, or globally at the DAO level.

Two strategies are available:

* [DO_NOT_SET]: the mapper won't call the corresponding setter on the [BoundStatement]. The
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
    strategy with a lower Cassandra version, the mapper will throw an [MapperException] when you try
    to access the corresponding DAO.

* [SET_TO_NULL]: the mapper will always call the setter, even with a null value. The generated code
  looks approximately like this:
  
    ```java
    // Called even if entity.getDescription() == null
    boundStatement = boundStatement.setString("description", entity.getDescription());
    ```

### Method level

Specify `nullSavingStrategy` on the method annotation:

```java
import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.SET_TO_NULL;

@Update(nullSavingStrategy = SET_TO_NULL)
void update(Product product);
```

This applies to [@Insert](../insert/), [@Query](../query/), [@SetEntity](../setentity/) and
[@Update](../update/) (other method types don't need it since they don't write data).

### DAO level

Annotate your [DAO](../../daos/) interface with [@DefaultNullSavingStrategy]. Any method that does
not explicitly define its strategy inherits the DAO-level one:

```java
@Dao
@DefaultNullSavingStrategy(SET_TO_NULL)
public interface ProductDao {

  @Insert
  void insert(Product product); // inherits SET_TO_NULL

  @Update(nullSavingStrategy = DO_NOT_SET)
  void update(Product product); // uses DO_NOT_SET
}
```

If you don't define a DAO-level default, any method that does not declare its own value defaults to
[DO_NOT_SET]:

```java
@Dao
public interface ProductDao {

  @Insert
  void insert(Product product); // defaults to DO_NOT_SET
}
```

Note that you can use inheritance to set a common default for all your DAOs:

```java
@DefaultNullSavingStrategy(SET_TO_NULL)
public interface InventoryDao {}

@Dao
public interface ProductDao extends InventoryDao {
  @Insert
  void insert(Product product); // inherits SET_TO_NULL
}

@Dao
public interface UserDao extends InventoryDao {
  @Insert
  void insert(User user); // inherits SET_TO_NULL
}
```

[@DefaultNullSavingStrategy]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/DefaultNullSavingStrategy.html
[BoundStatement]:             https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/BoundStatement.html
[MapperException]:            https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/MapperException.html
[DO_NOT_SET]:                 https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/entity/saving/NullSavingStrategy.html#DO_NOT_SET
[SET_TO_NULL]:                https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/entity/saving/NullSavingStrategy.html#SET_TO_NULL
      
[CASSANDRA-7304]: https://issues.apache.org/jira/browse/CASSANDRA-7304
