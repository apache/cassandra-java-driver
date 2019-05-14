## Statement attributes

The [@Delete](../delete/), [@Insert](../insert/), [@Query](../query/), [@Select](../select/) and
[@Update](../update/) annotations allow you to control some aspects of the execution of the
underlying statement, such as the consistency level, timeout, etc.

### As a parameter

If the **last** parameter of any of those methods is a [StatementAttributes], it will automatically
be used to customize the statement:

```java
@Dao
public interface ProductDao {
  @Select
  Product findById(int productId, StatementAttributes attributes);
}

StatementAttributes attributes =
    StatementAttributes.builder()
        .withTimeout(Duration.ofSeconds(3))
        .withConsistencyLevel(DefaultConsistencyLevel.QUORUM)
        .build();
Product product = dao.findById(1, attributes);
``` 

Use this if you need to execute the same DAO methods with different configurations that can change
dynamically.

Note that the default implementation of [StatementAttributes] returned by the builder is immutable.
If you use the same combinations often, you can store them as constants to reduce allocation costs. 

[StatementAttributes]:  https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/StatementAttributes.html
