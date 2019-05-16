## DAOs

A DAO is an interface that defines a set of query methods. In general, those queries will relate to
the same [entity](../entities/) (although that is not a requirement).

It must be annotated with [@Dao]:

```java
@Dao
public interface ProductDao {
  @Select
  Product findById(UUID productId);

  @Insert
  void save(Product product);

  @Delete
  void delete(Product product);
}
```

### Query methods

To add queries, define methods on your interface and mark them with one of the following
annotations:

* [@Delete](delete/)
* [@GetEntity](getentity/)
* [@Insert](insert/)
* [@Query](query/)
* [@Select](select/)
* [@SetEntity](setentity/)
* [@Update](update/)

The methods can have any name. The allowed parameters and return type are specific to each
annotation.

### Runtime usage

To obtain a DAO instance, use a [factory method](../mapper/#dao-factory-methods) on the mapper
interface.

```java
InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
ProductDao dao = inventoryMapper.productDao("someKeyspace");
```

The returned object is thread-safe, and can securely be shared throughout your application.

[@Dao]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/Dao.html
