## DAOs

### Quick overview

Interface annotated with [@Dao].

* interface-level annotations:
  * [@DefaultNullSavingStrategy]
  * [@HierarchyScanStrategy]
* method-level annotations: query methods (see child pages).
* instantiated from a [@DaoFactory] method on the mapper.

-----

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
* [@QueryProvider](queryprovider/)
* [@Select](select/)
* [@SetEntity](setentity/)
* [@Update](update/)
* [@Increment](increment/)

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

### Inheritance

DAOs can benefit from inheriting methods from other interfaces.  This is useful when you
have a common set of query methods that could be shared between entities. For example, using the
class hierarchy defined in [Entity Inheritance], one may define a set of DAO interfaces in the 
following manner:

```java
interface BaseDao<T> {
  @Insert
  void save(T t);

  @Select
  T findById(UUID id);
  
  @SetEntity
  void bind(T t, BoundStatementBuilder builder);
}

@Dao
interface CircleDao extends BaseDao<Circle> {}

@Dao
interface RectangleDao extends BaseDao<Rectangle> {}

@Dao
interface SphereDao extends BaseDao<Sphere> {}

@Mapper
public interface ShapeMapper {
  @DaoFactory
  CircleDao circleDao(@DaoKeyspace CqlIdentifier keyspace);

  @DaoFactory
  RectangleDao rectangleDao(@DaoKeyspace CqlIdentifier keyspace);

  @DaoFactory
  SphereDao sphereDao(@DaoKeyspace CqlIdentifier keyspace);
}
```

Note that interfaces that declare generic type variables should not be annotated with
[@Dao].

In addition to inheriting methods from parent interfaces, interface-level annotations such as
[@DefaultNullSavingStrategy] are also inherited:

```java
import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.SET_TO_NULL;

@DefaultNullSavingStrategy(SET_TO_NULL)
interface BaseDao {
}

@Dao
interface RectangleDao extends BaseDao {}
```

Annotation priority is driven by proximity to the [@Dao]-annotated interface.  For example:

```java
import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.SET_TO_NULL;
import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.DO_NOT_SET;

@DefaultNullSavingStrategy(SET_TO_NULL)
interface BaseDao {
}

@Dao
@DefaultNullSavingStrategy(DO_NOT_SET)
interface RectangleDao extends BaseDao {}
```

In this case `@DefaultNullSavingStrategy(DO_NOT_SET)` on `RectangleDao` would override the 
annotation on `BaseDao`.

If two parent interfaces at the same level declare the same annotation, the priority of annotation 
chosen is controlled by the order the interfaces are declared, for example:

```java
interface RectangleDao extends Dao1, Dao2 {}
```

In this case, any annotations declared in `Dao1` would be chosen over `Dao2`.

To control how the hierarchy is scanned, annotate interfaces with [@HierarchyScanStrategy].

[@Dao]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Dao.html
[@DaoFactory]:   https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/DaoFactory.html
[@DefaultNullSavingStrategy]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/DefaultNullSavingStrategy.html
[@HierarchyScanStrategy]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/HierarchyScanStrategy.html
[Entity Inheritance]: ../entities/#inheritance
