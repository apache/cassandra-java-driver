## SetEntity methods

Annotate a DAO method with [@SetEntity] to fill a core driver data structure from an
[Entity](../../entities):

```java
public interface ProductDao {
  @SetEntity
  BoundStatement bind(Product product, BoundStatement boundStatement);
}
```

The generated code will set each entity property on the target, such as:

```java
boundStatement = boundStatement.set("id", product.getId(), UUID.class);
boundStatement = boundStatement.set("description", product.getDescription(), String.class);
...
```

It does not perform a query. Instead, those methods are intended for cases where you will execute
the query yourself, and just need the conversion logic.

### Lenient mode

By default, the mapper operates in "strict" mode: the target statement must contain a matching
column for every property in the entity definition, *except computed ones*. If such a column is not
found, an error will be thrown.

Starting with driver 4.12.0, the `@SetEntity` annotation has a new `lenient` attribute. If this
attribute is explicitly set to `true`, the mapper will operate in "lenient" mode: all entity
properties that have a matching column in the target statement will be set. However, *unmatched
properties will be left untouched*.

As an example to illustrate how lenient mode works, assume that we have the following entity and
DAO:

```java
@Entity class Product {

  @PartitionKey int id;
  String description;
  float price;
  // other members omitted
}

interface ProductDao {

  @SetEntity(lenient = true)
  BoundStatement setLenient(Product product, BoundStatement stmt);

}
```

Then the following code would be possible:

```java
Product product = new Product(1, "scented candle", 12.99);
// stmt does not contain the price column
BoundStatement stmt = session.prepare("INSERT INTO product (id, description) VALUES (?, ?)").bind();
stmt = productDao.setLenient(product, stmt);
```

Since no `price` column was found in the target statement, `product.price` wasn't read (if the
statement is executed, the resulting row in the database will have a price of zero). Without lenient
mode, the code above would throw an error instead.

Lenient mode allows to achieve the equivalent of driver 3.x [manual mapping
feature](https://docs.datastax.com/en/developer/java-driver/3.10/manual/object_mapper/using/#manual-mapping).

**Beware that lenient mode may result in incomplete rows being inserted in the database.**

### Parameters

The method must have two parameters: one is the entity instance, the other must be a subtype of
[SettableByName] \(the most likely candidates are [BoundStatement], [BoundStatementBuilder] and
[UdtValue]). Note that you can't use [SettableByName] itself.

The order of the parameters does not matter.

The annotation can define a [null saving strategy](../null_saving/) that applies to the properties
of the object to set. This is only really useful with bound statements (or bound statement
builders): if the target is a [UdtValue], the driver sends null fields in the serialized form
anyway, so both strategies are equivalent.

### Return type

The method can either be void, or return the exact same type as its settable parameter.

```java
@SetEntity
void bind(Product product, UdtValue udtValue);

@SetEntity
void bind(Product product, BoundStatementBuilder builder);
```

Note that if the settable parameter is immutable, the method should return a new instance, because
the generated code won't be able to modify the argument in place. This is the case for
[BoundStatement], which is immutable in the driver:

```java
// Wrong: statement won't be modified
@SetEntity
void bind(Product product, BoundStatement statement);

// Do this instead:
@SetEntity
BoundStatement bind(Product product, BoundStatement statement);
```

If you use a void method with [BoundStatement], the mapper processor will issue a compile-time
warning.

[@SetEntity]:            https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/SetEntity.html
[BoundStatement]:        https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/BoundStatement.html
[BoundStatementBuilder]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/BoundStatementBuilder.html
[SettableByName]:        https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/data/SettableByName.html
[UdtValue]:              https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/data/UdtValue.html
