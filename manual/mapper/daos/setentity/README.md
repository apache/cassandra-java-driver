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

[@SetEntity]:            https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/mapper/annotations/SetEntity.html
[BoundStatement]:        https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/core/cql/BoundStatement.html
[BoundStatementBuilder]: https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/core/cql/BoundStatementBuilder.html
[SettableByName]:        https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/core/data/SettableByName.html
[UdtValue]:              https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/core/data/UdtValue.html
