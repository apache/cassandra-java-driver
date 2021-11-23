## GetEntity methods

Annotate a DAO method with [@GetEntity] to convert a core driver data structure into one or more
[Entities](../../entities):

```java
@Dao
public interface ProductDao {
  @GetEntity
  Product asProduct(Row row);
}
```

The generated code will retrieve each entity property from the source, such as:

```java
Product product = new Product();
product.setId(row.get("id", UUID.class));
product.setDescription(row.get("description", String.class));
...
```

It does not perform a query. Instead, those methods are intended for cases where you already have a
query result, and just need the conversion logic.

### Lenient mode

By default, the mapper operates in "strict" mode: the source row must contain a matching column for
every property in the entity definition, *including computed ones*. If such a column is not found,
an error will be thrown.

Starting with driver 4.12.0, the `@GetEntity` annotation has a new `lenient` attribute. If this
attribute is explicitly set to `true`, the mapper will operate in "lenient" mode: all entity
properties that have a matching column in the source row will be set. However, *unmatched properties
will be left untouched*.

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

  @GetEntity(lenient = true)
  Product getLenient(Row row);

}
```

Then the following code would be possible:

```java
// row does not contain the price column
Row row = session.execute("SELECT id, description FROM product").one();
Product product = productDao.getLenient(row);
assert product.price == 0.0;
```

Since no `price` column was found in the source row, `product.price` wasn't set and was left to its
default value (0.0). Without lenient mode, the code above would throw an error instead.

Lenient mode allows to achieve the equivalent of driver 3.x [manual mapping
feature](https://docs.datastax.com/en/developer/java-driver/3.10/manual/object_mapper/using/#manual-mapping).

**Beware that lenient mode may result in incomplete entities being produced.**

### Parameters

The method must have a single parameter. The following types are allowed:

* [GettableByName] or one of its subtypes (the most likely candidates are [Row] and [UdtValue]).
* [ResultSet].
* [AsyncResultSet].

The data must match the target entity: the generated code will try to extract every mapped property,
and fail if one is missing.

### Return type

The method can return:

* a single entity instance. If the argument is a result set type, the generated code will extract
  the first row and convert it, or return `null` if the result set is empty.

    ````java
    @GetEntity
    Product asProduct(Row row);

    @GetEntity
    Product firstRowAsProduct(ResultSet resultSet);
    ```
  
* a [PagingIterable] of an entity class. In that case, the type of the parameter **must** be
  [ResultSet]. Each row in the result set will be converted into an entity instance.
  
    ```java
    @GetEntity
    PagingIterable<Product> asProducts(ResultSet resultSet);
    ```

* a [Stream] of an entity class. In that case, the type of the parameter **must** be [ResultSet].
  Each row in the result set will be converted into an entity instance.

    Note: even if streams are lazily evaluated, results are fetched synchronously; therefore, as the
    returned stream is traversed, blocking calls may occur, as more results are fetched from the
    server in the background. For details about the stream's characteristics, see
    [PagingIterable.spliterator].

    ```java
    @GetEntity
    Stream<Product> asProducts(ResultSet resultSet);
    ```

* a [MappedAsyncPagingIterable] of an entity class. In that case, the type of the parameter **must**
  be [AsyncResultSet]. Each row in the result set will be converted into an entity instance.
  
    ```java
    @GetEntity
    MappedAsyncPagingIterable<Product> asProducts(AsyncResultSet resultSet);
    ```

If the return type doesn't match the parameter type (for example [PagingIterable] for
[AsyncResultSet]), the mapper processor will issue a compile-time error.


[@GetEntity]:                https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/GetEntity.html
[AsyncResultSet]:            https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/AsyncResultSet.html
[GettableByName]:            https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/data/GettableByName.html
[MappedAsyncPagingIterable]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/MappedAsyncPagingIterable.html
[PagingIterable]:            https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/PagingIterable.html
[PagingIterable.spliterator]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/PagingIterable.html#spliterator--
[ResultSet]:                 https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/ResultSet.html
[Row]:                       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/Row.html
[UdtValue]:                  https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/data/UdtValue.html

[Stream]: https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html



