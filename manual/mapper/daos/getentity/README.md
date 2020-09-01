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
    
* a [MappedAsyncPagingIterable] of an entity class. In that case, the type of the parameter **must**
  be [AsyncResultSet]. Each row in the result set will be converted into an entity instance.
  
    ```java
    @GetEntity
    MappedAsyncPagingIterable<Product> asProducts(AsyncResultSet resultSet);
    ```

If the return type doesn't match the parameter type (for example [PagingIterable] for
[AsyncResultSet]), the mapper processor will issue a compile-time error.


[@GetEntity]:                https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/mapper/annotations/GetEntity.html
[AsyncResultSet]:            https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/core/cql/AsyncResultSet.html
[GettableByName]:            https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/core/data/GettableByName.html
[MappedAsyncPagingIterable]: https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/core/MappedAsyncPagingIterable.html
[PagingIterable]:            https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/core/PagingIterable.html
[ResultSet]:                 https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/core/cql/ResultSet.html
[Row]:                       https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/core/cql/Row.html
[UdtValue]:                  https://docs.datastax.com/en/drivers/java/4.9/com/datastax/oss/driver/api/core/data/UdtValue.html




