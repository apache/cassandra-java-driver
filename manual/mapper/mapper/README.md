<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Mapper interface

The mapper interface is the top-level entry point to mapping features. It wraps a core driver
session, and acts as a factory of [DAO](../daos/) objects that will be used to execute requests.

It must be annotated with [@Mapper]:

```java
@Mapper
public interface InventoryMapper {
  @DaoFactory
  ProductDao productDao();
}
```

### Mapper builder

For each mapper interface, a builder is generated. By default, it resides in the same package, and
is named by appending a "Builder" suffix, for example `InventoryMapper => InventoryMapperBuilder`.

You can also use the `builderName()` element to specify a different name. It must be
fully-qualified:

```java
@Mapper(builderName = "com.acme.MyCustomBuilder")
public interface InventoryMapper {
  ...
}
```

The builder allows you to create a mapper instance, by wrapping a core `CqlSession` (if you need
more details on how to create a session, refer to the [core driver documentation](../core/)).

```java
CqlSession session = CqlSession.builder().build();
InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
```

One nice trick you can use is to create a static factory method on your interface. This hides the
name of the generated class from the rest of your application:

```java
@Mapper
public interface InventoryMapper {
  
  static MapperBuilder<InventoryMapper> builder(CqlSession session) {
    return new InventoryMapperBuilder(session);
  }
  ...
}

InventoryMapper inventoryMapper = InventoryMapper.builder(session).build();
```

Like the session, the mapper is a long-lived object: you should create it once at initialization
time, and reuse it for the entire lifetime of your application. It doesn't need to get closed. It is
thread-safe.

### DAO factory methods

The mapper's main goal is to provide DAO instances. Your interface should provide one or more
methods annotated with [@DaoFactory], that return a DAO interface:

```java
@DaoFactory
ProductDao productDao();
```

These methods can also receive a keyspace and/or table identifier as parameters (how those
parameters affect the returned DAO is explained in the next section). They must be annotated with
[@DaoKeyspace] and [@DaoTable] respectively, and be of type `String` or [CqlIdentifier]:

```java
@DaoFactory
ProductDao productDao(@DaoKeyspace String keyspace, @DaoTable String table);

@DaoFactory
ProductDao productDao(@DaoKeyspace String keyspace);

@DaoFactory
ProductDao productDao(@DaoTable CqlIdentifier table);
```

The mapper maintains an interface cache. Calling a factory method with the same arguments will yield
the same DAO instance:

```java
ProductDao dao1 = inventoryMapper.productDao("keyspace1", "product");
ProductDao dao2 = inventoryMapper.productDao("keyspace1", "product");
assert dao1 == dao2;
```

### DAO parameterization

The mapper allows you to reuse the same DAO interface for different tables. For example, given the
following definitions:

```java
@Dao
public interface ProductDao {
  @Select
  Product findById(UUID productId);
}

@Mapper
public interface InventoryMapper {
  @DaoFactory
  ProductDao productDao();
  
  @DaoFactory
  ProductDao productDao(@DaoKeyspace String keyspace);
  
  @DaoFactory
  ProductDao productDao(@DaoKeyspace String keyspace, @DaoTable String table);
}

ProductDao dao1 = inventoryMapper.productDao();
ProductDao dao2 = inventoryMapper.productDao("keyspace2");
ProductDao dao3 = inventoryMapper.productDao("keyspace3", "table3");
```

* `dao1.findById` executes the query `SELECT ... FROM product WHERE id = ?`. No table name was
  specified for the DAO, so it uses the default name for the `Product` entity (which in this case is
  the entity name converted with the default [naming strategy](../entities/#naming-strategy)). No
  keyspace was specified either, so the table is unqualified, and this DAO will only work with a
  session that was built with a default keyspace:

    ```java
    CqlSession session = CqlSession.builder().withKeyspace("keyspace1").build();
    InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
    ProductDao dao1 = inventoryMapper.productDao();
    ```

* `dao2.findById` uses the DAO's keyspace, and the default table name: `SELECT ... FROM
  keyspace2.product WHERE id = ?`.

* `dao3.findById` uses the DAO's keyspace and table name: `SELECT ... FROM keyspace3.table3 WHERE id
  = ?`.

The DAO's keyspace and table can also be injected into custom query strings; see [Query
methods](../daos/query/).

[CqlIdentifier]: https://docs.datastax.com/en/drivers/java/4.1/com/datastax/oss/driver/api/core/CqlIdentifier.html
[@DaoFactory]:   https://docs.datastax.com/en/drivers/java/4.1/com/datastax/oss/driver/api/mapper/annotations/DaoFactory.html
[@DaoKeyspace]:  https://docs.datastax.com/en/drivers/java/4.1/com/datastax/oss/driver/api/mapper/annotations/DaoKeyspace.html
[@DaoTable]:     https://docs.datastax.com/en/drivers/java/4.1/com/datastax/oss/driver/api/mapper/annotations/DaoTable.html
[@Mapper]:       https://docs.datastax.com/en/drivers/java/4.1/com/datastax/oss/driver/api/mapper/annotations/Mapper.html
