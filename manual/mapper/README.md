## Mapper

The mapper generates the boilerplate to execute queries and convert the results into
application-level objects.

It is published as two artifacts:

* the `java-driver-mapper-processor` module is **only needed in the compile classpath**, your
  application doesn't need to depend on it at runtime.

    ```xml
    <dependency>
      <groupId>com.datastax.oss</groupId>
      <artifactId>java-driver-mapper-processor</artifactId>
      <version>4.1.0</version>
    </dependency>
    ```
    
    See [Configuring the annotation processor](config/) for detailed instructions for different
    build tools.
    
* the `java-driver-mapper-runtime` module is a regular runtime dependency: 
    
    ```xml
    <dependency>
      <groupId>com.datastax.oss</groupId>
      <artifactId>java-driver-mapper-runtime</artifactId>
      <version>4.1.0</version>
    </dependency>
    ```

### Quick start

For a quick overview of mapper features, we are going to build a trivial example based on the
following schema:

```
CREATE KEYSPACE inventory
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE inventory.product(id uuid PRIMARY KEY, description text);
```

#### Entity class

This is a simple data container that will represent a row in the `product` table:

```java
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

@Entity
public class Product {

  @PartitionKey private UUID id;
  private String description;
  
  public Product() {}

  public Product(UUID id, String description) {
    this.id = id;
    this.description = description;
  }  

  public UUID getId() { return id; }

  public void setId(UUID id) { this.id = id; }

  public String getDescription() { return description; }

  public void setDescription(String description) { this.description = description; }
}
```

Entity classes must have a no-arg constructor; note that, because we also have a constructor that
takes all the fields, we have to define the no-arg constructor explicitly.

We use mapper annotations to mark the class as an entity, and indicate which field(s) correspond to
the primary key.

More annotations are available; for more details, see [Entities](entities/).

#### DAO interface

A DAO defines a set of query methods:

```java
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Select;

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

Again, mapper annotations are used to mark the interface, and indicate what kind of request each
method should execute. You can probably guess what they are in this example.

For the full list of available query types, see [DAOs](daos/).

#### Mapper interface

This is the top-level entry point to mapper features, that allows you to obtain DAO instances:

```java
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

@Mapper
public interface InventoryMapper {
  @DaoFactory
  ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
}
```

For more details, see [Mapper](mapper/).

#### Generating the code

The mapper uses *annotation processing*: it hooks into the Java compiler to analyze annotations, and
generate additional classes that implement the mapping logic. Annotation processing is a common
technique in modern frameworks, and is generally well supported by build tools and IDEs; this is
covered in detail in [Configuring the annotation processor](config/).

#### Using the generated code

One of the classes generated during annotation processing is `InventoryMapperBuilder`. It allows you
to initialize a mapper instance by wrapping a core driver session:

```java
CqlSession session = CqlSession.builder().build();
InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
```

The mapper should have the same lifecycle as the session in your application: created once at
initialization time, then reused. It is thread-safe.

From the mapper, you can then obtain a DAO instance and execute queries:

```java
ProductDao dao = productDao(CqlIdentifier.fromCql("inventory"));
dao.save(new Product(UUID.randomUUID(), "Mechanical keyboard"));
```