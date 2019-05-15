## Entities

An entity is a Java class that will be mapped to a Cassandra table or [UDT](../../core/udts).
Entities are used as arguments or return types of [DAO](../daos/) methods; they can also be nested
inside other entities (to map UDT columns).

In order to be detected by the mapper, the class must be annotated with [@Entity]:

```java
@Entity
public class Product {
  @PartitionKey private UUID productId;
  private String description;
  
  public UUID getProductId() { return productId; }
  public void setProductId(UUID productId) { this.productId = productId; }
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
}
```

Each entity property will be mapped to a CQL column. In order to detect a property, the mapper looks
for all of the following:

* a getter method that follows the usual naming convention (e.g. `getDescription`) and has no
  parameters. The name of the property is obtained by removing the "get" prefix and decapitalizing
  (`description`), and the type of the property is the return type of the getter.
* a matching setter method (`setDescription`), with a single parameter that has the same type as the
  property (the return type does not matter).
* optionally, a matching field (`description`) that has the same type as the property.

Note that the field is not mandatory, a property can have only a getter and a setter (for example if
the value is computed, or the field has a different name, or is nested into another field, etc.)
  
The class must expose a no-arg constructor that is at least package-private.

### Naming strategy

The mapper infers the database schema from your Java model: the entity class's name is converted
into a table name, and the property names into column names.

You can control the details of this conversion by annotating your entity class with
[@NamingStrategy].

#### Naming conventions

The simplest strategy is to use one of the mapper's built-in conventions: 

```java
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.UPPER_SNAKE_CASE;

@Entity
@NamingStrategy(convention = UPPER_SNAKE_CASE)
public class Product {
  @PartitionKey private UUID productId;
  ...
}
```

Conventions convert names according to pre-defined rules. For example, with the `UPPER_SNAKE_CASE`
convention used above, the mapper expects the following schema:

```
CREATE TABLE "PRODUCT"("PRODUCT_ID" int primary key ...)
```

For the list of all available conventions, look at the enum constants in [NamingConvention].

If you don't annotate your class with [@NamingStrategy], the mapper defaults to the
`SNAKE_CASE_INSENSITIVE` convention.

#### User-provided name converter

If none of the built-in conventions work for you, you can provide your own conversion logic by
implementing [NameConverter]:

```java
public class MyNameConverter implements NameConverter {
  @Override
  public String toCassandraName(String javaName) {
    ... // implement your logic here
  }
}
```

Then pass your converter class to the annotation:

```java
@Entity
@NamingStrategy(customConverterClass = MyNameConverter.class)
public class Product {
  ...
}
```

The mapper will use reflection to build an instance of the converter; it needs to expose a public
no-arg constructor.

Note that, unlike built-in conventions, the mapper processor cannot invoke your converter at compile
time and use the converted names directly in generated code. Instead, the generated code will invoke
the converter at runtime (that is, every time you run a query). If you want to squeeze the last bit
of performance from the mapper, we recommend sticking to conventions.

#### User-provided names

Finally, you can override the CQL name manually with the [@CqlName] annotation:

```java
@PartitionKey
@CqlName("id")
private UUID productId;
```

It works both on entity properties, and on the entity class itself.

This takes precedence over the entity-level naming strategy, so it's convenient if almost all of
your schema follows a convention, but you need exceptions for a few columns.

### Property annotations

Properties can be annotated to configure various aspects of the mapping. The annotation can be
either on the field, or on the getter (if both are specified, the mapper processor issues a
compile-time warning, and the field annotation will be ignored).

#### Primary key columns

If the entity maps to a table, properties that map to partition key columns must be annotated with
[@PartitionKey]:

```java
// CREATE TABLE sales(countryCode text, areaCode text, sales int,
//                    PRIMARY KEY((countryCode, areaCode)));

@PartitionKey(1)
private String countryCode;
@PartitionKey(2)
private String areaCode;
```

If the partition key is composite, the annotation's integer value indicates the position of each
property in the key. Note that any values can be used, but for clarity it's probably a good idea to
use consecutive integers starting at 0 or 1.

Similarly, properties that map to clustering columns must be annotated with [@ClusteringColumn]: 

```java
// CREATE TABLE sensor_reading(id uuid, year int, month int, day int, value double,
//                             PRIMARY KEY(id, year, month, day));
@PartitionKey
private UUID id;
@ClusteringColumn(1)
private int year;
@ClusteringColumn(2)
private int month;
@ClusteringColumn(3)
private int day;
```

This information is used by some of the DAO method annotations; for example,
[@Select](../daos/select/)'s default behavior is to generate a selection by primary key.

[@ClusteringColumn]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/ClusteringColumn.html
[@CqlName]:          http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/CqlName.html
[@Entity]:           http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/Entity.html
[NameConverter]:     http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/entity/naming/NameConverter.html
[NamingConvention]:  http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/entity/naming/NamingConvention.html
[@NamingStrategy]:   http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/NamingStrategy.html
[@PartitionKey]:     http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/PartitionKey.html