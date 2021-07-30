## Entities

### Quick overview

POJO annotated with [@Entity], must expose a no-arg constructor.

* class-level annotations:
  * [@NamingStrategy]
  * [@CqlName]
  * [@HierarchyScanStrategy]
  * [@PropertyStrategy]
* field/method-level annotations:
  * [@PartitionKey], [@ClusteringColumn]
  * [@Computed]
  * [@Transient]
  * [@CqlName]
* can inherit annotated fields/methods and [@NamingStrategy]. Only use [@Entity] on concrete
  classes.

-----

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

Each entity property will be mapped to a CQL column. The way properties are detected is
configurable, as explained below:

### Property detection

#### Mutability

By default, the mapper expects mutable entity classes:

```java
@Entity
public class Product {
  @PartitionKey private UUID productId;

  public Product() {}

  public UUID getProductId() { return productId; }
  public void setProductId(UUID productId) { this.productId = productId; }
}
```

With mutable entities:

* each entity property:
  * **must** have a non-void, no-argument getter method.
  * **must** have a corresponding setter method: matching name, and exactly one argument matching
    the getter's return type. Note that the return type of the setter does not matter.
  * *may* have a corresponding field: matching name and type.
* the type **must** expose a non-private, no-argument constructor.

When the mapper reads a mutable entity from the database, it will invoke the no-argument
constructor to materialize the instance, and then read and set the properties one by one.

You can switch to an immutable style with the [@PropertyStrategy] annotation:

```java
@Entity
@PropertyStrategy(mutable = false)
public class ImmutableProduct {
  @PartitionKey private final UUID productId;

  public ImmutableProduct(UUID productId) { this.productId = productId; }

  public UUID getProductId() { return productId; }
}
```

With immutable entities:

* each entity property:
  * **must** have a non-void, no-argument getter method. The mapper will not look for a setter.
  * *may* have a corresponding field: matching name and type. You'll probably want to make that
    field final (although that has no impact on the mapper-generated code).
* the type **must** expose a non-private constructor that takes every
  non-[transient](#transient-properties) property, in the declaration order.
  
When the mapper reads an immutable entity from the database, it will first read all properties, then
invoke the "all columns" constructor to materialize the instance.

Note: the "all columns" constructor must take the properties in the order that they are declared in
the entity. If the entity inherits properties from parent types, those must come last in the
constructor signature, ordered from the closest parent to the farthest. If things get too
complicated, a good trick is to deliberately omit the constructor to let the mapper processor fail:
the error message describes the expected signature.

#### Accessor styles

By default, the mapper looks for JavaBeans-style accessors: getter prefixed with "get" (or "is" for
boolean properties) and, if the entity is mutable, setter prefixed with "set":

```java
@Entity
public class Product {
  @PartitionKey private UUID productId;

  public UUID getProductId() { return productId; }
  public void setProductId(UUID productId) { this.productId = productId; }
}
```

You can switch to a "fluent" style (no prefixes) with the [@PropertyStrategy] annotation:

```java
import static com.datastax.oss.driver.api.mapper.entity.naming.GetterStyle;
import static com.datastax.oss.driver.api.mapper.entity.naming.SetterStyle;

@Entity
@PropertyStrategy(getterStyle = GetterStyle.FLUENT, setterStyle = SetterStyle.FLUENT)
public class Product {
  @PartitionKey private UUID productId;

  public UUID productId() { return productId; }
  public void productId(UUID productId) { this.productId = productId; }
}
```

Note that if you use the fluent style with immutable entities, Java's built-in `hashCode()` and
`toString()` methods would qualify as properties. The mapper skips them automatically. If you have
other false positives that you'd like to ignore, mark them as [transient](#transient-properties).

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

#### Computed properties

Annotating an entity property with [@Computed] indicates that when retrieving data with the mapper
this property should be set to the result of a computation on the Cassandra side, typically a
function call:

```java
private int v;

@Computed("writetime(v)")
private long writetime;
```

The CQL return type of the formula must match the type of the property, otherwise an exception
will be thrown.

[@Computed] does not support case-sensitivity. If the expression contains case-sensitive column
or function names, you'll have to escape them:

```java
@Computed("\"myFunction\"(\"myColumn\")")
private int f;
```

[@Computed] fields are only used for select-based queries, so they will not be considered for
[@Update] or [@Insert] operations.

Also note that like all other properties, the expected name in a query result for a [@Computed]
property is based on the property name and the employed [@NamingStrategy](#naming-strategy). You may
override this behavior using [@CqlName](#user-provided-names).

Mapping computed results to property names is accomplished using [aliases].  If you wish to use
entities with [@Computed] properties with [@GetEntity] or [@Query]-annotated dao methods, you
must also do the same:

```java
@Entity
class MyEntity {
  @PartitionKey private int k;

  private int v;

  @Computed("ttl(v)")
  private int myTtl;

  @Computed("writetime(v)")
  @CqlName("ts")
  private long writetime;
}
```

would expect a [@Query] such as:

```java
@Dao
class MyDao {
  @Query("select k, v, ttl(v) as my_ttl, writetime(v) as ts from ${qualifiedTableId} where k=:id")
  MyEntity findById(int id);
}
```

#### Transient properties

In some cases, one may opt to exclude properties defined on an entity from being considered
by the mapper.  In this case, simply annotate these properties with [@Transient]:

```java
@Transient
private int notAColumn;
```

In addition, one may specify transient property names at the entity level by leveraging the
[@TransientProperties] annotation:

```java
@TransientProperties({"notAColumn", "x"})
@Entity
public class Product {
  @PartitionKey private UUID id;
  private String description;
  // these columns are not included because their names are specified in @TransientProperties
  private int notAColumn;
  private int x;
}
```

Finally, any field including the `transient` keyword modifier will also be considered transient,
i.e.:

```java
private transient int notAColumn;
```

#### Custom column name

Override the CQL name manually with [@CqlName], see [User-provided names](#user-provided-names)
above.

### Default keyspace

You can specify a default keyspace to use when doing operations on a given entity:

```java
@Entity(defaultKeyspace = "inventory")
public class Product {
  //....
}
```

This will be used when you build a DAO without an explicit keyspace parameter:

```java
@Mapper
public interface InventoryMapper {
  @DaoFactory
  ProductDao productDao();

  @DaoFactory
  ProductDao productDao(@DaoKeyspace String keyspace);
}

ProductDao productDao = mapper.productDao();
productDao.insert(product); // inserts into inventory.product

ProductDao productDaoTest = mapper.productDao("test");
productDaoTest.insert(product); // inserts into test.product
```

The default keyspace optional: if it is not specified, and you build a DAO without a keyspace, then
the session **must** have a default keyspace, otherwise an error will be thrown:

```java
@Entity
public class Product { ... }

CqlSession session = CqlSession.builder()
    .withKeyspace("default_ks")
    .build();
InventoryMapper mapper = new InventoryMapperBuilder(session).build();

ProductDao productDao = mapper.productDao();
productDao.insert(product); // inserts into default_ks.product
```

If you want the name to be case-sensitive, it must be enclosed in double-quotes, for example:

```java
@Entity(defaultKeyspace = "\"defaultKs\"")
```

### Inheritance

When mapping an entity class or a UDT class, the mapper will transparently scan superclasses and
parent interfaces for properties and annotations, thus enabling polymorphic mapping of one class
hierarchy into different CQL tables or UDTs.

Each concrete class must be annotated with [@Entity] and abstract classes and interfaces must not
use this annotation.

Here is an example of a polymorphic mapping:

```java
@Entity
static class Point2D {
  private int x;
  private int y;

  @CqlName("\"X\"")
  public int getX() { return x; }

  public void setX(int x) { this.x = x; }

  @CqlName("\"Y\"")
  public int getY() { return y; }

  public void setY(int y) { this.y = y; }
}

@Entity
static class Point3D extends Point2D {
  private int z;

  @CqlName("\"Z\"")
  public int getZ() { return z; }

  public void setZ(int z) {  this.z = z; }
}

abstract static class Shape {
  @PartitionKey // annotated field on superclass; annotation will get inherited in all subclasses
  protected UUID id;

  public abstract UUID getId();

  public void setId(UUID id) { this.id = id; }
}

@CqlName("rectangles")
@Entity
static class Rectangle extends Shape {
  private Point2D bottomLeft;
  private Point2D topRight;

  @CqlName("rect_id")
  @Override
  public UUID getId() { return id; }

  public Point2D getBottomLeft() { return bottomLeft; }

  public void setBottomLeft(Point2D bottomLeft) { this.bottomLeft = bottomLeft; }

  public Point2D getTopRight() { return topRight; }

  public void setTopRight(Point2D topRight) { this.topRight = topRight; }

  public double getWidth() { return Math.abs(topRight.getX() - bottomLeft.getX()); }

  public double getHeight() { return Math.abs(topRight.getY() - bottomLeft.getY()); }
}

@CqlName("circles")
@Entity
static class Circle extends Shape {
  @CqlName("center2d")
  protected Point2D center;

  protected double radius;

  @Override
  @CqlName("circle_id")
  public UUID getId() { return id; }

  public double getRadius() { return this.radius; }

  public Circle setRadius(double radius) {
    this.radius = radius;
    return this;
  }

  public Point2D getCenter() { return center; }

  public void setCenter(Point2D center) { this.center = center; }
}

@CqlName("spheres")
@Entity
static class Sphere extends Circle {

  @CqlName("sphere_id")
  @Override
  public UUID getId() { return id; }

  // overrides field annotation in Circle,
  // note that the property type is narrowed down to Point3D
  @CqlName("center3d")
  @Override
  public Point3D getCenter() { return (Point3D) center; }

  @Override
  public void setCenter(Point2D center) {
    assert center instanceof Point3D;
    this.center = center;
  }

  // overridden builder-style setter
  @Override
  public Sphere setRadius(double radius) {
    super.setRadius(radius);
    return this;
  }
}
```

The generated entity code should map to the following schema:

```
CREATE TYPE point2d ("X" int, "Y" int)
CREATE TYPE point3d ("X" int, "Y" int, "Z" int)
CREATE TABLE rectangles (rect_id uuid PRIMARY KEY, bottom_left frozen<point2d>, top_right frozen<point2d>)
CREATE TABLE circles (circle_id uuid PRIMARY KEY, center2d frozen<point2d>, radius double)
CREATE TABLE spheres (sphere_id uuid PRIMARY KEY, center3d frozen<point3d>, radius double)
```

Annotation priority is driven by proximity to the [@Entity] class. For example, in the code above 
the use of `@CqlName("sphere_id")` on `Sphere.getId()` overrides the annotation
`@CqlName("circle_id")` on `Circle.getId()` for the `Sphere` entity.

Annotations declared on classes are given priority over annotations declared by interfaces 
the same level.

To control how the class hierarchy is scanned, annotate classes with [@HierarchyScanStrategy].

[@ClusteringColumn]:    https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/ClusteringColumn.html
[@CqlName]:             https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/CqlName.html
[@Dao]:                 https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Dao.html
[@Entity]:              https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Entity.html
[NameConverter]:        https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/entity/naming/NameConverter.html
[NamingConvention]:     https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/entity/naming/NamingConvention.html
[@NamingStrategy]:      https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/NamingStrategy.html
[@PartitionKey]:        https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/PartitionKey.html
[@Computed]:            https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Computed.html
[@Select]:              https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Select.html
[@Insert]:              https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Insert.html
[@Update]:              https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Update.html
[@GetEntity]:           https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/GetEntity.html
[@Query]:               https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Query.html
[aliases]:              http://cassandra.apache.org/doc/latest/cql/dml.html?#aliases
[@Transient]:           https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Transient.html
[@TransientProperties]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/TransientProperties.html
[@HierarchyScanStrategy]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/HierarchyScanStrategy.html
[@PropertyStrategy]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/PropertyStrategy.html
