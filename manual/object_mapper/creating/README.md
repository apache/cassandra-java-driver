## Definition of mapped classes

The object mapper is configured by annotations on the mapped classes.

### Creating a table entity

A table entity is a class that will be mapped to a table in Cassandra.
It is annotated with [@Table][table]:

```
CREATE TABLE user (user_id uuid PRIMARY KEY, name text);
```

```java
@Table(keyspace = "ks", name = "users",
       readConsistency = "QUORUM",
       writeConsistency = "QUORUM",
       caseSensitiveKeyspace = false,
       caseSensitiveTable = false)
public class User {
    @PartitionKey
    @Column(name = "user_id")
    private UUID userId;
    private String name;
    // ... constructors / getters / setters
}
```

`@Table` takes the following options:

- `keyspace`: the keyspace for the table.
- `name`: the name of the table in the database.
- `caseSensitiveKeyspace`: whether the keyspace name is [case-sensitive].
- `caseSensitiveTable`: whether the table name is [case-sensitive].
- `readConsistency`: the [consistency level] that will be used on each
  get operation in the mapper. (if unspecified, it defaults to the
  cluster-wide setting)
- `writeConsistency`: the [consistency level] that will be used on each
  save, or delete operation in the mapper. (if unspecified, it defaults
  to the cluster-wide setting)

The class must provide a default constructor. The default constructor
is allowed to be non-public, provided that the security manager, if any, grants
the mapper access to it via [reflection][set-accessible].

#### Mapping table columns

The mapping of each table column can be customized through annotations 
placed on either a field declaration, or on a getter method of a 
[Java bean property][java-beans]. _Annotations on setters are not 
supported and will be silently ignored by the mapper_. 

Users are strongly encouraged to follow the Java bean property 
conventions when designing mapped classes: if this is the case, the mapper will
transparently "bind" together fields and accessor methods (getters and setters)
into a single logical abstraction; e.g. a field named `myCol` and a Java
bean property named `myCol` offering a public getter named `getMyCol()`
will be assumed to represent the very same table column,
and annotations for this column are thus allowed to be placed indifferently
on the field declaration, or on the getter method declaration.

The following mappings are thus equivalent:

```java
@Table(name = "users")
public class User {

    // annotation on a field
    @PartitionKey
    private UUID id;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

}
```

```java
@Table(name = "users")
public class User {

    private UUID id;

    // annotation on a getter method
    @PartitionKey
    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

}

```

It is recommended to adopt one strategy or the other for
placing annotations on class members (i.e., either always on fields, or always on getters), 
but not both at the same time, unless strictly required.
Annotating fields is usually less verbose, but with polymorphic
data models (see below), annotating getters generally proves to be more flexible.

If duplicated annotations for a given column are found on the field declaration
and on the getter method declaration, the one declared in the getter method
declaration wins. No warnings are generated in such situations.

When reading or writing mapped property values, the mapper will first
try to invoke the property getter and setter methods, if they are available;
and if they are not, the mapper will try a direct access to the property's corresponding field.

A mapped field is thus allowed to be non-public if:

1. Reads and writes are done through its public getters and setters; or
2. The security manager, if any, grants the mapper access to it via [reflection][set-accessible].

Note that, according to the [Java Beans specification][java-beans], a setter
must have a `void` return type; the driver, however, will consider as a setter any public method
having a matching signature (i.e., name and parameter types match those expected),
but having a different return type. This allows the usage of "fluent" setters that can
be chained together in a builder-pattern style:

```java
@Table(name = "users")
public class User {

    // fluent setters
    
    public User setId(UUID id) {
        this.id = id;
        return this;
    }

    public User setName(String name) {
        this.name = name;
        return this;
    }

}

// chained calls
User user = new User()
    .setId(UUIDs.random())
    .setName("John Doe");
```

[table]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/annotations/Table.html
[case-sensitive]:http://docs.datastax.com/en/cql/3.3/cql/cql_reference/ucase-lcase_r.html
[consistency level]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/ConsistencyLevel.html
[java-beans]:https://docs.oracle.com/javase/tutorial/javabeans/writing/properties.html
[set-accessible]:https://docs.oracle.com/javase/8/docs/api/java/lang/reflect/AccessibleObject.html#setAccessible-boolean-

#### Column names

By default, the mapper tries to map each Java bean property to a
column of the same name. _The mapping is case-insensitive by default_,
i.e., the property `userId` would map to a column named `userid`.

If you want to use a different name, or [case-sensitive] names, 
use the [@Column][column] annotation.

To specify a different column name, use the `name` attribute:

```
CREATE TABLE users(id uuid PRIMARY KEY, user_name text);
```

```java
// column name does not match field name
@Column(name = "user_name")
private String userName;
```

When case-sensitive identifiers are required, use the
`caseSensitive` attribute:

```
CREATE TABLE users(id uuid PRIMARY KEY, "userName" text);
```

```java
// column name is case-sensitive
@Column(caseSensitive = true)
private String userName;
```

[column]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/annotations/Column.html

#### Primary key fields

[@PartitionKey][pk] and [@ClusteringColumn][cc] are used to indicate the
[partition key][pks] and [clustering columns][pks]. In the case of a
composite partition key or multiple clustering columns, the integer
value indicates the position of the column in the key:

```
CREATE TABLE sales(countryCode text, areaCode text, sales int,
                   PRIMARY KEY((countryCode, areaCode)));
```

```java
@PartitionKey(0)
private String countryCode;
@PartitionKey(1)
private String areaCode;
```

The order of the indices must match that of the columns in the table
declaration.

[pk]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/annotations/PartitionKey.html
[cc]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/annotations/ClusteringColumn.html
[pks]:http://thelastpickle.com/blog/2013/01/11/primary-keys-in-cql.html

#### Computed fields

[@Computed][computed] can be used on properties that are the result of a
computation on the Cassandra side, typically a function call. Native
functions in Cassandra like `writetime()` or [User-Defined Functions] are
supported.

```java
@Computed("ttl(name)")
Integer ttl;
```

The CQL return type of function must match the type of the property,
otherwise an exception will be thrown.

Computed fields are ignored when saving an entity.

`@Computed` does not support case-sensitivity. If the expression
contains case-sensitive column or function names, you'll have to escape
them:

```java
@Computed("\"myFunction\"(\"myColumn\")")
int f;
```

Finally, computed properties are only supported with [basic read
operations](../using/#basic-crud-operations) at this time.
Support in [accessors](../using/#accessors) is planned for a future
version (see
[JAVA-832](https://datastax-oss.atlassian.net/browse/JAVA-832)).

[User Defined Functions]:http://www.planetcassandra.org/blog/user-defined-functions-in-cassandra-3-0/
[computed]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/annotations/Computed.html

#### Transient properties

By default, the mapper will try to map all fields and Java bean properties
to table columns. [@Transient][transient] can be used to prevent a field or 
a Java bean property from being mapped. Like other column-level annotations, 
it should be placed on either the field declaration or the property getter method.

[transient]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/annotations/Transient.html

### Mapping User Types

[User-Defined Types] can also be mapped by using [@UDT][udt]:

```
CREATE TYPE address (street text, zip_code int);
```

```java
@UDT(keyspace = "ks", name = "address")
class Address {
    private String street;
    @Field(name = "zip_code")
    private int zipCode;
    // ... constructors / getters / setters
}
```

`@UDT` takes the following options:

- `keyspace`: the keyspace for the UDT.
- `name`: the name of the UDT in the database.
- `caseSensitiveKeyspace`: whether the keyspace name is case-sensitive.
- `caseSensitiveType`: whether the UDT name is case-sensitive.

The class must provide a default constructor. The default constructor
is allowed to be non-public, provided that the security manager, if any, grants
the mapper access to it via [reflection][set-accessible].

As for table entities, properties in UDT classes are mapped to an UDT field of the same
name by default.

To declare a different name or use case-sensitivity,
use the [@Field][field] annotation:

```java
@Field(name = "zip_code")
private int zipCode;
```

When a table has a UDT column, the mapper will automatically map it to
the corresponding class:

```
CREATE TABLE company (company_id uuid PRIMARY KEY, name text, address address);
```

```java
public class Company {
    @PartitionKey
    @Column(name = "company_id")
    private UUID companyId;
    private String name;
    private Address address;
}
```

This also works with UDTs inside collections or other UDTs, with any arbitrary
nesting level.

[User Defined Types]: ../../udts/
[udt]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/annotations/UDT.html
[field]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/annotations/Field.html

### Mapping collections

Java collections will be automatically mapped into corresponding
Cassandra types. As in Cassandra, collections can contain all native
types and all [user types](#mapping-user-types) previously defined is
the database.

Collection and UDT fields should be annotated to indicate whether they are
frozen. Currently this is only for informational purposes (the mapper
won't check that the declarations match the rules in Cassandra).
However it is a good idea to keep using these annotations and make sure
they match the schema, in anticipation for the schema generation features
that will be added in a future version.
The default annotation is [@Frozen][frozen], [@FrozenKey][frozenkey] and
[@FrozenValue][frozenvalue] are also provided for convenience:

```java
// Will be mapped as a 'list<text>'
private List<String> stringList;

// Will be mapped as a 'frozen<list<text>>'
@Frozen
private List<String> frozenStringList;

// Will be mapped as 'map<frozen<address>, frozen<list<text>>>'
@FrozenKey
@FrozenValue
private Map<Address, List<String>> frozenKeyValueMap;

// Will be mapped as 'map<text, frozen<list<address>>>'
@FrozenValue
private Map<String, List<Address>> frozenValueMap;
```

With regards to tuples, these can be represented as `TupleValue` fields, i.e.:

```java
@Frozen
private TupleValue myTupleValue;
```

Please note however that tuples are not a good fit for the mapper since it is up to the user to
resolve the associated `TupleType` when creating and accessing `TupleValue`s and properly use the
right types since java type information is not known.

Also note that `@UDT`-annotated classes are not implicitly registered with `TupleValue` like they
otherwise are because the mapper is not able to identify the cql type information at the time
entities are constructed.

To work around this, one may use [udtCodec] to register a `TypeCodec` that the mapper can use
to figure out how to appropriately handle UDT conversion, i.e.:

```java
mappingManager.udtCodec(Address.class);
```

[frozen]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/annotations/Frozen.html
[frozenkey]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/annotations/FrozenKey.html
[frozenvalue]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/annotations/FrozenValue.html
[udtCodec]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/mapping/MappingManager.html#udtCodec-java.lang.Class-

#### Prefer Frozen Collections

If `Mapper.save` is used to create and update entities, it is recommended to
use frozen collections over non-frozen collections.

Frozen collections in Cassandra are serialized as a single cell value where
non-frozen collections serialize each individual element in a collection as a
cell.

Since `Mapper.save` provides the entire collection for an entity field value on
each invocation, it is more efficient to use frozen collections as the entire
collection is serialized as one cell.

Also, when using non-frozen collections, on INSERT Cassandra must
create a tombstone to invalidate all existing collection elements, even if
there are none. When using frozen collections, no such tombstone is needed.

See [Freezing collection types] for more information about the frozen keyword.

[Freezing collection types]: https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/refCollectionType.html

### Polymorphism support

When mapping an entity class or a UDT class, the mapper will transparently
scan superclasses and superinterfaces for annotations on fields and getter methods,
thus enabling the polymorphic mapping of one class hierarchy
into different CQL tables or UDTs.

Each concrete class must correspond to a table or a UDT,
and should be annotated accordingly with `@Table` or `@UDT`. 
This is analogous to the so-called "table per concrete class" 
mapping strategy usually found in popular SQL mapping frameworks, such as Hibernate.

Here is an example of a polymorphic mapping:

```sql
CREATE TYPE point2d (x int, y int);
CREATE TYPE point3d (x int, y int, z int);
CREATE TABLE rectangles (id uuid PRIMARY KEY, bottom_left frozen<point2d>, top_right frozen<point2d>);
CREATE TABLE spheres (id uuid PRIMARY KEY, center frozen<point3d>, radius double);
```

```java
@UDT(name = "point2d")
public class Point2D {
    public int x;
    public int y;
}

@UDT(name = "point3d")
public class Point3D extends Point2D {
    public int z;
}

public interface Shape2D {

    @Transient
    double getArea();

}

public interface Shape3D {

    @Transient
    double getVolume();

}

public abstract class Shape {

    private UUID id;

    @PartitionKey
    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }
    
}

@Table(name = "rectangles")
public class Rectangle extends Shape implements Shape2D {

    private Point2D bottomLeft;
    private Point2D topRight;

    @Column(name = "bottom_left")
    @Frozen
    public Point2D getBottomLeft() {
        return bottomLeft;
    }

    public void setBottomLeft(Point2D bottomLeft) {
        this.bottomLeft = bottomLeft;
    }

    @Column(name = "top_right")
    @Frozen
    public Point2D getTopRight() {
        return topRight;
    }

    public void setTopRight(Point2D topRight) {
        this.topRight = topRight;
    }

    @Transient
    public double getWidth() {
        return Math.abs(topRight.x - bottomLeft.x);
    }

    @Transient
    public double getHeight() {
        return Math.abs(topRight.y - bottomLeft.y);
    }

    @Override
    public double getArea() {
        return getWidth() * getHeight();
    }
    
}

@Table(name = "spheres")
public class Sphere extends Shape implements Shape3D {

    private Point3D center;
    private double radius;

    @Frozen
    public Point3D getCenter() {
        return center;
    }

    public void setCenter(Point3D center) {
        this.center = center;
    }

    public double getRadius() {
        return radius;
    }

    public void setRadius(double radius) {
        this.radius = radius;
    }
    
    @Override
    public double getVolume() {
        return 4d / 3d * Math.PI * Math.pow(getRadius(), 3);
    }

}
```

One powerful advantage of annotating getter methods is that
annotations are inherited from overridden methods in superclasses and superinterfaces;
in other words, if a getter method is overridden in a subclass, annotations in both
method declarations will get merged together. If duplicated annotations are found during this merge
process, the overriding method's annotations will take precedence over the overridden's.
