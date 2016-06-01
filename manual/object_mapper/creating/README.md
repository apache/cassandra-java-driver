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
public static class User {
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

The class must provide a default constructor (it can however be private).

The mapping of each table column can be customized through annotations placed on either a field declaration,
or on a Java bean property getter method. _Annotations on setters are not supported and will be
silently ignored by the mapper_.

The following mappings are thus equivalent:

```java
@Table(name = "users")
public static class User {

    // annotation on a field
    @PartitionKey
    @Column(name = "user_id")
    private UUID userId;

    public UUID getUserId() {
        return userId;
    }

    public void setUserId(UUID userId) {
        this.userId = userId;
    }

}

@Table(name = "users")
public static class User {

    private UUID userId;

    // annotation on a getter method
    @PartitionKey
    @Column(name = "user_id")
    public UUID getUserId() {
        return userId;
    }

    public void setUserId(UUID userId) {
        this.userId = userId;
    }

}

```

To explicitly exclude a Java bean property or a field from being mapped,
annotate either the field or the property getter method with `@Transient`.

The mapper tries to "bind" fields and accessor methods (getters and setters) together
into a single logical abstraction;
it is therefore advised that users follow the [Java bean property conventions]
(java-beans) when designing mapped classes. 

In particular, when reading or writing mapped property values, the mapper will first
try the property getter and setter methods, if they are available;
and if they are not, the mapper will try a direct access to the property's corresponding field.

[table]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/Table.html
[case-sensitive]:http://docs.datastax.com/en/cql/3.3/cql/cql_reference/ucase-lcase_r.html
[consistency level]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/ConsistencyLevel.html
[java-beans]:https://docs.oracle.com/javase/tutorial/javabeans/writing/properties.html

#### Column names

By default, the mapper tries to map each Java property to a
case insensitive column of the same name. If you want to use a different
name, or [case-sensitive] names, use the [@Column][column] annotation on
the property.

[column]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/Column.html

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

[pk]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/PartitionKey.html
[cc]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/ClusteringColumn.html
[pks]:http://thelastpickle.com/blog/2013/01/11/primary-keys-in-cql.html

#### Computed fields

[@Computed][computed] can be used on properties that are the result of a
computation on the Cassandra side, typically a function call. Native
functions in Cassandra like `writetime()` or [User Defined Functions] are
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
[computed]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/Computed.html

#### Transient properties

[@Transient][transient] can be used to prevent a field or a Java bean property from being mapped.
It should be placed on either the field declaration or the property getter method.

[transient]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/Transient.html

### Mapping User Types

[User Defined Types] can also be mapped by using [@UDT][udt]:

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

The class must provide a default constructor (it can however be private).

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
public static class Company {
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
[udt]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/UDT.html
[field]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/Field.html

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

[frozen]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/Frozen.html
[frozenkey]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/FrozenKey.html
[frozenvalue]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/FrozenValue.html


### Polymorphism support

By default, the mapper will consider all Java bean properties and all fields
(including private ones) found in an entity class or in a UDT class,
as well as in their superclasses and superinterfaces. 

This allows for a (basic) polymorphic mapping of a given class hierarchy
into different CQL tables or UDTs.

Currently, each concrete class must correspond to a table or a UDT,
and should be annotated accordingly. This corresponds to the so called
"Table per concrete class" mapping strategy provided by
some popular SQL mapping frameworks such as Hibernate.

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

It is usually recommended to adopt one strategy or the other for
placing annotations on class members (i.e., either always on fields, or always on getters), 
but not both at the same time, unless strictly required.
Usually, annotating getters is more flexible than annotating fields.

If the mapper needs to disambiguate a specific property mapping,
note that annotations on getters will always take precedence over annotations on fields.
Likewise, if a getter method is overridden in a subclass, annotations in both
methods will get merged together, and in case of conflict, the overriding method's annotations 
will take precedence over the overridden's.

