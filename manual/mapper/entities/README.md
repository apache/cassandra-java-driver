## Entities

An entity is a Java class that will be mapped to a Cassandra table or [UDT](../../core/udts).
Entities are used as arguments or return types of [DAO](../daos/) methods; they can also be nested
inside other entities (to map UDT columns).

In order to be detected by the mapper, the class must be annotated with [@Entity]:

```java
@Entity
public class Product {
  @PartitionKey private UUID id;
  private String description;
  
  public UUID getId() { return id; }
  public void setId(UUID id) { this.id = id; }
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
}
```

Each entity property will be mapped to a CQL column. In order to detect a property, the mapper looks
for:

* a getter method that follows the usual naming convention (e.g. `getDescription`) and has no
  parameters. The name of the property is obtained by removing the "get" prefix and decapitalizing
  (`description`), and the type of the property is the return type of the getter.
* a matching setter method (`setDescription`), with a single parameter that has the same type as the
  property (the return type does not matter).
* optionally, a matching field (`description`) that has the same type as the property.

Note that the field is not mandatory, a property can have only a getter and a setter (for example if
the value is computed, or the field has a different name, or is nested into another field, etc.)
  
The class must expose a no-arg constructor that is at least package-private.

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
[@Entity]:           http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/Entity.html
[@PartitionKey]:     http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/mapper/annotations/PartitionKey.html