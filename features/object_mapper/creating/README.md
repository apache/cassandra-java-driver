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

The class must provide a default constructor, and all fields (except the
ones annotated `@Transient`) must have corresponding setters and
getters.

[table]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/Table.html
[case-sensitive]:http://docs.datastax.com/en/cql/3.3/cql/cql_reference/ucase-lcase_r.html
[consistency level]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/ConsistencyLevel.html

#### Column names

By default, the mapper tries to map each Java property to a
case insensitive column of the same name. If you want to use a different
name, or [case-sensitive] names, use the [@Column][column] annotation on
the field.

[column]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/Column.html

#### Primary key fields

[@PartitionKey][pk] and [@ClusteringColumn][cc] are used to indicate the
[partition key][pks] and [clustering columns][pks]. In the case of a
composite partition key or multiple clustering columns, the integer
value indicates the position of the column in the key:

```
CREATE TABLE sales(countryCode text, areaCode text, sales int,
                   PRIMARY KEY((countryCode, area Code)));
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

#### Enumerations

[@Enumerated][enum] is used to map Java enumerations. Since Cassandra
does not support enumerations, the object mapper defines two ways to
persist an enum value :

- `EnumType.ORDINAL` : the value is persisted as a Cassandra's `int` and
  its value is its position in the enum.
- `EnumType.STRING` : the value is persisted as a Cassandra's `text` and
  its value is its name in the enum.

According to the chosen type, the matching database column must be the
same type.

```
CREATE TABLE person (id uuid PRIMARY KEY, name text, gender int);
```

```java
@Enumerated(EnumType.ORDINAL)
private Gender gender;
```

[enum]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/Enumerated.html

#### Computed fields

[@Computed][computed] can be used on fields that are the result of a
computation on the Cassandra side. Typically a function call. Native
functions in Cassandra like `writetime()` or [User Defined Functions] are
supported.

```java
@Computed("ttl(name)")
Integer ttl;
```

The CQL return type of function must match the type of the field,
otherwise an exception will be thrown.

Computed fields are ignored when saving an entity.

`@Computed` does not support case-sensitivity. If the expression
contains case-sensitive column or function names, you'll have to escape
them:

```java
@Computed("\"myFunction\"(\"myColumn\")")
int f;
```

Finally, computed fields are only supported with [basic read
operations](../using/#basic-crud-operations) at this time.
Support in [accessors](../using/#accessors) in planned for a future
version (see
[JAVA-832](https://datastax-oss.atlassian.net/browse/JAVA-832)).

[User Defined Functions]:http://www.planetcassandra.org/blog/user-defined-functions-in-cassandra-3-0/
[computed]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/mapping/annotations/Computed.html

#### Transient fields

[@Transient][transient] can be used to prevent a field from being mapped.

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

The class must provide a default constructor, and all fields (except the
ones annotated `@Transient`) must have corresponding setters and
getters.

As in table entities, fields are mapped to an UDT field of the same
name by default. To declare a different name or use case-sensitivity,
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

(this also works with UDTs inside collections or other UDTs, with
arbitrary level of nesting)

[User Defined Types]:http://docs.datastax.com/en/developer/java-driver/2.1/java-driver/reference/userDefinedTypes.html
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
[@FrozenValue][frozenvalue]` are also provided for convenience:

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
