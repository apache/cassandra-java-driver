# Using custom codecs

The mapper can take advantage of [custom codecs](../../custom_codecs/)
to apply custom conversions to mapped columns and fields.

## Declaring codecs

Let's assume you have a table containing a timestamp column:

```
create table user(id int primary key, birth timestamp);
```

You've created a custom class, and the codec to convert it:

```java
public class MyCustomDate { ... }

public class MyCustomDateCodec extends TypeCodec<MyCustomDate> {
    public MyCustomDate() {
        super(DataType.timestamp(), MyCustomDate.class);
    }
    ...
}
```

### Pre-registered codecs

If you register your codec with the mapper's underlying `Cluster`, it
will be automatically available to the mapper:

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withCodecRegistry(
        new CodecRegistry().register(new MyCustomDateCodec())
    ).build();

MappingManager mappingManager = new MappingManager(cluster.connect());
```

You can normally create your mapped classes using your custom type,
without any additional configuration:

```java
@Table(name = "user")
public class User {
  @PartitionKey
  private int id;
  private MyCustomDate birth;

  ... // getters and setters
}
```

This also works in accessors:

```java
@Accessor
interface UserAccessor {
  @Query("update user set birth = :b where id = :i")
  void updateBirth(@Param("i") int id,
                   @Param("b") MyCustomDate birth);
}
```

### One-time declaration

Sometimes you might want to use your codec only for one particular
column/field. In that case you won't register it when initializing the
`Cluster`:

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build();

MappingManager mappingManager = new MappingManager(cluster.connect());
```

Instead, reference the codec's class in the [@Column][column]
annotation:

```java
@Table(name = "user")
public class User {
  @PartitionKey
  private int id;
  @Column(codec = MyCustomDateCodec.class)
  private MyCustomDate birth;

  ... // getters and setters
}
```

The class must have a no-arg constructor. The mapper will create an
instance (one per column) and cache it for future use.

This also works with [@Field][field] and [@Param][param] annotations.

[column]:http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/mapping/annotations/Column.html
[field]:http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/mapping/annotations/Field.html
[param]:http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/mapping/annotations/Param.html


## Implicit UDT codecs

The mapper uses custom codecs internally to handle UDT conversions: when
you register an [entity](../using/#entity-mappers), the mapper inspects
the type of all fields to find classes annotated with
[@UDT](../creating/#mapping-user-types) (this works recursively with
nested UDTs and collections). For each class, the mapper creates a codec
and registers it with the underlying `Cluster`.

```java
@UDT(name = "address")
public class Address { ... }

@Entity(name = "user")
public class User {
  ...
  private Address address;
  ...
}

Mapper<User> userMapper = mappingManager.mapper(User.class);

// Codec is now registered for Address <-> CQL address
```

A nice side-effect is that you can now use the `@UDT`-annotated class
with any driver method, not just mapper methods:

```java
Row row = session.execute("select address from user where id = 1").one();
Address address = row.get("address", Address.class);
```

If you don't use entity mappers but still want the convenience of the
UDT codec for core driver methods, the mapper provides a way to create
it independently:

```java
mappingManager.udtCodec(Address.class);

// Codec is now registered
```
