## Custom Codecs

Custom codecs support transparent, user-configurable mapping of CQL types to arbitrary Java objects.

Practical use cases that justify such a feature are numerous:

* Ability to map CQL timestamp, date and time columns to Java 8 or Joda Time classes (rather than the built-in mappings for Date, [LocalDate] and Long);
* Ability to map CQL varchar columns directly to JSON or XML mapped objects (i.e. using frameworks such as Jackson);
* Ability to map CQL user-defined types to Java objects;
* Ability to map CQL lists directly to Java arrays;
* Ability to map CQL collections directly to Scala collections;
* etc.

This page explains the implementation and how to write your own custom codecs. Note that the driver also provides a set
of [optional codecs](extras/) that might fit your needs.

### Overview of the serialization mechanism

The central piece in the serialization mechanism is [TypeCodec].

Each `TypeCodec` supports a bidirectional mapping between a Java type and a CQL type. A `TypeCodec` is thus capable of 4 basic operations:

* [Serialize][serialize] a Java object into a CQL value;
* [Deserialize][deserialize] a CQL value into a Java object;
* [Format][TypeCodec.format] a Java object into a CQL literal;
* [Parse][TypeCodec.parse] a CQL literal into a Java object.

Additionally, it implements inspection methods that verify if the codec is suitable for a specific
CQL type and/or Java object.

`TypeCodec` can be freely subclassed by users willing to implement their own serialization logic.
However, users are only required to do so when they want to extend
the driver's default set of codecs by registering additional codecs that handle
Java types that the driver is otherwise unable to process. For all other users,
the built-in set of codecs that is bundled with the driver provides
exactly the same functionality that the driver used to offer so far,
with out-of-the-box conversion from all CQL types to standard Java equivalents.

Refer to the [javadocs][TypeCodec] of `TypeCodec` for more information.

`TypeCodec`s are centralized in a [CodecRegistry] instance.
Whenever the driver needs to perform one of the aforementioned operations,
it will lookup for an appropriate codec in a `CodecRegistry`.
`CodecRegistry` is also used to manually register user-created codecs (see below for detailed instructions).
It also caches codec lookup results for faster execution whenever possible.

Refer to the [javadocs][CodecRegistry] of `CodecRegistry` for more information.

### Implementing and using custom codecs

Let's consider the following scenario: a user has JSON documents stored in a varchar column, and wants
the driver to automatically map that column to a Java object using the [Jackson] library,
instead of returning the raw JSON string.

The (admittedly simplistic) table structure is as follows:

```sql
CREATE TABLE t (id int PRIMARY KEY, json VARCHAR);
```

The first step is to implement a suitable codec. Using Jackson, here is what a Json codec could look like:

```java
/**
 * A simple Json codec.
 */
public class JsonCodec<T> extends TypeCodec<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonCodec(Class<T> javaType) {
        super(DataType.varchar(), javaType);
    }

    @Override
    public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null)
            return null;
        try {
            return ByteBuffer.wrap(objectMapper.writeValueAsBytes(value));
        } catch (JsonProcessingException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null)
            return null;
        try {
            byte[] b = new byte[bytes.remaining()];
            // always duplicate the ByteBuffer instance before consuming it!
            bytes.duplicate().get(b);
            return (T) objectMapper.readValue(b, toJacksonJavaType());
        } catch (IOException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
    }

    @Override
    public String format(T value) throws InvalidTypeException {
        if (value == null)
            return "NULL";
        String json;
        try {
            json = objectMapper.writeValueAsString(value);
        } catch (IOException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
        return '\'' + json.replace("\'", "''") + '\'';
    }

    @Override
    @SuppressWarnings("unchecked")
    public T parse(String value) throws InvalidTypeException {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;
        if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
            throw new InvalidTypeException("JSON strings must be enclosed by single quotes");
        String json = value.substring(1, value.length() - 1).replace("''", "'");
        try {
            return (T) objectMapper.readValue(json, toJacksonJavaType());
        } catch (IOException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
    }

    protected JavaType toJacksonJavaType() {
        return TypeFactory.defaultInstance().constructType(getJavaType().getType());
    }

}
```

A few implementation guidelines:

* Your codecs should be thread-safe, or better yet, immutable;
* Your codecs should be fast: do not forget that codecs are executed often and are usually very "hot" pieces of code;
* Your codecs should never block.

The second step is to register your codec with a `CodecRegistry` instance:

```java
JsonCodec<MyPojo> myJsonCodec = new JsonCodec<MyPojo>(MyPojo.class);
CodecRegistry myCodecRegistry = cluster.getConfiguration().getCodecRegistry();
myCodecRegistry.register(myJsonCodec);
```

As you can see, the easiest way to do so is to access the `Cluster`'s `CodecRegistry`.
By default, `Cluster` instances will use `CodecRegistry.DEFAULT_INSTANCE`, 
which should be adequate for most users.

It is however possible to create a `Cluster` using a different `CodecRegistry`:

```java
CodecRegistry myCodecRegistry = new CodecRegistry();
Cluster cluster = new Cluster.builder().withCodecRegistry(myCodecRegistry).build();
```

Note: when you instantiate a new `CodecRegistry`, it automatically registers all the default codecs used by the driver.
This ensures that the new registry will not lack of an essential codec. *You cannot deregister nor override 
an already-registered codec, only register new ones*. If you try to register a codec that would override
an existing one, the driver will log a warning and ignore it.

From now on, your custom codec is fully operational. It will be used every time the driver encounters
a `MyPojo` instance when executing queries, or when you ask it to retrieve a `MyPojo` instance from a `ResultSet`.

For example, here is how to save a `MyPojo` object:

```java
Cluster cluster = ...
Session session = ...
MyPojo myPojo = ...
// Using SimpleStatement
Statement stmt = new SimpleStatement("INSERT INTO t (id, json) VALUES (?, ?)", 42, myPojo));
// Using the Query Builder
BuiltStatement insertStmt = QueryBuilder.insertInto("t")
    .value("id", 42)
    .value("json", myPojo);
// Using BoundStatements
PreparedStatement ps = session.prepare("INSERT INTO t (id, json) VALUES (?, ?)");
BoundStatement bs1 = ps.bind(42, myPojo); // or alternatively...
BoundStatement bs2 = ps.bind()
    .setInt(0, 42)
    .set(1, myPojo, MyPojo.class);
```

And here is how to retrieve a `MyPojo` object converted from a JSON document:

```java
ResultSet rs = session.execute(...);
Row row = rs.one();
// Let the driver convert the string for you...
MyPojo myPojo = row.get(1, MyPojo.class);
// ... or retrieve the raw string if you need it
String json = row.get(1, String.class); // row.getString(1) would have worked too
```

Tip: `Row` and `BoundStatement` have several methods to set and retrieve values.
But if you plan to use custom codecs,
make sure that you use preferably the `get()` and `set()` methods: they
avoid any ambiguity by requiring the user to explicitly specify the desired Java type,
thus forcing the driver to pick the right codec for the right task.

Custom codecs also work with the driver's [object mapper](../object_mapper/custom_codecs/).

### On-the-fly codec generation

`CodecRegistry` instances not only store default codecs and user-defined codecs;
they can also create new codecs on the fly, based on the set of codecs they currently hold.
They can manage the following mappings:

* Collections (list, sets and maps) of known types. For example, if you registered a `TypeCodec<A>`, you get `List<A>>` handled for free. This works recursively for nested collections;
* [UserType] instances are automatically mapped to [UDTValue] objects. All registered codecs are available recursively to the UDT's fields;
* [TupleType] instances are automatically mapped to [TupleValue] objects (with the same rules for nested fields);
* [CustomType] instances are automatically mapped to [ByteBuffer] instances.

This way, the user does not have to manually register all derived codecs for a given "base" codec.
However, other combinations of Java and CQL types not listed above cannot have their codecs created on the fly;
such codecs must be manually registered.

If the codec registry encounters a mapping that it can't handle automatically, a [CodecNotFoundException]
is thrown.

Thanks to on-the-fly codec generation, it is possible to leverage the `JsonCodec` from our previous
example to retrieve lists of `MyPojo` objects stored in a CQL column of type `list<text>`, without
the need to explicitly declare a codec for this specific CQL type:

```java
JsonCodec<MyPojo> myJsonCodec = new JsonCodec<MyPojo>(MyPojo.class);
myCodecRegistry.register(myJsonCodec);
Row row = ...
List<MyPojo> myPojos = row.getList("jsonList", MyPojo.class); // works out of the box!
```

### Creating custom codecs for user-defined types (UDTs)

By default, the driver maps user-defined type values to [UDTValue] instances.

If you want to register a custom codec for a specific user-defined type, 
follow these steps:

Let's suppose you have a keyspace containing the following type and table:

```sql
CREATE TYPE address (street VARCHAR, zipcode INT);
CREATE TABLE users (id UUID PRIMARY KEY, name VARCHAR, address frozen<address>);
```

And let's suppose that you want to map the type `address` to an `Address` class:

```java
public class Address {

    private final String street;

    private final int zipcode;

    public Address(String street, int zipcode) {
        this.street = street;
        this.zipcode = zipcode;
    }

    // getters, setters, equals() and hashcode() omitted
}
```

First of all, create a codec that knows how to serialize this class; one possible solution 
(but not necessarily the most efficient one) would be to convert `Address` instances into `UDTValue`
instances, then serialize them with another codec:


```java
public class AddressCodec extends TypeCodec<Address> {

    private final TypeCodec<UDTValue> innerCodec;
    
    private final UserType userType;

    public AddressCodec(TypeCodec<UDTValue> innerCodec, Class<Address> javaType) {
        super(innerCodec.getCqlType(), javaType);
        this.innerCodec = innerCodec;
        this.userType = (UserType)innerCodec.getCqlType();
    }

    @Override
    public ByteBuffer serialize(Address value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return innerCodec.serialize(toUDTValue(value), protocolVersion);
    }

    @Override
    public Address deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return toAddress(innerCodec.deserialize(bytes, protocolVersion));
    }

    @Override
    public Address parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? 
            null : toAddress(innerCodec.parse(value));
    }

    @Override
    public String format(Address value) throws InvalidTypeException {
        return value == null ? "NULL" : innerCodec.format(toUDTValue(value));
    }

    protected Address toAddress(UDTValue value) {
        return value == null ? null : new Address(
            value.getString("street"), 
            value.getInt("zipcode")
        );
    }

    protected UDTValue toUDTValue(Address value) {
        return value == null ? null : userType.newValue()
            .setString("street", value.getStreet())
            .setInt("zipcode", value.getZipcode());
    }
}
```

Now (and only if you intend to use your own `CodecRegistry`), 
create it and register it on your `Cluster` instance:

```java
// only if you do not intend to use CodecRegistry.DEFAULT_INSTANCE
CodecRegistry codecRegistry = new CodecRegistry();
Cluster cluster = Cluster.builder()
    .addContactPoints(...)
    .withCodecRegistry(codecRegistry)
    .build();
```

Now, retrieve the metadata for type `address`:

```java
UserType addressType = cluster.getMetadata().getKeyspace(...).getUserType("address");
TypeCodec<UDTValue> addressTypeCodec = codecRegistry.codecFor(addressType);
```

And finally, register your `AddressCodec` with the `CodecRegistry`. Note that
your codec simply delegates the hard work to an inner codec that knows how to deal with
user type values:

```java
AddressCodec addressCodec = new AddressCodec(addressTypeCodec, Address.class);
codecRegistry.register(addressCodec);
```

And voilà! You can now retrieve `address` values as `Address` instances:

```java
ResultSet rows = session.execute(...);
Row row = rows.one();
// let the driver convert it for you...
Address address = row.get("address", Address.class);
// ...or access the low-level UDTValue, if you need
UDTValue addressUdt = row.get("address", UDTValue.class); // row.getUDTValue("address") would have worked too
```

Note that this solution creates an intermediary `UDTValue` object when
serializing and deserializing. It's possible to bypass this step with a
lower-level implementation that manipulates the binary stream directly.
That's also how the object mapper handles UDTs, and you can rely on the
mapper to generate UDT codecs for you; see
[this page](../object_mapper/custom_codecs/#implicit-udt-codecs) for more
information.

### Support for generic (parameterized) types

Java generic (parameterized) types are fully supported, through Guava's [TypeToken] API. 
Be sure to only use `get()` and `set()` methods that take a `TypeToken` argument:

```java
// this works
Foo<Bar> foo = row.get(0, new TypeToken<Foo<Bar>>(){})
// this does not work due to type erasure
Foo<Bar> foo = row.get(0, Foo.class);
```

### Support for subtype polymorphism

Suppose the following class hierarchy:

```java
class Animal {}
class Cat extends Animal {}
```

By default, a codec will accept to serialize any object that extends or
implements its declared Java type: a codec such as
`AnimalCodec extends TypeCodec<Animal>` will accept `Cat` instances as well.

This allows a codec to handle interfaces and superclasses
in a generic way, regardless of the actual implementation being
used by client code; for example, the driver has a built-in codec
that handles `List` instances, and this codec is capable of
serializing any concrete `List` implementation.

But this has one caveat: when setting or retrieving values
with `get()` and `set()`, *it is vital to pass the exact 
Java type the codec handles*:

```java
codecRegistry.register(new AnimalCodec());
BoundStatement bs = ...
bs.set(0, new Cat(), Animal.class); // works
bs.set(0, new Cat(),    Cat.class); // throws CodecNotFoundException
```

The same is valid when retrieving values:

```java
codecRegistry.register(new AnimalCodec());
Row row = ...
Animal animal = row.get(0, Animal.class); // works
Cat    cat    = row.get(0,    Cat.class); // throws CodecNotFoundException
```

### Performance considerations

A codec lookup operation may be costly; to mitigate this, the `CodecRegistry` 
caches lookup results whenever possible.

When the cache can be used and the registry looks up a codec, the following rules apply:

1. if a result was previously cached for that mapping, it is returned;
2. otherwise, the registry checks the list of "basic" codecs: the default ones, 
and the ones that were explicitly registered (in the order that they were registered). 
It calls each codec's [accepts] methods to determine if it can handle the mapping, 
and if so, adds it to the cache and returns it;
3. otherwise, the registry tries to generate a codec on the fly, according to the rules outlined above;
4. if the creation succeeds, the registry adds the created codec to the cache and returns it;
5. otherwise, the registry throws [CodecNotFoundException].

The cache can be used as long as *at least the CQL type is known*. The following situations 
are exceptions where it is *not* possible to cache lookup results:

* With [SimpleStatement] instances;
* With [BuiltStatement] instances (created via the Query Builder).
 
In these places, the driver has no way to determine the right CQL type to use, so it performs 
a best-effort heuristic to guess which codec to use. The result of this heuristic is never cached.

Beware that in these cases, the lookup performs in average 10x worse. If performance is a key factor for your application,
consider using prepared statements all the time.

[JAVA-721]: https://datastax-oss.atlassian.net/browse/JAVA-721
[TypeCodec]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/TypeCodec.html
[LocalDate]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/LocalDate.html
[ByteBuffer]: http://docs.oracle.com/javase/8/docs/api/java/nio/ByteBuffer.html
[serialize]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/TypeCodec.html#serialize-T-com.datastax.driver.core.ProtocolVersion-
[deserialize]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/TypeCodec.html#deserialize-java.nio.ByteBuffer-com.datastax.driver.core.ProtocolVersion-
[TypeCodec.format]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/TypeCodec.html#format-T-
[TypeCodec.parse]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/TypeCodec.html#parse-java.lang.String-
[accepts]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/TypeCodec.html#accepts-com.datastax.driver.core.DataType-
[CodecRegistry]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/CodecRegistry.html
[CodecNotFoundException]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/exceptions/CodecNotFoundException.html
[Jackson]: https://github.com/FasterXML/jackson
[AbstractType]: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/AbstractType.java
[UserType]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/UserType.html
[UDTValue]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/UDTValue.html
[TupleType]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/TupleType.html
[TupleValue]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/TupleValue.html
[CustomType]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/DataType.CustomType.html
[TypeToken]: https://google.github.io/guava/releases/19.0/api/docs/com/google/common/reflect/TypeToken.html
[SimpleStatement]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/SimpleStatement.html
[BuiltStatement]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/querybuilder/BuiltStatement.html
[setList]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/SettableByIndexData.html#setList-int-java.util.List-
[setSet]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/SettableByIndexData.html#setSet-int-java.util.Set-
[setMap]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/SettableByIndexData.html#setMap-int-java.util.Map-
