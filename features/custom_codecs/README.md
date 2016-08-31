## Custom Codecs

Custom codecs support transparent, user-configurable mapping of CQL types to arbitrary Java objects.

Practical use cases that justify such a feature are numerous:

* Ability to map CQL timestamp, date and time columns to Java 8 or Joda Time classes (rather than the built-in mappings for Date, [LocalDate] and Long);
* Ability to map CQL varchar columns directly to JSON or XML mapped objects (i.e. using frameworks such as Jackson);
* Ability to map CQL user-defined types to Java objects;
* Ability to map CQL lists directly to Java arrays;
* Ability to map CQL collections directly to Scala collections;
* etc.

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

Refer to the [javadocs][CodecRegistry] of User `CodecRegistry` for more information.

### Implementing and using custom codecs

Let's consider the following scenario: a user has JSON documents stored in a varchar column, and wants
the driver to automatically map that column to a Java object using the [Jackson] library,
instead of returning the raw JSON string.

The (admittedly simplistic) table structure is as follows:

```sql
CREATE TABLE t (id int PRIMARY KEY, json VARCHAR);
```

The first step is to implement a suitable codec. Using Jackson, here is what a Json codec could like like:

```java
/**
 * A simple Json codec.
 */
public class JsonCodec<T> extends TypeCodec.StringParsingCodec<T> {

    private final ObjectMapper objectMapper;

    public JsonCodec(Class<T> javaType, ObjectMapper objectMapper) {
        super(javaType);
        this.objectMapper = objectMapper;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T fromString(String value) {
        if (value == null)
            return null;
        try {
            return (T) objectMapper.readValue(value, getJavaType().getRawType());
        } catch (IOException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
    }

    @Override
    public String toString(T value) {
        if (value == null)
            return null;
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
    }
}
```

As you can see, the driver already provides a convenient base class: [StringParsingCodec][StringParsingCodec].

A few implementation guidelines:

* Your codecs should be thread-safe, or better yet, immutable;
* Your codecs should be fast: do not forget that codecs are executed often and are usually very "hot" pieces of code;
* Your codecs should never block.

Note: the code above is an example and is not particularly efficient;
it suffers from the overhead of deserializing raw bytes into a String,
to only then parse the String into an object.
More advanced solutions that read the underlying byte stream directly are possible.

The second step is to register your codec with a `CodecRegistry` instance:

```java
ObjectMapper objectMapper = ...
JsonCodec<MyPojo> myJsonCodec = new JsonCodec<MyPojo>(MyPojo.class, objectMapper);
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
This ensures that the new registry will not lack of an essential codec.

From now on, your custom codec is fully operational. It will be used every time the driver encounters
a `MyPojo` instance when executing queries, or when you ask it to retrieve a `MyPojo` instance from a `ResultSet`.

For example, here is how to save a `MyPojo` object:

```java
Cluster cluster = ...
Session session = ...
MyPojo myPojo = ...
// Using SimpleStatement
Statement stmt = session.newSimpleStatement("INSERT INTO t (id, json) VALUES (?, ?)", 42, myPojo));
// Using the Query Builder
BuiltStatement insertStmt = new QueryBuilder(cluster).insertInto("t")
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
ResultSet rows = session.execute(...);
Row row = one();
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

### Support for parameterized types

`get()` and `set()` can even be used to retrieve collections or other
user-defined parameterized types; simply use the `get()` and `set()` variants that
take Guava's [TypeToken] parameter:

```java
Row row = ...
List<MyPojo> myPojos = row.get("jsonList", new TypeToken<List<MyPojo>>(){});
// or alternatively, the following also works for collections:
List<MyPojo> myPojos = row.getList("jsonList", MyPojo.class);
```

### Support for collections, user types, tuples and Java Enums

`CodecRegistry` instances not only store default codecs and user-defined codecs;
they can also create new codecs on the fly, based on the set of codecs they currently hold.

* Codecs for [UserType] instances are created on the fly using [UDTCodec];
* Codecs for [TupleType] instances are created on the fly using [TupleCodec];
* Codecs for Java Enums are created on the fly using [EnumStringCodec];
* Codecs for collections are created on the fly using [ListCodec],
  [SetCodec] and [MapCodec], if their element types can be handled by
  existing codecs (or codecs that can themselves be generated).

This way, the user does not have to manually register all derived codecs for a given "base" codec.
However, other combinations of Java and CQL types not listed above cannot have their codecs created on the fly;
such codecs must be manually registered.

The driver also provides another codec for Enums that converts Enum instances to CQL ints
representing their ordinal position: [EnumIntCodec]. If you plan to use this codec, make sure to register it
manually, otherwise Enums would be converted using [EnumStringCodec].
_It is not possible to mix both codecs in the same `CodecRegistry`_.

Note that the default set of codecs has no support for
[Cassandra custom types][AbstractType];
to be able to deserialize values of such types, you need to manually register an appropriate codec.

### Using custom codecs with user types and tuples

By default, the driver maps user-defined type values and tuple values to [UDTValue] and [TupleValue],
using the built-in codecs [UDTCodec] and [TupleCodec] respectively.

If you want to register a custom codec for a specific user type, follow these steps:

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

First of all, create a codec for this class:

```java
public class AddressCodec extends TypeCodec.MappingCodec<Address, UDTValue> {

    private final UserType userType;

    public AddressCodec(TypeCodec<UDTValue> innerCodec, Class<Address> javaType) {
        super(innerCodec, javaType);
        userType = (UserType) innerCodec.getCqlType();
    }

    @Override
    protected Address deserialize(UDTValue value) {
        return value == null ? null : new Address(value.getString("street"), value.getInt("zipcode"));
    }

    @Override
    protected UDTValue serialize(Address address) {
        return address == null ? null : userType.newValue().setString("street", address.getStreet()).setInt("zipcode", address.getZipcode());
    }
}
```

Again, the driver provides a convenient base class: [MappingCodec].

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
```

And finally, register your `AddressCodec` with the `CodecRegistry`. Note that
your codec simply delegates the hard work to an inner codec that knows how to deal with
user type values:

```java
AddressCodec addressCodec = new AddressCodec(new TypeCodec.UDTCodec(addressType), Address.class);
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

### Replacing (overriding) built-in codecs

User-provided codecs should generally *enrich* the driver capabilities, e.g. by adding support
for additional Java types; it is usually not recommended to register a codec that handles
the exact same CQL-to-Java mapping as another already registered codec's. 

Overriding a registered codec is however permitted, even if the driver will log a warning.

The only cases that could justify overriding a codec are:
 
 * The overriding codec returns a different implementation of a common Java interface;
 * The overriding codec claims to perform better than the registered one.

### Limitations

#### Generics

Java generic types are fully supported, through Guava's [TypeToken] API. 
Be sure to only use `get()` and `set()` methods
that take a `TypeToken` argument:

```java
// this works
Foo<Bar> foo = row.get(0, new TypeToken<Foo<Bar>>(){})
// this does not work due to type erasure
Foo<Bar> foo = row.get(0, Foo.class);
```

#### Type inheritance

If a codec accepts a Java type that is assignable to the
desired Java type, that codec may be returned if it is found first
in the registry, *even if another codec is a better match*.

As a consequence, type inheritance and interfaces should be used with care. 
Let's consider an example and assume that class `B` extends class `A`:
 
1) If you register a codec for `A`, you can pass `B` as the target type,
because `B` instances are assignable to `A`, and the driver is capable
of detecting that:

```java
codecRegistry.register(new ACodec());
// all work
A a1 = row.get(0, A.class);
B b  = row.get(0, B.class);
A a2 = row.get(0, B.class);
```

However, because `List<B>` is *not* assignable to `List<A>`:

```java
codecRegistry.register(new ACodec());
// this works
List<A> as1 = row.getList(0, A.class);
// but this throws CodecNotFoundException
List<B> bs = row.getList(0, B.class);
List<? extends A> as2 = row.getList(0, B.class);
```

2) If you register a codec for `B`, you can't pass `A` as the target type, 
because no codec matches `A.class` exactly:

```java
codecRegistry.register(new BCodec());
// this works
A a1 = row.get(0, B.class);
B b  = row.get(0, B.class);
// but this throws CodecNotFoundException
A a2 = row.get(0, A.class);
// and with collections, this works
List<B> bs = row.getList(0, B.class);
// but this throws CodecNotFoundException
List<A> as = row.getList(0, A.class);
```

The same apply for interfaces.

### Performance considerations

A codec lookup operation may be costly; to mitigate this, the `CodecRegistry` caches lookup results whenever possible.

The following situations are exceptions where it is not possible to cache lookup results:

* With [SimpleStatement] instances and [BuiltStatement] instances (created via the Query Builder); in these places, the driver has
  no way to determine the right CQL type to use, so it performs a best-effort heuristic to guess which codec to use.
* When using "ambiguous" methods of `BoundStatement` and `Row`, such as [setList()][setList],
  [setSet()][setSet], [setMap()][setMap], [setObject()][setObject];
  these methods are "ambiguous" because they do not convey enough compile-time information about the object
  to serialize, so the driver performs the same heuristic to guess which codec to use.

Beware that in these cases the lookup performs in average 10x worse. If performance is a key factor for your application,
consider using prepared statements all the time, and avoid calling ambiguous methods (prefer `get()` and `set()`).

[JAVA-721]: https://datastax-oss.atlassian.net/browse/JAVA-721
[TypeCodec]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.html
[LocalDate]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/LocalDate.html
[serialize]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.html#serialize(T,%20com.datastax.driver.core.ProtocolVersion)
[deserialize]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.html#deserialize(java.nio.ByteBuffer,%20com.datastax.driver.core.ProtocolVersion)
[TypeCodec.format]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.html#format(T)
[TypeCodec.parse]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.html#parse(java.lang.String)
[CodecRegistry]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/CodecRegistry.html
[Jackson]: http://wiki.fasterxml.com/JacksonHome
[AbstractType]: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/AbstractType.java
[EnumStringCodec]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.EnumStringCodec.html
[EnumIntCodec]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.EnumIntCodec.html
[UserType]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/UserType.html
[UDTValue]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/UDTValue.html
[UDTCodec]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.UDTCodec.html
[TupleType]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TupleType.html
[TupleValue]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TupleValue.html
[TupleCodec]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.TupleCodec.html
[ListCodec]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.ListCodec.html
[SetCodec]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.SetCodec.html
[MapCodec]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.MapCodec.html
[TypeToken]: http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/reflect/TypeToken.html
[MappingCodec]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.MappingCodec.html
[StringParsingCodec]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/TypeCodec.StringParsingCodec.html
[SimpleStatement]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/SimpleStatement.html
[BuiltStatement]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/BuiltStatement.html
[setList]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/SettableByIndexData.html#setList(int,%20java.util.List)
[setSet]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/SettableByIndexData.html#setSet(int,%20java.util.Set)
[setMap]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/SettableByIndexData.html#setMap(int,%20java.util.Map)
[setObject]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/SettableByIndexData.html#setObject(int,%20V)
