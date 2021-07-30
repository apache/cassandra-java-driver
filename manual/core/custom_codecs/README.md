## Custom codecs

### Quick overview

Define custom Java to CQL mappings.

* implement the [TypeCodec] interface, or use one of the alternative codecs in `ExtraTypeCodecs`.
* registering a codec:
  * at init time: [CqlSession.builder().addTypeCodecs()][SessionBuilder.addTypeCodecs]
  * at runtime:
  
    ```java
    MutableCodecRegistry registry =
        (MutableCodecRegistry) session.getContext().getCodecRegistry();    
    registry.register(myCodec);
    ```
* using a codec:
  * if already registered: `row.get("columnName", MyCustomType.class)`
  * otherwise: `row.get("columnName", myCodec)`

-----

Out of the box, the driver comes with [default CQL to Java mappings](../#cql-to-java-type-mapping).
For example, if you read a CQL `text` column, it is mapped to its natural counterpart
`java.lang.String`:

```java
// cqlsh:ks> desc table test;
// CREATE TABLE ks.test (k int PRIMARY KEY, v text)...
ResultSet rs = session.execute("SELECT * FROM ks.test WHERE k = 1");
String v = rs.one().getString("v");
```

Sometimes you might want to use different mappings, for example:

* read a text column as a Java enum;
* map an `address` UDT to a custom `Address` class in your application;
* manipulate CQL collections as arrays in performance-intensive applications.

Custom codecs allow you to define those dedicated mappings, and plug them into your session.

### Using alternative codecs provided by the driver

The first thing you can do is use one of the many alternative codecs shipped with the driver. They
are exposed on the [ExtraTypeCodecs] class. In this section we are going to introduce these codecs,
then you will see how to register and use them in the next sections.

#### Mapping CQL blobs to Java arrays

The driver default is [TypeCodecs.BLOB], which maps CQL `blob` to Java's [java.nio.ByteBuffer].
Check out our [CQL blob example] to understand how to manipulate the `ByteBuffer` API correctly.

If the `ByteBuffer` API is too cumbersome for you, an alternative is to use
[ExtraTypeCodecs.BLOB_TO_ARRAY] which maps CQL blobs to Java's `byte[]`.

#### Mapping CQL lists to Java arrays

By default, the driver maps CQL `list` to Java's [java.util.List]. If you prefer to deal with 
arrays, the driver offers the following codecs:

1. For primitive types:

    | Codec | CQL type | Java type |
    |---|---|---|
    | [ExtraTypeCodecs.BOOLEAN_LIST_TO_ARRAY] | `list<boolean>` | `boolean[]` |
    | [ExtraTypeCodecs.BYTE_LIST_TO_ARRAY] | `list<tinyint>` | `byte[]` |
    | [ExtraTypeCodecs.SHORT_LIST_TO_ARRAY] | `list<smallint>` | `short[]` |
    | [ExtraTypeCodecs.INT_LIST_TO_ARRAY] | `list<int>` | `int[]` |
    | [ExtraTypeCodecs.LONG_LIST_TO_ARRAY] | `list<bigint>` | `long[]` |
    | [ExtraTypeCodecs.FLOAT_LIST_TO_ARRAY] | `list<float>` | `float[]` |
    | [ExtraTypeCodecs.DOUBLE_LIST_TO_ARRAY] | `list<double>` | `double[]` |
    
2. For other types, you should use [ExtraTypeCodecs.listToArrayOf(TypeCodec)]; for example, to map
   CQL `list<text>` to `String[]`:

    ```java
    TypeCodec<String[]> stringArrayCodec = ExtraTypeCodecs.listToArrayOf(TypeCodecs.TEXT);
    ```

#### Mapping CQL timestamps to Java "instant" types

By default, the driver maps CQL `timestamp` to Java's [java.time.Instant] \(using
[TypeCodecs.TIMESTAMP]). This is the most natural mapping, since neither type contains any time zone
information: they just represent absolute points in time.

The driver also provides codecs to map to a Java `long` representing the number of milliseconds
since the epoch (this is the raw form return by `Instant.toEpochMilli`, and also how Cassandra
stores the value internally).

In either case, you can pick the time zone that the codec will use for its [format()] and [parse()]
methods. Note that this is only relevant for these two methods (follow the links for more
explanations on how the driver uses them); for regular encoding and decoding, like setting a value
on a bound statement or reading a column from a row, the time zone does not matter.

| Codec | CQL type | Java type | Time zone used by `format()` and `parse()` |
|---|---|---|---|
| [TypeCodecs.TIMESTAMP] | `timestamp` | `Instant` | System default |
| [ExtraTypeCodecs.TIMESTAMP_UTC] | `timestamp` | `Instant` | UTC |
| [ExtraTypeCodecs.timestampAt(ZoneId)] | `timestamp` | `Instant` | User-provided |
| [ExtraTypeCodecs.TIMESTAMP_MILLIS_SYSTEM] | `timestamp` | `long` | System default |
| [ExtraTypeCodecs.TIMESTAMP_MILLIS_UTC] | `timestamp` | `long` | UTC |
| [ExtraTypeCodecs.timestampMillisAt(ZoneId)] | `timestamp` | `long` | User-provided |

For example, given the schema:

```
CREATE TABLE example (k int PRIMARY KEY, ts timestamp);
INSERT INTO example(k, ts) VALUES (1, 0);
```

When reading column `ts`, all `Instant` codecs return `Instant.ofEpochMilli(0)`. But if asked to
format it, they behave differently:

* `ExtraTypeCodecs.TIMESTAMP_UTC` returns `'1970-01-01T00:00:00.000Z'`
* `ExtraTypeCodecs.timestampAt(ZoneId.of("Europe/Paris")` returns `'1970-01-01T01:00:00.000+01:00'`

#### Mapping CQL timestamps to `ZonedDateTime`

If your application works with one single, pre-determined time zone, then you probably would like
the driver to map `timestamp` to [java.time.ZonedDateTime] with a fixed zone. Use one of the
following codecs:

| Codec | CQL type | Java type | Time zone used by all codec operations |
|---|---|---|---|
| [ExtraTypeCodecs.ZONED_TIMESTAMP_SYSTEM] | `timestamp` | `ZonedDateTime` | System default |
| [ExtraTypeCodecs.ZONED_TIMESTAMP_UTC] | `timestamp` | `ZonedDateTime` | UTC |
| [ExtraTypeCodecs.zonedTimestampAt(ZoneId)] | `timestamp` | `ZonedDateTime` | User-provided |

This time, the zone matters for all codec operations, including encoding and decoding. For example,
given the schema:
                                                                                       
```
CREATE TABLE example (k int PRIMARY KEY, ts timestamp);
INSERT INTO example(k, ts) VALUES (1, 0);
```

When reading column `ts`:

* `ExtraTypeCodecs.ZONED_TIMESTAMP_UTC` returns the same value as
  `ZonedDateTime.parse("1970-01-01T00:00Z")`
* `ExtraTypeCodecs.zonedTimestampAt(ZoneId.of("Europe/Paris"))` returns the same value as
  `ZonedDateTime.parse("1970-01-01T01:00+01:00[Europe/Paris]")`

These are two distinct `ZonedDateTime` instances: although they represent the same absolute point in
time, they do not compare as equal.

#### Mapping CQL timestamps to `LocalDateTime` 
 
If your application works with one single, pre-determined time zone, but only exposes local
date-times, then you probably would like the driver to map timestamps to [java.time.LocalDateTime]
obtained from a fixed zone. Use one of the following codecs:

| Codec | CQL type | Java type | Time zone used by all codec operations |
|---|---|---|---|
| [ExtraTypeCodecs.LOCAL_TIMESTAMP_SYSTEM] | `timestamp` | `LocalDateTime` | System default |
| [ExtraTypeCodecs.LOCAL_TIMESTAMP_UTC] | `timestamp` | `LocalDateTime` | UTC |
| [ExtraTypeCodecs.localTimestampAt(ZoneId)] | `timestamp` | `LocalDateTime` | User-provided |


Again, the zone matters for all codec operations, including encoding and decoding. For example,
given the schema:
                                                                                       
```
CREATE TABLE example (k int PRIMARY KEY, ts timestamp);
INSERT INTO example(k, ts) VALUES (1, 0);
```

When reading column `ts`:

* `ExtraTypeCodecs.LOCAL_TIMESTAMP_UTC` returns `LocalDateTime.of(1970, 1, 1, 0, 0)`
* `ExtraTypeCodecs.localTimestampAt(ZoneId.of("Europe/Paris"))` returns `LocalDateTime.of(1970, 1,
  1, 1, 0)`

#### Storing the time zone in Cassandra

If your application needs to remember the time zone that each date was entered with, you need to
store it in the database. We suggest using a `tuple<timestamp, text>`, where the second component
holds the [zone id][java.time.ZoneId].

If you follow this guideline, then you can use [ExtraTypeCodecs.ZONED_TIMESTAMP_PERSISTED] to map
the CQL tuple to [java.time.ZonedDateTime].

For example, given the schema:

```
CREATE TABLE example(k int PRIMARY KEY, zts tuple<timestamp, text>);
INSERT INTO example (k, zts) VALUES (1, (0, 'Z'));
INSERT INTO example (k, zts) VALUES (2, (-3600000, 'Europe/Paris'));
```

When reading column `zts`, `ExtraTypeCodecs.ZONED_TIMESTAMP_PERSISTED` returns:

* `ZonedDateTime.parse("1970-01-01T00:00Z")` for the first row
* `ZonedDateTime.parse("1970-01-01T00:00+01:00[Europe/Paris]")` for the second row

Each value is read back in the time zone that it was written with. But note that you can still
compare rows on a absolute timeline with the `timestamp` component of the tuple.

#### Mapping to `Optional` instead of `null` 

If you prefer to deal with [java.util.Optional] in your application instead of nulls, then you can 
use [ExtraTypeCodecs.optionalOf(TypeCodec)]:

```java
TypeCodec<Optional<UUID>> optionalUuidCodec = ExtraTypeCodecs.optionalOf(TypeCodecs.UUID);
```

Note that because the CQL native protocol does not distinguish empty collections from null 
collection references, this codec will also map empty collections to [Optional.empty()].

#### Mapping Java Enums

Java [Enums] can be mapped to CQL in two ways:

1. By name: [ExtraTypeCodecs.enumNamesOf(Class)] will create a codec for a given `Enum` class that
maps its constants to their [programmatic names][Enum.name()]. The corresponding CQL column must be
of type `text`. Note that this codec relies on the enum constant names; it is therefore vital that
enum names never change.
1. By ordinal: [ExtraTypeCodecs.enumOrdinalsOf(Class)] will create a codec for a given `Enum` class
that maps its constants to their [ordinal value][Enum.ordinal()]. The corresponding CQL column must
be of type `int`.

    **We strongly recommend against this approach.** It is provided for compatibility with driver 3,
    but relying on ordinals is a bad practice: any reordering of the enum constants, or insertion
    of a new constant before the end, will change the ordinals. The codec won't fail, but it will
    insert different codes and corrupt your data.

    If you really want to use integer codes for storage efficiency, implement an explicit mapping
    (for example with a `toCode()` method on your enum type). It is then fairly straightforward to
    implement a codec with [MappingCodec](#creating-custom-java-to-cql-mappings-with-mapping-codec),
    using `TypeCodecs#INT` as the "inner" codec.

For example, assuming the following enum:

```java
public enum WeekDay {
  MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY 
}
```

You can define codecs for it the following ways:

```java
// MONDAY will be persisted as "MONDAY", TUESDAY as "TUESDAY", etc.
TypeCodec<String> weekDaysByNameCodec = ExtraTypeCodecs.enumNamesOf(WeekDay.class);

// MONDAY will be persisted as 0, TUESDAY as 1, etc.
TypeCodec<Integer> weekDaysByNameCodec = ExtraTypeCodecs.enumOrdinalsOf(WeekDay.class);
```

#### Mapping Json

The driver provides out-of-the-box support for mapping Java objects to CQL `text` using the popular
Jackson library. The method [ExtraTypeCodecs.json(Class)] will create a codec for a given Java class
that maps instances of that class to Json strings, using a newly-allocated, default [ObjectMapper].
It is also possible to pass a custom `ObjectMapper` instance using [ExtraTypeCodecs.json(Class,
ObjectMapper)] instead.

### Writing codecs

If none of the driver built-in codecs above suits you, it is also possible to roll your own.

To write a custom codec, implement the [TypeCodec] interface. Here is an example that maps a CQL
`int` to a Java string containing its textual representation:

```java
public class CqlIntToStringCodec implements TypeCodec<String> {

  @Override
  public GenericType<String> getJavaType() {
    return GenericType.STRING;
  }

  @Override
  public DataType getCqlType() {
    return DataTypes.INT;
  }

  @Override
  public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    } else {
      int intValue = Integer.parseInt(value);
      return TypeCodecs.INT.encode(intValue, protocolVersion);
    }
  }

  @Override
  public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    Integer intValue = TypeCodecs.INT.decode(bytes, protocolVersion);
    return intValue.toString();
  }

  @Override
  public String format(String value) {
    int intValue = Integer.parseInt(value);
    return TypeCodecs.INT.format(intValue);
  }

  @Override
  public String parse(String value) {
    Integer intValue = TypeCodecs.INT.parse(value);
    return intValue == null ? null : intValue.toString();
  }
}
```

Admittedly, this is a trivial -- and maybe not very realistic -- example, but it illustrates a few
important points:
 
* which methods to override. Refer to the [TypeCodec] javadocs for additional information about each
  of them; 
* how to piggyback on a built-in codec, in this case `TypeCodecs.INT`. Very often, this is the best
  approach to keep the code simple. If you want to handle the binary encoding yourself (maybe to
  squeeze the last bit of performance), study the driver's
  [built-in codec implementations](https://github.com/datastax/java-driver/tree/4.x/core/src/main/java/com/datastax/oss/driver/internal/core/type/codec). 

### Using codecs

Once you have your codec, register it when building your session. The following example registers
`CqlIntToStringCodec` along with a few driver-supplied alternative codecs:

```java
enum WeekDay { MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY };
class Price {
  ... // a custom POJO that will be serialized as JSON
}

CqlSession session =
  CqlSession.builder()
    .addTypeCodecs(
      new CqlIntToStringCodec(),                  // user-created codec
      ExtraTypeCodecs.ZONED_TIMESTAMP_PERSISTED,  // tuple<timestamp,text> <-> ZonedDateTime
      ExtraTypeCodecs.BLOB_TO_ARRAY,              // blob <-> byte[]
      ExtraTypeCodecs.arrayOf(TypeCodecs.TEXT),   // list<text> <-> String[]
      ExtraTypeCodecs.enumNamesOf(WeekDay.class), // text <-> WeekDay
      ExtraTypeCodecs.json(Price.class),          // text <-> MyJsonPojo
      ExtraTypeCodecs.optionalOf(TypeCodecs.UUID) // uuid <-> Optional<UUID>
    )
    .build();
```

You may also add codecs to an existing session at runtime:

```java
// The cast is required for backward compatibility reasons (registry mutability was introduced in
// 4.3.0). It is safe as long as you didn't write a custom registry implementation.
MutableCodecRegistry registry =
    (MutableCodecRegistry) session.getContext().getCodecRegistry();

registry.register(new CqlIntToStringCodec());
```

You can now use the new mappings in your code:

```java
// cqlsh:ks> desc table test2;
// CREATE TABLE ks.test2 (k int PRIMARY KEY, v int)...
ResultSet rs = session.execute("SELECT * FROM ks.test2 WHERE k = 1");
String v = rs.one().getString("v"); // read a CQL int as a java.lang.String

PreparedStatement ps = session.prepare("INSERT INTO ks.test2 (k, v) VALUES (?, ?)");
session.execute(
    ps.boundStatementBuilder()
        .setInt("k", 2)
        .setString("v", "12") // write a java.lang.String as a CQL int
        .build());
```

In the above example, the driver will look up in the codec registry a codec for CQL `int` and Java
String, and will transparently pick `CqlIntToStringCodec` for that.

So far our examples have used a Java type with dedicated accessors in the driver: `getString` and
`setString`. But sometimes you won't find suitable accessor methods; for example, there is no 
accessor for `ZonedDateTime` or for `Optional<UUID>`, and yet we registered codecs for these types. 

When you want to retrieve such objects, you need a way to tell the driver which Java type you want.
You do so by using one of the generic `get` and `set` methods:

```java
// Assuming that ExtraTypeCodecs.ZONED_TIMESTAMP_PERSISTED was registered
// Assuming that ExtraTypeCodecs.BLOB_TO_ARRAY was registered
// Assuming that ExtraTypeCodecs.arrayOf(TypeCodecs.TEXT) was registered

// Reading
ZonedDateTime v1 = row.get("v1", ZonedDateTime.class); // assuming column is of type timestamp
byte[] v2        = row.get("v2", byte[].class);        // assuming column is of type blob
String[] v3      = row.get("v3", String[].class);      // assuming column is of type list<text>


// Writing
boundStatement.set("v1", v1, ZonedDateTime.class);
boundStatement.set("v2", v2, byte[].class);
boundStatement.set("v3", v3, String[].class);
```

This is also valid for arbitrary Java types. This is particularly useful when dealing with Enums and 
JSON mappings, for example our `WeekDay` and `Price` types:

```java
// Assuming that TypeCodecs.enumNamesOf(WeekDay.class) was registered
// Assuming that TypeCodecs.json(Price.class) was registered

// Reading
WeekDay v1 = row.get("v1", WeekDay.class); // assuming column is of type text
Price v2   = row.get("v2", Price.class);   // assuming column is of type text

// Writing
boundStatement.set("v1", v1, WeekDay.class);
boundStatement.set("v2", v2, Price.class);
```

Note that, because the underlying CQL type is `text` you can still retrieve the column's contents
as a plain string:

```java
// Reading
String enumName = row.getString("v1");
String priceJson = row.getString("v2");

// Writing
boundStatement.setString("v1", enumName);
boundStatement.setString("v2", priceJson);
```

And finally, for `Optional<UUID>`, you will need the `get` and `set` methods with an extra *type 
token* argument, because `Optional<UUID>` is a parameterized type:

```java
// Assuming that TypeCodecs.optionalOf(TypeCodecs.UUID) was registered

// Reading
Optional<UUID> opt = row.get("v", GenericType.optionalOf(UUID.class));

// Writing
boundStatement.set("v", opt, GenericType.optionalOf(UUID.class));
```

Type tokens are instances of [GenericType]. They are immutable and thread-safe, you should store
them as reusable constants. The `GenericType` class itself has constants and factory methods to help
creating `GenericType` objects for common types. If you don't see the type you are looking for, a
type token for any Java type can be created using the following pattern:

```java
// Notice the '{}': this is an anonymous inner class
GenericType<Foo<Bar>> fooBarType = new GenericType<Foo<Bar>>(){};

Foo<Bar> v = row.get("v", fooBarType);
```

Custom codecs are used not only for their base type, but also recursively in collections, tuples and
UDTs. For example, once your Json codec for the `Price` class is registered, you can also read a CQL
`list<text>` as a Java `List<Price>`:

```java
// Assuming that TypeCodecs.json(Price.class) was registered
// Assuming that each element of the list<text> column is a valid Json string

// Reading
List<Price> prices1 = row.getList("v", Price.class);
// alternative method using the generic get method with type token argument:
List<Price> prices2 = row.get("v", GenericType.listOf(Price.class));

// Writing
boundStatement.setList("v", prices1, Price.class);
// alternative method using the generic set method with type token argument:
boundStatement.set("v", prices2, GenericType.listOf(Price.class));
``` 

Whenever you read or write a value, the driver tries all the built-in mappings first, followed by
custom codecs. If two codecs can process the same mapping, the one that was registered first is
used. Note that this means that built-in mappings can't be overridden.

In rare cases, you might have a codec registered in your application, but have a legitimate reason
to use a different mapping in one particular place. In that case, you can pass a codec instance 
to `get` / `set` instead of a type token:

```java
TypeCodec<String> defaultCodec = new CqlIntToStringCodec();
TypeCodec<String> specialCodec = ...; // a different implementation

CqlSession session =
    CqlSession.builder().addTypeCodecs(defaultCodec).build();

String s1 = row.getString("anIntColumn");         // int -> String, will decode with defaultCodec
String s2 = row.get("anIntColumn", specialCodec); // int -> String, will decode with specialCodec
``` 

By doing so, you bypass the codec registry completely and instruct the driver to use the given 
codec. Note that it is your responsibility to ensure that the codec can handle the underlying CQL
type (this cannot be enforced at compile-time).

### Creating custom Java-to-CQL mappings with `MappingCodec`

The above example, `CqlIntToStringCodec`, could be rewritten to leverage [MappingCodec], an abstract 
class that ships with the driver. This class has been designed for situations where we want to 
represent a CQL type with a different Java type than the Java type natively supported by the driver,
and the conversion between the former and the latter is straightforward. 

All you have to do is extend `MappingCodec` and implement two methods that perform the conversion 
between the supported Java type -- or "inner" type -- and the target Java type -- or "outer" type:

```java
public class CqlIntToStringCodec extends MappingCodec<Integer, String> {

  public CqlIntToStringCodec() {
    super(TypeCodecs.INT, GenericType.STRING);
  }

  @Nullable
  @Override
  protected String innerToOuter(@Nullable Integer value) {
    return value == null ? null : value.toString();
  }

  @Nullable
  @Override
  protected Integer outerToInner(@Nullable String value) {
    return value == null ? null : Integer.parseInt(value);
  }
}
```

This technique is especially useful when mapping user-defined types to Java objects. For example, 
let's assume the following user-defined type:

```
CREATE TYPE coordinates (x int, y int);
 ```
 
And let's suppose that we want to map it to the following Java class:
 
```java
public class Coordinates {
  public final int x;
  public final int y;
  public Coordinates(int x, int y) { this.x = x; this.y = y; }
}
```

All  you have to do is create a `MappingCodec` subclass that piggybacks on an existing 
`TypeCodec<UdtValue>` for the above user-defined type:

```java
public class CoordinatesCodec extends MappingCodec<UdtValue, Coordinates> {

  public CoordinatesCodec(@NonNull TypeCodec<UdtValue> innerCodec) {
    super(innerCodec, GenericType.of(Coordinates.class));
  }

  @NonNull @Override public UserDefinedType getCqlType() {
    return (UserDefinedType) super.getCqlType();
  }

  @Nullable @Override protected Coordinates innerToOuter(@Nullable UdtValue value) {
    return value == null ? null : new Coordinates(value.getInt("x"), value.getInt("y"));
  }

  @Nullable @Override protected UdtValue outerToInner(@Nullable Coordinates value) {
    return value == null ? null : getCqlType().newValue().setInt("x", value.x).setInt("y", value.y);
  }
}
```

Then the new mapping codec could be registered as follows:

```java
CqlSession session = ...
CodecRegistry codecRegistry = session.getContext().getCodecRegistry();
// The target user-defined type
UserDefinedType coordinatesUdt =
    session
        .getMetadata()
        .getKeyspace("...")
        .flatMap(ks -> ks.getUserDefinedType("coordinates"))
        .orElseThrow(IllegalStateException::new);
// The "inner" codec that handles the conversions from CQL from/to UdtValue
TypeCodec<UdtValue> innerCodec = codecRegistry.codecFor(coordinatesUdt);
// The mapping codec that will handle the conversions from/to UdtValue and Coordinates
CoordinatesCodec coordinatesCodec = new CoordinatesCodec(innerCodec);
// Register the new codec
((MutableCodecRegistry) codecRegistry).register(coordinatesCodec);
```

...and used just like explained above:

```java
BoundStatement stmt = ...;
stmt.set("coordinates", new Coordinates(10,20), Coordinates.class);

Row row = ...;
Coordinates coordinates = row.get("coordinates", Coordinates.class);
``` 

Note: if you need even more advanced mapping capabilities, consider adopting
the driver's [object mapping framework](../../mapper/).

### Subtype polymorphism

Suppose the following class hierarchy:

```java
class Animal {}
class Cat extends Animal {}
```

By default, a codec will accept to serialize any object that extends or implements its declared Java
type: a codec such as `AnimalCodec extends TypeCodec<Animal>` will accept `Cat` instances as well.

This allows a codec to handle interfaces and superclasses in a generic way, regardless of the actual
implementation being used by client code; for example, the driver has a built-in codec that handles
`List` instances, and this codec is capable of serializing any concrete `List` implementation.

But this has one caveat: when setting or retrieving values with `get()` and `set()`, *you must pass
the exact Java type the codec handles*:

```java
BoundStatement bs = ...
bs.set(0, new Cat(), Animal.class); // works
bs.set(0, new Cat(),    Cat.class); // throws CodecNotFoundException

Row row = ...
Animal animal = row.get(0, Animal.class); // works
Cat    cat    = row.get(0,    Cat.class); // throws CodecNotFoundException
```

### The codec registry

The driver stores all codecs (built-in and custom) in an internal [CodecRegistry]:

```java
CodecRegistry getCodecRegistry = session.getContext().getCodecRegistry();

// Get the custom codec we registered earlier:
TypeCodec<String> cqlIntToString = codecRegistry.codecFor(DataTypes.INT, GenericType.STRING);
```

If all you're doing is executing requests and reading responses, you probably won't ever need to
access the registry directly. But it's useful if you do some kind of generic processing, for
example printing out an arbitrary row when the schema is not known at compile time:

```java
private static String formatRow(Row row) {
  StringBuilder result = new StringBuilder();
  for (int i = 0; i < row.size(); i++) {
    String name = row.getColumnDefinitions().get(i).getName().asCql(true);
    Object value = row.getObject(i);
    DataType cqlType = row.getType(i);
    
    // Find the best codec to format this CQL type: 
    TypeCodec<Object> codec = row.codecRegistry().codecFor(cqlType);

    if (i != 0) {
      result.append(", ");
    }
    result.append(name).append(" = ").append(codec.format(value));
  }
  return result.toString();
}
```

[CodecRegistry]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/registry/CodecRegistry.html
[GenericType]:   https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/reflect/GenericType.html
[TypeCodec]:     https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/TypeCodec.html
[format()]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/TypeCodec.html#format-JavaTypeT-
[parse()]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/TypeCodec.html#parse-java.lang.String-
[MappingCodec]:     https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/MappingCodec.html
[SessionBuilder.addTypeCodecs]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/SessionBuilder.html#addTypeCodecs-com.datastax.oss.driver.api.core.type.codec.TypeCodec...-

[Enums]: https://docs.oracle.com/javase/8/docs/api/java/lang/Enum.html
[Enum.name()]: https://docs.oracle.com/javase/8/docs/api/java/lang/Enum.html#name--
[Enum.ordinal()]: https://docs.oracle.com/javase/8/docs/api/java/lang/Enum.html#ordinal--
[java.nio.ByteBuffer]: https://docs.oracle.com/javase/8/docs/api/java/nio/ByteBuffer.html
[java.util.List]: https://docs.oracle.com/javase/8/docs/api/java/util/List.html
[java.util.Optional]: https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html
[Optional.empty()]: https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html#empty--
[java.time.Instant]: https://docs.oracle.com/javase/8/docs/api/java/time/Instant.html
[java.time.ZonedDateTime]: https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html
[java.time.LocalDateTime]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html
[java.time.ZoneId]: https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html

[ExtraTypeCodecs]:                           https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html
[ExtraTypeCodecs.BLOB_TO_ARRAY]:             https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#BLOB_TO_ARRAY
[ExtraTypeCodecs.BOOLEAN_LIST_TO_ARRAY]:     https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#BOOLEAN_LIST_TO_ARRAY
[ExtraTypeCodecs.BYTE_LIST_TO_ARRAY]:        https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#BYTE_LIST_TO_ARRAY
[ExtraTypeCodecs.SHORT_LIST_TO_ARRAY]:       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#SHORT_LIST_TO_ARRAY
[ExtraTypeCodecs.INT_LIST_TO_ARRAY]:         https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#INT_LIST_TO_ARRAY
[ExtraTypeCodecs.LONG_LIST_TO_ARRAY]:        https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#LONG_LIST_TO_ARRAY
[ExtraTypeCodecs.FLOAT_LIST_TO_ARRAY]:       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#FLOAT_LIST_TO_ARRAY
[ExtraTypeCodecs.DOUBLE_LIST_TO_ARRAY]:      https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#DOUBLE_LIST_TO_ARRAY
[ExtraTypeCodecs.listToArrayOf(TypeCodec)]:  https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#listToArrayOf-com.datastax.oss.driver.api.core.type.codec.TypeCodec-
[ExtraTypeCodecs.TIMESTAMP_UTC]:             https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#TIMESTAMP_UTC
[ExtraTypeCodecs.timestampAt(ZoneId)]:       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#timestampAt-java.time.ZoneId-
[ExtraTypeCodecs.TIMESTAMP_MILLIS_SYSTEM]:   https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#TIMESTAMP_MILLIS_SYSTEM
[ExtraTypeCodecs.TIMESTAMP_MILLIS_UTC]:      https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#TIMESTAMP_MILLIS_UTC
[ExtraTypeCodecs.timestampMillisAt(ZoneId)]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#timestampMillisAt-java.time.ZoneId-
[ExtraTypeCodecs.ZONED_TIMESTAMP_SYSTEM]:    https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#ZONED_TIMESTAMP_SYSTEM
[ExtraTypeCodecs.ZONED_TIMESTAMP_UTC]:       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#ZONED_TIMESTAMP_UTC
[ExtraTypeCodecs.zonedTimestampAt(ZoneId)]:  https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#zonedTimestampAt-java.time.ZoneId-
[ExtraTypeCodecs.LOCAL_TIMESTAMP_SYSTEM]:    https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#LOCAL_TIMESTAMP_SYSTEM
[ExtraTypeCodecs.LOCAL_TIMESTAMP_UTC]:       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#LOCAL_TIMESTAMP_UTC
[ExtraTypeCodecs.localTimestampAt(ZoneId)]:  https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#localTimestampAt-java.time.ZoneId-
[ExtraTypeCodecs.ZONED_TIMESTAMP_PERSISTED]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#ZONED_TIMESTAMP_PERSISTED
[ExtraTypeCodecs.optionalOf(TypeCodec)]:     https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#optionalOf-com.datastax.oss.driver.api.core.type.codec.TypeCodec-
[ExtraTypeCodecs.enumNamesOf(Class)]:        https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#enumNamesOf-java.lang.Class-
[ExtraTypeCodecs.enumOrdinalsOf(Class)]:     https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#enumOrdinalsOf-java.lang.Class-
[ExtraTypeCodecs.json(Class)]:               https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#json-java.lang.Class-
[ExtraTypeCodecs.json(Class, ObjectMapper)]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/ExtraTypeCodecs.html#json-java.lang.Class-com.fasterxml.jackson.databind.ObjectMapper-

[TypeCodecs.BLOB]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#BLOB
[TypeCodecs.TIMESTAMP]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#TIMESTAMP


[ObjectMapper]: http://fasterxml.github.io/jackson-databind/javadoc/2.10/com/fasterxml/jackson/databind/ObjectMapper.html

[CQL blob example]: https://github.com/datastax/java-driver/blob/4.x/examples/src/main/java/com/datastax/oss/driver/examples/datatypes/Blobs.java