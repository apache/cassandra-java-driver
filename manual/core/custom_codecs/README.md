## Custom codecs

### Quick overview

Define custom Java to CQL mappings.

* implement the [TypeCodec] interface.
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

### Writing codecs

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

Once you have your codec, register it when building your session:

```java
CqlSession session = CqlSession.builder()
    .addTypeCodecs(new CqlIntToStringCodec())
    .build();
```

You may also add codecs to an existing session at runtime:

```java
// The cast is required for backward compatibility reasons (registry mutability was introduced in
// 4.3.0). It is safe as long as you didn't hack the driver internals to plug a custom registry
// implementation.
MutableCodecRegistry registry =
    (MutableCodecRegistry) session.getContext().getCodecRegistry();

registry.register(new CqlIntToStringCodec());
```

You can now use the new mapping in your code:

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

Custom codecs are used not only for their base type, but also recursively in collections, tuples and
UDTs. For example, once your `int <-> String` codec is registered, you can also read a CQL
`list<int>` as a Java `List<String>`:

```java
// cqlsh:ks> desc table test3;
// CREATE TABLE ks.test2 (k int PRIMARY KEY, v list<int>)...
ResultSet rs = session.execute("SELECT * FROM ks.test3 WHERE k = 1");
List<String> v = rs.one().getList("v", String.class);
``` 

So far our examples have used a Java type with dedicated accessors in the driver: `getString` and
`setString`. But you can also map your own Java types. For example, let's assume you have a `Price`
class, and have registered a codec that maps it to a particular CQL type. When reading or writing
values, you need a way to tell the driver which Java type you want; this is done with the generic
`get` and `set` methods with an extra *type token* arguments:

```java
GenericType<Price> priceType = GenericType.of(Price.class);

// Reading
Price price = row.get("v", priceType);

// Writing
boundStatement.set("v", price, priceType);
```

Type tokens are instances of [GenericType]. They are immutable and thread-safe, you should store
them as reusable constants. Generic Java types are fully supported, using the following pattern:

```java
// Notice the '{}': this is an anonymous inner class
GenericType<Foo<Bar>> fooBarType = new GenericType<Foo<Bar>>(){};

Foo<Bar> v = row.get("v", fooBarType);
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

[CodecRegistry]: https://docs.datastax.com/en/drivers/java/4.7/com/datastax/oss/driver/api/core/type/codec/registry/CodecRegistry.html
[GenericType]:   https://docs.datastax.com/en/drivers/java/4.7/com/datastax/oss/driver/api/core/type/reflect/GenericType.html
[TypeCodec]:     https://docs.datastax.com/en/drivers/java/4.7/com/datastax/oss/driver/api/core/type/codec/TypeCodec.html
[MappingCodec]:     https://docs.datastax.com/en/drivers/java/4.7/com/datastax/oss/driver/api/core/type/codec/MappingCodec.html
[SessionBuilder.addTypeCodecs]: https://docs.datastax.com/en/drivers/java/4.7/com/datastax/oss/driver/api/core/session/SessionBuilder.html#addTypeCodecs-com.datastax.oss.driver.api.core.type.codec.TypeCodec...-
