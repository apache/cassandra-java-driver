## Optional codecs

The driver's "extras" module provides additional [codec](../) implementations: these codecs are not required by core
driver features, but will probably prove useful in a lot of client applications. You can also study their source code as
a reference to write your own.

The module is published as a separate Maven artifact:

```xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-extras</artifactId>
  <version>3.1.0</version>
</dependency>
```

It also declares a number of optional dependencies, that you'll need to explicitly re-declare in your application
depending on which codecs you plan to use (this will be covered for each case below).

### Temporal types

JDK 8 and Joda Time support have often been requested; they're not in the core module because we want to preserve
compatibility with older JDKs and avoid extra dependencies.

#### JDK 8

[InstantCodec] maps [Instant] to CQL's `timestamp`:

```java
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import java.time.Instant;

cluster.getConfiguration().getCodecRegistry()
    .register(InstantCodec.instance);

// schema: CREATE TABLE example(id int PRIMARY KEY, t timestamp)
session.execute("INSERT INTO example (id, t) VALUES (1, ?)",
        Instant.parse("2010-06-30T01:20:30Z"));
```

Similarly:

* [LocalDateCodec] maps [LocalDate] to `date`;
* [LocalTimeCodec] maps [LocalTime] to `time`.

One problem with `timestamp` is that it does not store time zones. [ZonedDateTimeCodec] addresses that, by mapping a
[ZonedDateTime] to a `tuple<timestamp,varchar>`:

```java
// Due to internal implementation details, you have to retrieve the tuple type from a live cluster:
TupleType tupleType = cluster.getMetadata()
        .newTupleType(DataType.timestamp(), DataType.varchar());
cluster.getConfiguration().getCodecRegistry()
        .register(new ZonedDateTimeCodec(tupleType));

// schema: CREATE TABLE example(id int PRIMARY KEY, t tuple<timestamp,varchar>)
session.execute("INSERT INTO example (id, t) VALUES (1, ?)",
        ZonedDateTime.parse("2010-06-30T01:20:47.999+01:00"));
```

[InstantCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/jdk8/InstantCodec.html
[LocalDateCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/jdk8/LocalDateCodec.html
[LocalTimeCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/jdk8/LocalTimeCodec.html
[ZonedDateTimeCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/jdk8/ZonedDateTimeCodec.html
[Instant]: https://docs.oracle.com/javase/8/docs/api/java/time/Instant.html
[LocalDate]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDate.html
[LocalTime]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalTime.html
[ZonedDateTime]: https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html


#### Joda time

These codecs require an explicit dependency on Joda Time in your application:

```xml
<dependency>
  <groupId>joda-time</groupId>
  <artifactId>joda-time</artifactId>
  <version>2.9.1</version>
</dependency>
```

Like for JDK8, there is a codec for each time type:

* [InstantCodec][InstantCodec_joda]: maps [Instant][Instant_joda] to `timestamp`;
* [LocalDateCodec][LocalDateCodec_joda]: maps [LocalDate][LocalDate_joda] to `date`;
* [LocalTimeCodec][LocalTimeCodec_joda]: maps [LocalTime][LocalTime_joda] to `time`.

```java
import com.datastax.driver.extras.codecs.joda.InstantCodec;
import org.joda.time.Instant;

cluster.getConfiguration().getCodecRegistry()
        .register(InstantCodec.instance);

// schema: CREATE TABLE example(id int PRIMARY KEY, t timestamp)
session.execute("INSERT INTO example (id, t) VALUES (1, ?)",
        Instant.parse("2010-06-30T01:20:30Z"));
```

And [DateTimeCodec] can save [DateTime] instances with a time zone:

```java
TupleType tupleType = cluster.getMetadata()
        .newTupleType(DataType.timestamp(), DataType.varchar());
cluster.getConfiguration().getCodecRegistry()
        .register(new DateTimeCodec(tupleType));

// schema: CREATE TABLE example(id int PRIMARY KEY, t tuple<timestamp,varchar>)
session.execute("INSERT INTO example (id, t) VALUES (1, ?)",
        DateTime.parse("2010-06-30T01:20:47.999+01:00"));
```

[InstantCodec_joda]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/joda/InstantCodec.html
[LocalDateCodec_joda]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/joda/LocalDateCodec.html
[LocalTimeCodec_joda]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/joda/LocalTimeCodec.html
[DateTimeCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/joda/DateTimeCodec.html
[DateTime]: http://www.joda.org/joda-time/apidocs/org/joda/time/DateTime.html
[Instant_joda]: http://www.joda.org/joda-time/apidocs/org/joda/time/Instant.html
[LocalDate_joda]: http://www.joda.org/joda-time/apidocs/org/joda/time/LocalDate.html
[LocalTime_joda]: http://www.joda.org/joda-time/apidocs/org/joda/time/LocalTime.html


#### Primitive types

Time can also be expressed as simple durations:

* [SimpleTimestampCodec] maps `timestamp` to a primitive Java `long` representing the number of milliseconds since the
  Epoch;
* [SimpleDateCodec] maps `date` to a primitive Java `int` representing the number of days since the Epoch.

There is no extra codec for `time`, because by default the driver already maps that type to a `long` representing the
number of milliseconds since midnight.

[SimpleTimestampCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/date/SimpleTimestampCodec.html
[SimpleDateCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/date/SimpleDateCodec.html


### Enums

Enums can be mapped in two different ways:

[EnumNameCodec] stores enum constants as a `varchar` representing their [name]:

```java
enum State {INIT, RUNNING, STOPPING, STOPPED}

cluster.getConfiguration().getCodecRegistry()
        .register(new EnumNameCodec<State>(State.class));

// schema: create table name_example(id int PRIMARY KEY, state text)
session.execute("insert into name_example (id, state) values (1, ?)", State.INIT);
// state is saved as 'INIT'
```

[EnumOrdinalCodec] stores enum constants as an `int` representing their [ordinal]:

```java
enum State {INIT, RUNNING, STOPPING, STOPPED}

cluster.getConfiguration().getCodecRegistry()
        .register(new EnumOrdinalCodec<State>(State.class));

// schema: create table ordinal_example(id int PRIMARY KEY, state int)
session.execute("insert into ordinal_example (id, state) values (1, ?)", State.INIT);
// state saved as 0
```

Note that if you registered an `EnumNameCodec` and an `EnumOrdinalCodec` _for the same enum_ at the same time, there could be a problem when executing [simple statements](../../statements/simple/), because in a simple statement, the target CQL type of a given query parameter is not known in advance, so the driver, on a best-effort attempt, will pick one or the other, whichever was registered first. If the chosen codec proves to be the wrong one, the request would fail on the server side.

In practice, this is unlikely to happen, because you'll probably stick to a single CQL type for a given enum type;
however, if you ever run into that issue, the workaround is to use [prepared statements](../../statements/prepared/), for which the driver knows the CQL type and can pick the exact codec.

[EnumNameCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/enums/EnumNameCodec.html
[EnumOrdinalCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/enums/EnumOrdinalCodec.html
[name]: https://docs.oracle.com/javase/8/docs/api/java/lang/Enum.html#name--
[ordinal]: https://docs.oracle.com/javase/8/docs/api/java/lang/Enum.html#ordinal--


### JSON

#### Jackson

[JacksonJsonCodec] maps a Java object to a JSON string stored into a `varchar` column, using the popular [Jackson]
library.

It requires an explicit dependency on Jackson in your application:

```xml
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
  <version>2.6.3</version>
</dependency>
```

```java
public class User {
    private int id;
    private String name;

    @JsonCreator
    public User(@JsonProperty("id") int id, @JsonProperty("name") String name) {
        this.id = id;
        this.name = name;
    }

    ... // getters and setters
}

cluster.getConfiguration().getCodecRegistry()
        .register(new JacksonJsonCodec<User>(User.class));

// schema: create table example(id int primary key, owner varchar);
session.execute("insert into example (id, owner) values (1, ?)",
        new User(1, "root"));
// owner saved as '{"id":1,"name":"root"}'
```

[JacksonJsonCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/json/JacksonJsonCodec.html
[Jackson]: http://wiki.fasterxml.com/JacksonHome


#### JSR 353

[Jsr353JsonCodec] maps a [JsonStructure] to a JSON string stored into a `varchar` column, using the JSR-353 standard.

It requires an explicit dependency on the JSR-353 API and an implementation:

```xml
<dependency>
  <groupId>javax.json</groupId>
  <artifactId>javax.json-api</artifactId>
  <version>1.0</version>
</dependency>

<!-- Reference implementation -->
<dependency>
  <groupId>org.glassfish</groupId>
  <artifactId>javax.json</artifactId>
  <version>1.0.4</version>
</dependency>
```

```java
cluster.getConfiguration().getCodecRegistry()
        .register(new Jsr353JsonCodec());

// schema: create table example(id int primary key, owner varchar);
session.execute("insert into example (id, owner) values (1, ?)",
        javax.json.Json.createObjectBuilder()
                .add("id", 1)
                .add("name", "root")
                .build());
// owner saved as '{"id":1,"name":"root"}'
```


[Jsr353JsonCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/json/Jsr353JsonCodec.html
[JsonStructure]: https://javaee-spec.java.net/nonav/javadocs/javax/json/JsonStructure.html


### Optional

"Optional" types provide an alternate way to represent missing values, in an attempt to avoid null pointer errors. With
these codecs, missing values will be mapped to `NULL` in the database.

#### JDK8

[OptionalCodec] transforms another codec to wrap its Java type in a JDK8 [Optional].

```java
import com.datastax.driver.extras.codecs.jdk8.OptionalCodec;
import com.google.common.reflect.TypeToken;
import java.util.Optional;

// The built-in driver codec, that maps CQL's int to Java's Integer
TypeCodec<Integer> intCodec = TypeCodec.cint();

// A codec that maps CQL's int to Java's Optional<Integer>
OptionalCodec<Integer> optionalIntCodec = new OptionalCodec<Integer>(intCodec);

cluster.getConfiguration().getCodecRegistry()
        .register(optionalIntCodec);

TypeToken<Optional<Integer>> optionalIntToken = new TypeToken<Optional<Integer>>() {};

// schema: create table example (id int primary key, v int);
PreparedStatement pst = session.prepare("insert into example (id, v) values (?, ?)");

Optional<Integer> v1 = Optional.empty();
session.execute(pst.bind()
        .setInt("id", 1)
        .set("v", v1, optionalIntToken));
// v1 saved as NULL

Optional<Integer> v2 = Optional.of(12);
session.execute(pst.bind()
        .setInt("id", 2)
        .set("v", v2, optionalIntToken));
// v2 saved as 12
```

This example is a bit more complex: `Optional<Integer>` is a generic type, so we can't use simple statements, because
they select codecs according to the runtime types of their values (and we would lose the `<Integer>` part with type
erasure).

For the same reason, we need to give a type hint when setting "v", in the form of a [TypeToken]. Note that this is an
anonymous inner class; we recommend storing these tokens as constants in a utility class, to avoid creating them too
often.

[OptionalCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/jdk8/OptionalCodec.html
[Optional]: https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html
[TypeToken]: http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/reflect/TypeToken.html

#### Guava

[OptionalCodec][OptionalCodec_guava] transforms another codec to wrap its Java type in a Guava
[Optional][Optional_guava].


```java
import com.datastax.driver.extras.codecs.guava.OptionalCodec;
import com.google.common.base.Optional;
import com.google.common.reflect.TypeToken;

// The built-in driver codec, that maps CQL's int to Java's Integer
TypeCodec<Integer> intCodec = TypeCodec.cint();

// A codec that maps CQL's int to Guava's Optional<Integer>
OptionalCodec<Integer> optionalIntCodec = new OptionalCodec<Integer>(intCodec);

cluster.getConfiguration().getCodecRegistry()
        .register(optionalIntCodec);

TypeToken<Optional<Integer>> optionalIntToken = new TypeToken<Optional<Integer>>() {};

// schema: create table example (id int primary key, v int);
PreparedStatement pst = session.prepare("insert into example (id, v) values (?, ?)");

Optional<Integer> v1 = Optional.absent();
session.execute(pst.bind()
        .setInt("id", 1)
        .set("v", v1, optionalIntToken));
// v1 saved as NULL

Optional<Integer> v2 = Optional.of(12);
session.execute(pst.bind()
        .setInt("id", 2)
        .set("v", v2, optionalIntToken));
// v2 saved as 12
```

See the JDK8 Optional section above for explanations about [TypeToken].

[OptionalCodec_guava]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/guava/OptionalCodec.html
[Optional_guava]: http://docs.guava-libraries.googlecode.com/git-history/v16.0.1/javadoc/com/google/common/base/Optional.html


### Arrays

By default, the driver maps CQL lists to collections of Java objects. For high-performance scenarios, and when the
element type maps to a Java primitive, you might want to map to an array instead, to avoid unnecessary boxing and
unboxing.

[IntArrayCodec] allows you to do that for lists of integers:

```java
cluster.getConfiguration().getCodecRegistry()
        .register(new IntArrayCodec());

// schema: create table example (i int primary key, l list<int>)
session.execute("insert into example (i, l) values (1, ?)",
        new int[]{1, 2, 3});
// array saved as [1,2,3]
```

Package [com.datastax.driver.extras.codecs.arrays][arrays] contains similar codecs for all primitive types, and
[ObjectArrayCodec] to map arrays of objects.

[IntArrayCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/arrays/IntArrayCodec.html
[ObjectArrayCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/arrays/ObjectArrayCodec.html
[arrays]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/arrays/package-summary.html

### Abstract utilities

The module provides two abstract classes that act as building blocks for your own codecs:

[MappingCodec] is useful when you can easily convert your Java type to another type that already has a codec.
For example, if you have a `Price` object that translates to a plain `Integer`, you can piggyback on the built-in
integer codec, and you'll only have to write two simple conversion methods:

```java
public static class PriceCodec extends MappingCodec<Price, Integer> {
    public PriceCodec() { super(TypeCodec.cint(), Price.class); }

    @Override
    protected Integer serialize(Price value) { ... }

    @Override
    protected Price deserialize(Integer value) { ... }
}
```

For a more detailed example, you can look at the `OptionalCodec` implementations in the source code.

[ParsingCodec] is similar, but specialized for text columns. See `EnumNameCodec` for an example.

These two classes are convenient, but since they perform conversions in two steps, extending them is not necessarily the
optimal approach. If performance is paramount, it's better to start from scratch and convert your objects to
`ByteBuffer` directly.

[MappingCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/MappingCodec.html
[ParsingCodec]: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/extras/codecs/ParsingCodec.html
