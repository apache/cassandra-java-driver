## Temporal types

### Quick overview

This page provides more details about the various CQL time types, and the Java types they are mapped
to in the driver.

| CQL | Java | |
|---|---|---|
|`date` | `java.time.LocalDate` ||
|`time` | `java.time.LocalTime` ||
|`timestamp` | `java.time.Instant` | No time zone. Use `Instant.atZone` or register [TypeCodecs.ZONED_TIMESTAMP_SYSTEM], [TypeCodecs.ZONED_TIMESTAMP_UTC] or [TypeCodecs.zonedTimestampAt()] |
|`duration` | [CqlDuration] | Custom driver type; can't be accurately represented by any of the `java.time` types. |

-----

### Date and time

CQL types `date` and `time` map directly to `java.time.LocalDate` and `java.time.LocalTime`.

These are simple, time-zone-free representations of date-only (`yyyy-mm-dd`) and time-only
(`HH:MM:SS[.fff]`) types.

### Timestamp

CQL type `timestamp` is the date-and-time representation, stored as a number of milliseconds since
the epoch (01/01/1970 UTC).
 
 
#### No time zone

`timestamp` does **not** store a time zone. This is not always obvious because clients generally do
use one for display. For instance, the following CQLSH snippet is from a machine in Pacific time: 

```
cqlsh> CREATE TABLE test(t timestamp PRIMARY KEY);
cqlsh> INSERT INTO test (t) VALUES (dateof(now()));
cqlsh> SELECT * FROM test;

 t
---------------------------------
 2018-11-07 08:50:52.433000-0800
```

It looks like the timestamp has a zone (`-0800`), but it is actually the client's. If you force
CQLSH to a different zone and observe the same data, it will be displayed differently:

```
$ TZ=UTC cqlsh
cqlsh> SELECT * FROM test;

 t
---------------------------------
 2018-11-07 16:50:52.433000+0000
```

Internally, Cassandra only stores the raw number of milliseconds. You can observe that with a cast:  

```
cqlsh> SELECT cast(t as bigint) FROM test;

 cast(t as bigint)
-------------------
     1541609452433
```

#### Java equivalent

By default, the driver maps `timestamp` to `java.time.Instant`. This Java type is the closest to the
internal representation; in particular, it does not have a time zone. On the downside, this means
you can't directly extract calendar fields (year, month, etc.). You need to call `atZone` to perform
the conversion: 

```java
Row row = session.execute("SELECT t FROM test").one();
Instant instant = row.getInstant("t");
ZonedDateTime dateTime = instant.atZone(ZoneId.of("America/Los_Angeles"));
System.out.println(dateTime.getYear());
```

Conversely, you can convert a `ZonedDateTime` back to an `Instant` with `toInstant`.

If you want to automate those `atZone`/`toInstant` conversions, the driver comes with an optional
`ZonedDateTime` codec, that must be registered explicitly with the session:

```java
CqlSession session = CqlSession.builder()
    .addTypeCodecs(TypeCodecs.ZONED_TIMESTAMP_UTC)
    .build();

Row row = session.execute("SELECT t FROM test").one();
ZonedDateTime dateTime = row.get("t", GenericType.ZONED_DATE_TIME);
``` 

There are various constants and methods to obtain a codec instance for a particular zone:

* [TypeCodecs.ZONED_TIMESTAMP_SYSTEM]\: system default;
* [TypeCodecs.ZONED_TIMESTAMP_UTC]\: UTC;
* [TypeCodecs.zonedTimestampAt()]\: user-provided.

Which zone you choose is application-dependent. The driver doesn't map to `ZonedDateTime` by default
because it would have to make an arbitrary choice; we want you to think about time zones explicitly
before you decide to use that type.

#### Millisecond-only precision

As already stated, `timestamp` is stored as a number of milliseconds. If you try to write an
`Instant` or `ZonedDateTime` with higher precision through the driver, the sub-millisecond part will
be truncated:

```java
CqlSession session =
    CqlSession.builder()
        .addTypeCodecs(TypeCodecs.ZONED_TIMESTAMP_UTC)
        .build();

ZonedDateTime valueOnClient = ZonedDateTime.parse("2018-11-07T16:50:52.433395762Z");
                                                // sub-millisecond digits ^^^^^^
session.execute(
    SimpleStatement.newInstance("INSERT INTO test (t) VALUES (?)", valueOnClient));

ZonedDateTime valueInDb =
    session.execute("SELECT * FROM test").one().get(0, GenericType.ZONED_DATE_TIME);
System.out.println(valueInDb);
// Prints "2018-11-07T16:50:52.433Z"
```

### Duration

CQL type `duration` represents a period in months, days and nanoseconds. The driver maps it to a
custom type: [CqlDuration].

We deliberately avoided `java.time.Period`, because it does not contain a nanoseconds part as
`CqlDuration` does; and we also avoided `java.time.Duration`, because it represents an absolute
time-based amount, regardless of the calendar, whereas `CqlDuration` manipulates conceptual days and
months instead. Thus a `CqlDuration` of "2 months" represents a different amount of time depending
on the date to which it is applied (because months have a different number of days, and because
daylight savings rules might also apply, etc).

`CqlDuration` implements `java.time.temporal.TemporalAmount`, so it interoperates nicely with the
JDK's built-in temporal types:

```java
ZonedDateTime dateTime = ZonedDateTime.parse("2018-10-04T00:00-07:00[America/Los_Angeles]");
System.out.println(dateTime.minus(CqlDuration.from("1h15s15ns")));
// prints "2018-10-03T22:59:44.999999985-07:00[America/Los_Angeles]"
```

[CqlDuration]:                       https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/data/CqlDuration.html
[TypeCodecs.ZONED_TIMESTAMP_SYSTEM]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#ZONED_TIMESTAMP_SYSTEM
[TypeCodecs.ZONED_TIMESTAMP_UTC]:    https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#ZONED_TIMESTAMP_UTC
[TypeCodecs.zonedTimestampAt()]:     https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#zonedTimestampAt-java.time.ZoneId-