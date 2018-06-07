## Detachable types

Some driver components need to keep an internal reference to their originating [Session]. Under
specific circumstances, they can lose that reference, and you might need to reattach them.

Namely, these components are:

* all [DataType] instances, in particular [tuples](../tuples/) and [UDTs](../udts/);
* [result rows][Row], and their [column definitions][ColumnDefinition].

Detachable types are an advanced topic, that should only be a concern for 3rd-party tool developers.
If you're simply executing requests and reading results, you probably won't need to worry about
them. See the [bottom line](#bottom-line) at the end of this page for details.

### Rationale

Detachable components are those that encode or decode their fields themselves. For example, when you
set a field on a [tuple value](../tuples):

```java
tupleValue = tupleValue.setString(0, "foo");
```

The string "foo" is encoded immediately, and the `TupleValue` object holds a reference to the binary
data. It is done that way in order to fail fast on encoding errors, and avoid duplicate work if you
reuse the tuple instance in multiple requests.

Encoding requires session-specific information:

* the [CodecRegistry] instance (in case it contains [custom codecs](../custom_codecs/));
* the [protocol version](../native_protocol/) (because the binary format can change across
  versions).

Therefore the tuple value needs a reference to the session to access those two objects. 

### Detached objects

Detachable types implement the [Detachable] interface, which has an `isDetached()` method to check
the current status. Whenever you get an object from the driver, it is attached:

* reading a row from a result set:

    ```java
    ResultSet rs = session.execute("SELECT * FROM foo");
    Row row = rs.one();
    assert !row.isDetached();
    ```

* reading a data type from schema metadata:

    ```java
    UserDefinedType udt = session.getMetadata().getKeyspace("ks").getUserDefinedType("type1");
    assert !udt.isDetached();
    ```

There is no way to detach an object explicitly. This can only happen when:

* deserializing a previously serialized instance (we're referring here to [Java serialization]);
* attaching an object to another session;
* creating a [tuple](../tuples/) or [UDT](../udts/) definition manually:

    ```java
    TupleType tupleType = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT, DataTypes.FLOAT);
    assert tupleType.isDetached();
    ```

When an object is detached, it uses a [default codec registry][CodecRegistry#DEFAULT] that only
handles built-in types, and the latest non-beta protocol version supported by the driver. This might
be good enough for you if you don't use any custom codec (the binary format has been stable across
modern protocol versions).

### Reattaching

Use `attach()` to reattach an object to the session:

```java
TupleType tupleType = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT, DataTypes.FLOAT);
assert tupleType.isDetached();

tupleType.attach(session.getContext());
assert !tupleType.isDetached();

// Now this will use the session's custom codecs if field 0 isn't a text CQL type:
TupleValue tupleValue = tupleType.newValue().setString(0, "foo");
```

When you pass a detached type to the session (for example by executing a request with a tuple value
based on a detached tuple type), it will automatically be reattached.

### Bottom line

You only need to worry about detachable types if you serialize driver rows or data types, or if you
create tuple or UDT types manually.

Even then, the defaults used by detached objects might be good enough for you:

* the default codec registry works if you don't have any [custom codec](../custom_codecs/);
* the binary encoding format is stable across modern protocol versions. The last changes were for
  collection encoding from v2 to v3; Java driver 4 only supports v3 and above. When in doubt, check
  the "Changes" section of the [protocol specifications].
  
Otherwise, just make sure you reattach objects any time you deserialize them or create them from
scratch.

[CodecRegistry]:         https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/type/codec/registry/CodecRegistry.html
[CodecRegistry#DEFAULT]: https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/type/codec/registry/CodecRegistry.html#DEFAULT
[DataType]:              https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/type/DataType.html
[Detachable]:            https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/detach/Detachable.html
[Session]:               https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/session/Session.html
[ColumnDefinition]:      https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/ColumnDefinition.html
[Row]:                   https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/Row.html

[Java serialization]: https://docs.oracle.com/javase/tutorial/jndi/objects/serial.html
[protocol specifications]: https://github.com/datastax/native-protocol/tree/1.x/src/main/resources
