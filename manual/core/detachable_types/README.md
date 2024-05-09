<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Detachable types

### Quick overview

Advanced topic, only needed if you use Java serialization with driver rows or data types, or create
tuple or UDT types manually.

-----

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

### Sharing data across sessions

If you're reading data from one session and writing it into another, you should take a few extra
precautions:

* if you use custom codecs, they should obviously be registered with both sessions;

* if the protocol version is different, you should avoid sharing UDT and tuple types; keep a
  separate set of definitions for each session, and copy the values field by field:
  
    ```java
    Row row = session1.execute("SELECT QUERY...").one();
    UdtValue user1 = row.getUdtValue("user");

    // Don't pass user1 to session2: create a new copy from userType2 instead
    UserDefinedType userType2 =
        session2.getMetadata().getKeyspace("ks").flatMap(ks -> ks.getUserDefinedType("user")).get();
    UdtValue user2 = userType2.newValue();
    user2.setString("first_name", user1.getString("first_name"));
    user2.setString("last_name", user1.getString("last_name"));

    session2.execute(SimpleStatement.newInstance("INSERT QUERY...", user2));
    ```
    
    This will ensure that UDT definition are not accidentally reattached to the wrong session, and
    use the correct protocol version to encode values.
    

### Bottom line

You only need to worry about detachable types if you serialize driver rows or data types, or if you
create tuple or UDT types manually.

Even then, the defaults used by detached objects might be good enough for you:

* the default codec registry works if you don't have any [custom codec](../custom_codecs/);
* the binary encoding format is stable across modern protocol versions. The last changes were for
  collection encoding from v2 to v3; Java Driver 4 only supports v3 and above. When in doubt, check
  the "Changes" section of the [protocol specifications].
  
Otherwise, just make sure you reattach objects any time you deserialize them or create them from
scratch.

[CodecRegistry]:         https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/type/codec/registry/CodecRegistry.html
[CodecRegistry#DEFAULT]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/type/codec/registry/CodecRegistry.html#DEFAULT
[DataType]:              https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/type/DataType.html
[Detachable]:            https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/detach/Detachable.html
[Session]:               https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/Session.html
[ColumnDefinition]:      https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/ColumnDefinition.html
[Row]:                   https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/cql/Row.html

[Java serialization]: https://docs.oracle.com/javase/tutorial/jndi/objects/serial.html
[protocol specifications]: https://github.com/datastax/native-protocol/tree/1.x/src/main/resources
