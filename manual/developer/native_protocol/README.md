## Native protocol layer

The native protocol layer encodes protocol messages into binary, before they are sent over the
network.

This part of the code lives in its own project:
[native-protocol](https://github.com/datastax/native-protocol). We extracted it to make it reusable
([Simulacron](https://github.com/datastax/simulacron) also uses it).

The protocol specifications are available in
[native-protocol/src/main/resources](https://github.com/datastax/native-protocol/tree/1.x/src/main/resources).
These files originally come from Cassandra, we copy them over for easy access. Note that, if the
latest version is a beta (this is the case for v5 at the time of writing -- September 2019), the
specification might not be up to date. Always compare with the latest revision in
[cassandra/doc](https://github.com/apache/cassandra/tree/trunk/doc).


For a broad overview of how protocol types are used in the driver, let's step through an example:

* the user calls `session.execute()` with a `SimpleStatement`. The protocol message for a
  non-prepared request is `QUERY`;
* `CqlRequestHandler` uses `Conversions.toMessage` to convert the statement into a
  `c.d.o.protocol.internal.request.Query`;
* `InflightHandler.write` assigns a stream id to that message, and wraps it into a
  `c.d.o.protocol.internal.Frame`;
* `FrameEncoder` uses `c.d.o.protocol.internal.FrameCodec` to convert the frame to binary.

(All types prefixed with `c.d.o.protocol.internal` belong to the native-protocol project.)

A similar process happens on the response path: decode the incoming binary payload into a protocol
message, then convert the message into higher-level driver objects: `ResultSet`, `ExecutionInfo`,
etc. 

### Native protocol types

#### Messages

Every protocol message is identified by an opcode, and has a corresponding `Message` subclass.

A `Frame` wraps a message to add metadata, such as the protocol version and stream id.

```ditaa
+-------+ contains +------------+
| Frame +--------->+ Message    +
+-------+          +------------+
                   | int opcode |
                   +--+---------+
                      |
                      |    +---------+
                      +----+ Query   |
                      |    +---------+
                      |
                      |    +---------+
                      +----+ Execute |
                      |    +---------+
                      |
                      |    +---------+
                      +----+ Rows    |
                           +---------+
                           
                           etc.
```

All value classes are immutable, but for efficiency they don't make defensive copies of their
fields. If these fields are mutable (for example collections), they shouldn't be modified after
creating a message instance.

The code makes very few assumptions about how the messages will be used. Data is often represented
in the most simple way. For example, `ProtocolConstants` uses simple integer constants to represent
protocol codes (enums wouldn't work at that level, because we need to add new codes in the DSE
driver); the driver generally rewraps them in more type-safe structures before exposing them to
higher-level layers.

#### Encoding/decoding

For every message, there is a corresponding `Message.Codec` for encoding and decoding. A
`FrameCodec` relies on a set of message codecs, for one or more protocol versions. Given an incoming
frame, it looks up the right message codec to use, based on the protocol version and opcode.
Optionally, it compresses frame bodies with a `Compressor`.
 

```ditaa
+-----------------+                +-------------------+
| FrameCodec[B]   +----------------+ PrimitiveCodec[B] |
+-----------------+                +-------------------+
| B encode(Frame) |
| Frame decode(B) +-------+        +---------------+
+------+----------+       +--------+ Compressor[B] |
       |                           +---------------+
       |
       |                           +-------------------+
       +---------------------------+ Message.Codec     |
           1 codec per opcode      +-------------------+
           and protocol version    | B encode(Message) |
                                   | Message decode(B) |
                                   +-------------------+
```

Most of the time, you'll want to use the full set of message codecs for a given protocol version.
`CodecGroup` provides a convenient way to register multiple codecs at once. The project provides
default implementations for all supported protocol version, both for clients like the driver (e.g.
encode `QUERY`, decode `RESULT`), or servers like Simulacron (decode `QUERY` encode `RESULT`).


```ditaa
+-------------+
|  CodecGroup |
+------+------+
       |
       |    +------------------------+
       +----+ ProtocolV3ClientCodecs |
       |    +------------------------+
       |
       |    +------------------------+
       +----+ ProtocolV3ServerCodecs |
       |    +------------------------+
       |
       |    +------------------------+
       +----+ ProtocolV4ClientCodecs |
       |    +------------------------+
       |
       |    +------------------------+
       +----+ ProtocolV4ClientCodecs |
       |    +------------------------+
       |
       |    +------------------------+
       +----+ ProtocolV5ClientCodecs |
       |    +------------------------+
       |
       |    +------------------------+
       +----+ ProtocolV5ClientCodecs |
            +------------------------+
```

The native protocol layer is agnostic to the actual binary representation. In the driver, this
happens to be a Netty `ByteBuf`, but the encoding logic doesn't need to be aware of that. This is
expressed by the type parameter `B` in `FrameCodec<B>`. `PrimitiveCodec<B>` abstracts the basic
primitives to work with a `B`: how to create an instance, read and write data to it, etc.

```java
public interface PrimitiveCodec<B> {
  B allocate(int size);
  int readInt(B source);
  void writeInt(int i, B dest);
  ...
}
```

Everything else builds upon those primitives. By just switching the `PrimitiveCodec` implementation,
the whole protocol layer could be reused with a different type, such as `byte[]`.

In summary, to initialize a `FrameCodec`, you need:

* a `PrimitiveCodec`;
* a `Compressor` (optional);
* one or more `CodecGroup`s.

### Integration in the driver

The driver initializes its `FrameCodec` in `DefaultDriverContext.buildFrameCodec()`.

* the primitive codec is `ByteBufPrimitiveCodec`, which implements the basic primitives for Netty's
  `ByteBuf`;
* the compressor comes from `DefaultDriverContext.buildCompressor()`, which determines the
  implementation from the configuration;
* it is built with `FrameCodec.defaultClient`, which is a shortcut to use the default client groups:
  `ProtocolV3ClientCodecs`, `ProtocolV4ClientCodecs` and `ProtocolV5ClientCodecs`.

### Extension points

The default frame codec can be replaced by [extending the
context](../common/context/#overriding-a-context-component) to override `buildFrameCodec`. This
can be used to add or remove a protocol version, or replace a particular codec.

If protocol versions change, `ProtocolVersionRegistry` will likely be affected as well.

Also, depending on the nature of the protocol changes, the driver's [request
processors](../request_execution/#request-processors) might require some adjustments: either replace
them, or introduce separate ones (possibly with new `executeXxx()` methods on a custom session
interface).
