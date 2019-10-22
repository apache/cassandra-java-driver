## Netty pipeline

With the [protocol layer](../native_protocol) in place, the next step is to build the logic for a
single server connection.

We use [Netty](https://netty.io/) for network I/O (to learn more about Netty, [this
book](https://www.manning.com/books/netty-in-action) is an excellent resource).

```ditaa
   +----------------+
   | ChannelFactory |
   +----------------+
   | connect()      |
   +-------+--------+
           |                              Application
           |creates     +----------------------------------------------+
           V            | Outgoing                                     |
   +-------+--------+   |  |         +---------------------+        ^  |
   | DriverChannel  |   |  |         | ProtocolInitHandler |        |  |
   +-------+--------+   |  |         +---------------------+        |  |
           |            |  |                                        |  |
   +-------+--------+   |  |         +---------------------+        |  |
   |    Channel     |   |  |         |   InFlightHandler   |        |  |
   |    (Netty)     |   |  |         +---------------------+        |  |
   +-------+--------+   |  |                                        |  |
           |            |  |         +---------------------+        |  |
   +-------+--------+   |  |         |   Heartbeathandler  |        |  |
   |ChannelPipeline +---+  |         +---------------------+        |  |
   |    (Netty)     |   |  |                                        |  |
   +----------------+   |  |  +--------------+   +--------------+   |  |
                        |  |  | FrameEncoder |   | FrameDecoder |   |  |
                        |  |  +--------------+   +--------------+   |  |
                        |  |                                        |  |
                        |  |         +---------------------+        |  |
                        |  |         |      SslHandler     |        |  |
                        |  |         |       (Netty)       |        |  |
                        |  V         +---------------------+        |  |
                        |                                     Incoming |
                        +----------------------------------------------+
                                             Network                   
```

Each Cassandra connection is based on a Netty `Channel`. We wrap it into our own `DriverChannel`,
that exposes higher-level operations. `ChannelFactory` is the entry point for other driver
components; it handles protocol negotiation for the first channel.

A Netty channel has a *pipeline*, that contains a sequence of *handlers*. As a request is sent, it
goes through the pipeline top to bottom; each successive handler processes the input, and passes the
result to the next handler. Incoming responses go the other way.

Our pipeline is configured with the following handlers:

### SslHandler

The implementation is provided by Netty (all the others handlers are custom implementations).

Internally, handler instances are provided by `SslHandlerFactory`. At the user-facing level, this is 
abstracted behind `SslEngineFactory`, based on Java's default SSL implementation.

See also the [Extension points](#extension-points) section below.

### FrameEncoder and FrameDecoder

This is where we integrate the protocol layer, as explained
[here](../native_protocol/#integration-in-the-driver).

Unlike the other pipeline stages, we use separate handlers for incoming and outgoing messages.

### HeartbeatHandler

The heartbeat is a background request sent on inactive connections (no reads since x seconds), to
make sure that they are still alive, and prevent them from being dropped by a firewall. This is
similar to TCP_KeepAlive, but we provide an application-side alternative because users don't always
have full control over their network configuration.

`HeartbeatHandler` is based on Netty's built-in `IdleStateHandler`, so there's not much in there
apart from the details of the control request.

### InFlightHandler

This handler is where most of the connection logic resides. It is responsible for:

* writing regular requests:
  * find an available stream id;
  * store the `ResponseCallback` provided by the client under that id;
  * when the response comes in, retrieve the callback and complete it;
* cancelling a request;
* switching the connection to a new keyspace (if a USE statement was executed through the session);
* handling shutdown: gracefully (allow all request to complete), or forcefully (error out all
  requests).
  
The two most important methods are:

* `write(ChannelHandlerContext, Object, ChannelPromise)`: processes outgoing messages. We accept
  different types of messages, because cancellation and shutdown also use that path. See
  `DriverChannel`, which abstracts those details.
* `channelRead`: processes incoming responses.

Netty handlers are confined to the channel's event loop (a.k.a I/O thread). Therefore the code
doesn't have to be concurrent, fields can be non-volatile and methods are guaranteed not to race
with each other.

In particular, a big difference from driver 3 is that stream ids are assigned within the event loop,
instead of from client code before writing to the channel (see also [connection
pooling](../request_execution/#connection_pooling)). `StreamIdGenerator` is not thread-safe.

All communication between the handler and the outside world must be done through messages or channel
events. There are 3 exceptions to this rule: `getAvailableIds`, `getInflight` and `getOrphanIds`,
which are based on volatile fields. They are all used for metrics, and `getAvailableIds` is also
used to balance the load over connections to the same node (see `ChannelSet`).

### ProtocolInitHandler

This handler manages the protocol initialization sequence on a newly established connection (see the
`STARTUP` message in the protocol specification).

Most of the logic resides in `InitRequest.onResponse`, which acts as a simple state machine based on
the last request sent.

There is also a bit of custom code to ensure that the channel is not made available to clients
before the protocol is ready. This is abstracted in the parent class `ConnectInitHandler`.

Once the initialization is complete, `ProtocolInitHandler` removes itself from the pipeline.

### Extension points 

#### NettyOptions

The `advanced.netty` section in the [configuration](../../core/configuration/reference/) exposes a
few high-level options.

For more elaborate customizations, you can [extend the
context](../common/context/#overriding-a-context-component) to plug in a custom `NettyOptions`
implementation. This allows you to do things such as:
 
* reusing existing event loops;
* using Netty's [native Epoll transport](https://netty.io/wiki/native-transports.html);
* adding custom handlers to the pipeline.

#### SslHandlerFactory

The [user-facing API](../../core/ssl/) (`advanced.ssl-engine-factory` in the configuration, or
`SessionBuilder.withSslContext` / `SessionBuilder.withSslEngineFactory`) only supports Java's
default SSL implementation.

The driver can also work with Netty's [native
integration](https://netty.io/wiki/requirements-for-4.x.html#tls-with-openssl) with OpenSSL or
boringssl. This requires a bit of custom development against the internal API:

* add a dependency to one of the `netty-tcnative` artifacts, following [these
  instructions](http://netty.io/wiki/forked-tomcat-native.html);
* implement `SslHandlerFactory`. Typically:
  * the constructor will create a Netty [SslContext] with [SslContextBuilder.forClient], and store
    it in a field;
  * `newSslHandler` will delegate to one of the [SslContext.newHandler] methods;  
* [extend the context](../common/context/#overriding-a-context-component) and override
  `buildSslHandlerFactory` to plug your custom implementation.

[SslContext]: https://netty.io/4.1/api/io/netty/handler/ssl/SslContext.html
[SslContext.newHandler]: https://netty.io/4.1/api/io/netty/handler/ssl/SslContext.html#newHandler-io.netty.buffer.ByteBufAllocator-
[SslContextBuilder.forClient]: https://netty.io/4.1/api/io/netty/handler/ssl/SslContextBuilder.html#forClient--