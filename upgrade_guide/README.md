## Upgrade guide

### 3.x to 4.0.0

Java driver 4 is **not binary compatible** with previous versions. However, most of the concepts
remain unchanged, and the new API will look very familiar to 2.x and 3.x users.

#### Runtime requirements

The driver now requires Java 8. It does not depend on Guava anymore (we still use it internally but
it's shaded).

#### Packages

The root package names have changed. There are also more sub-packages than previously. See [API 
conventions] for important information about the naming conventions.
  
Generally, the public types have kept the same name, so you can use the "find class" feature in your
IDE to find out the new locations.

[API conventions]: ../manual/api_conventions

#### New configuration API

The configuration has been completely revamped. Instead of ad-hoc configuration classes, the default
configuration mechanism is now file-based, using the [TypeSafe Config] library. This is a better
choice for most deployments, since it allows configuration changes without recompiling the client
application. This is fully customizable, including loading from different sources, or completely
overriding the default implementation.

For more details, refer to the [manual](../manual/core/configuration).

[TypeSafe Config]: https://github.com/typesafehub/config

#### Expose interfaces, not classes

Most types in the public API are now interfaces (as opposed to 3.x: `Cluster`, statement classes, 
etc). The actual implementations are part of the internal API. This provides more flexibility in
client code (e.g. to wrap them and write delegates).

Thanks to Java 8, factory methods can now be part of these interfaces directly, e.g.
`Cluster.builder()`, `SimpleStatement.newInstance`.

#### Immutable statement types

Simple, bound and batch statements implementations are now all immutable. This makes them
automatically thread-safe: you don't need to worry anymore about sharing them or reusing them
between asynchronous executions.

One word of warning -- all mutating methods return a new instance, so make sure you don't
accidentally ignore their result:

```java
BoundStatement boundSelect = preparedSelect.bind();

// This doesn't work: setInt doesn't modify boundSelect in place:
boundSelect.setInt("k", key);
session.execute(boundSelect);

// Instead, do this:
boundSelect = boundSelect.setInt("k", key);
```

Note that, as indicated in the previous section, the public API exposes these types as interfaces:
if for some reason you prefer a mutable implementation, it's possible to write your own.

#### Generic session API

`Session` is now a high-level abstraction capable of executing arbitrary requests. Out of the box,
the driver supports the same CQL queries as 3.x, and exposes familiar signatures
(`execute(Statement)`, `prepare(String)`, etc).

However, the request execution logic is completely pluggable, and supports arbitrary request types
as long as you write the boilerplate to convert them to protocol messages. In the future, we will
take advantage of that to provide:

* a reactive API;
* a high-performance implementation that exposes bare Netty buffers;
* specialized requests in our DataStax Enterprise driver.

If you're interested, take a look at `RequestProcessor`.

#### Dual result set APIs

In 3.x, both synchronous and asynchronous execution models shared a common result set
implementation. This made asynchronous usage [notably error-prone][3.x async paging], because of the
risk of accidentally triggering background synchronous fetches.

There are now two separate APIs: synchronous queries return a `ResultSet` which behaves like its 3.x
counterpart; asynchronous queries return a future of `AsyncResultSet`, a simplified type that only 
contains the rows of the current page. When iterating asynchronously, you no longer need to stop the
iteration manually: just consume all the rows in the iterator, and then call `fetchNextPage` to 
retrieve the next page asynchronously.

[3.x async paging]: http://docs.datastax.com/en/developer/java-driver/3.2/manual/async/#async-paging

#### Simplified request timeout

The driver-side request timeout -- defined by the `request.timeout` configuration option -- now
spans the <em>entire</em> request, including all retries, speculative executions, etc. In other
words, it's the maximum amount of time that the driver will spend processing the request. If it
fires, all pending tasks are cancelled, and a `DriverTimeoutException` is returned to the client.
(Note that the "cancellation" is only driver-side, currently the protocol does not provide a way to
tell the server to stop processing a request; if a message was "on the wire" when the timeout fired,
then the driver will simply ignore the response when it eventually comes back.)
 
This is in contrast to 3.x, where the timeout defined in the configuration was per retry, and a 
global timeout required specific user code.

#### Dedicated type for CQL identifiers

Instead of raw strings, the names of schema objects (keyspaces, tables, columns, etc.) are now 
wrapped in a dedicated `CqlIdentifier` type. This avoids ambiguities with regard to case
sensitivity.

For example, this type is used in schema metadata or when creating a session connected to a specific
keyspace. When manipulating "data containers" such as rows, UDTs and tuples, columns can also be
referenced by a `CqlIdentifier`; however, we've also kept a raw string variant for convenience, with
the same rules as in 3.x (see `GettableById` and `GettableByName` for details).