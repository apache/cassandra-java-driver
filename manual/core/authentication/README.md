## Authentication

Cassandra's binary protocol supports SASL-based authentication. To enable it, define an
`auth-provider` section in the [configuration](../configuration/)

```
datastax-java-driver {
  advanced.auth-provider {
    class = PlainTextAuthProvider
    username = cassandra
    password = cassandra
  }
}
```

Authentication must be configured before opening a session, it cannot be changed at runtime.

[PlainTextAuthProvider] is provided out of the box, for simple username/password authentication
(intended to work with the server-side `PasswordAuthenticator`). The credentials can be changed at
runtime, they will be used for new connection attempts once the configuration gets reloaded. 

You can also write your own provider; it must implement [AuthProvider] and declare a public
constructor with a [DriverContext] argument.


[SASL]: https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer

[AuthProvider]:          https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/auth/AuthProvider.html
[DriverContext]:         https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/context/DriverContext.html
[PlainTextAuthProvider]: https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/internal/core/auth/PlainTextAuthProvider.html
