## Authentication

Cassandra's binary protocol supports [SASL]-based authentication. To use it, you must provide an
*auth provider* that will authenticate with the server every time a new connection gets established.

This can be done in two ways: 

### In the configuration

Define an `auth-provider` section in the [configuration](../configuration/):

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

`PlainTextAuthProvider` is provided out of the box, for simple username/password authentication
(intended to work with the server-side `PasswordAuthenticator`). The credentials can be changed at
runtime, they will be used for new connection attempts once the configuration gets reloaded. 

You can also write your own provider; it must implement [AuthProvider] and declare a public
constructor with a [DriverContext] argument.

```
datastax-java-driver {
  advanced.auth-provider {
    class = com.mycompany.MyCustomAuthProvider
    ... // any custom options your provider might use
  }
}
```

### Programmatically

You can also pass an authenticator instance while building the session:

```java
CqlSession session =
    CqlSession.builder()
        .withAuthProvider(new MyCustomAuthProvider())
        .build();
```

For convenience, there is a shortcut that takes the credentials directly. This is equivalent to
using `PlainTextAuthProvider` in the configuration:

```java
CqlSession session =
    CqlSession.builder()
        .withAuthCredentials("user", "pass")
        .build();
```

One downside of `withAuthCredentials` is that the credentials are stored in clear text in memory;
this means they are vulnerable to an attacker who is able to perform memory dumps. If this is not
acceptable for you, consider writing your own [AuthProvider] implementation (the internal class
`PlainTextAuthProviderBase` is a good starting point). 


[SASL]: https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer

[AuthProvider]:          https://docs.datastax.com/en/drivers/java/4.1/com/datastax/oss/driver/api/core/auth/AuthProvider.html
[DriverContext]:         https://docs.datastax.com/en/drivers/java/4.1/com/datastax/oss/driver/api/core/context/DriverContext.html
