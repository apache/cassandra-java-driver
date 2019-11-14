## Authentication

### Quick overview

* `advanced.auth-provider` in the configuration.
* disabled by default. Also available: plain-text credentials, GSSAPI (DSE only), or write your own.
* can also be defined programmatically:
  [CqlSession.builder().withAuthCredentials][SessionBuilder.withAuthCredentials] or
  [CqlSession.builder().withAuthProvider][SessionBuilder.withAuthProvider].

-----

Cassandra's binary protocol supports [SASL]-based authentication. To use it, you must provide an
*auth provider* that will authenticate with the server every time a new connection gets established.

This can be done in two ways: 

### In the configuration

Define an `auth-provider` section in the [configuration](../configuration/):

```
datastax-java-driver {
  advanced.auth-provider {
    class = ...
  }
}
```

The auth provider must be configured before opening a session, it cannot be changed at runtime.

#### Plain text

`PlainTextAuthProvider` supports simple username/password authentication (intended to work with the
server-side `PasswordAuthenticator`). The credentials can be changed at runtime, they will be used
for new connection attempts once the configuration gets reloaded.

```
datastax-java-driver {
  advanced.auth-provider {
    class = PlainTextAuthProvider
    username = cassandra
    password = cassandra
  }
}
```

When connecting to DSE, an optional `authorization-id` can also be specified. It will be used for
proxy authentication (logging in as another user or role). If you try to use this feature with an
authenticator that doesn't support it, the authorization id will be ignored.

```
datastax-java-driver {
  advanced.auth-provider {
    class = PlainTextAuthProvider
    username = user
    password = pass
    authorization-id = otherUserOrRole
  }
}
```

Note that, for backward compatibility with previous driver versions, you can also use the class name
`DsePlainTextAuthProvider` to enable this provider.

#### GSSAPI (DSE only)

`DseGssApiAuthProvider` supports GSSAPI authentication against a DSE cluster secured with Kerberos:

```
dse-java-driver {
  advanced.auth-provider {
      class = DseGssApiAuthProvider
      login-configuration {
          principal = "user principal here ex cassandra@DATASTAX.COM"
          useKeyTab = "true"
          refreshKrb5Config = "true"
          keyTab = "Path to keytab file here"
      }
   }
 }
```

See the comments in [reference.conf] for more details.

#### Custom

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

For convenience, there are shortcuts that take the credentials directly. This is equivalent to
using `PlainTextAuthProvider` in the configuration:

```java
CqlSession session =
    CqlSession.builder()
        .withAuthCredentials("user", "pass")
        .build();

// With proxy authentication (DSE only)
CqlSession session =
    CqlSession.builder()
        .withAuthCredentials("user", "pass", "otherUserOrRole")
        .build();
```

One downside of `withAuthCredentials` is that the credentials are stored in clear text in memory;
this means they are vulnerable to an attacker who is able to perform memory dumps. If this is not
acceptable for you, consider writing your own [AuthProvider] implementation;
[PlainTextAuthProviderBase] is a good starting point.

Similarly, the driver provides [DseGssApiAuthProviderBase] as a starting point to write your own
GSSAPI auth provider. 

[SASL]: https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer

[AuthProvider]: https://docs.datastax.com/en/drivers/java/4.3/com/datastax/oss/driver/api/core/auth/AuthProvider.html
[DriverContext]: https://docs.datastax.com/en/drivers/java/4.3/com/datastax/oss/driver/api/core/context/DriverContext.html
[PlainTextAuthProviderBase]: https://docs.datastax.com/en/drivers/java/4.3/com/datastax/oss/driver/api/core/auth/PlainTextAuthProviderBase.html
[DseGssApiAuthProviderBase]: https://docs.datastax.com/en/drivers/java/4.3/com/datastax/dse/driver/api/core/auth/DseGssApiAuthProviderBase.html
[SessionBuilder.withAuthCredentials]: https://docs.datastax.com/en/drivers/java/4.3/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withAuthCredentials-java.lang.String-java.lang.String-
[SessionBuilder.withAuthProvider]: https://docs.datastax.com/en/drivers/java/4.3/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withAuthProvider-com.datastax.oss.driver.api.core.auth.AuthProvider-
[reference.conf]: ../configuration/reference/