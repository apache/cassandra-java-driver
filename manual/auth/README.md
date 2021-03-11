## Authentication

Cassandra’s binary protocol supports [SASL]-based authentication.  To enable it, use
[Cluster.Builder.withCredentials] when building your `Cluster` instance to provide the credentials
you wish to authenticate with:

```java
Cluster.builder()
  .withCredentials("bob", "mypassword")
  .build();
```

This is a shortcut for using [PlainTextAuthProvider] for simple username/password authentication
(intended to work with the server-side `PasswordAuthenticator`).  This may alternatively be
provided using the [Cluster.Builder.withAuthProvider] method:


```java
Cluster.builder()
  .withAuthProvider(new PlainTextAuthProvider("bob", "mypassword"))
  .build();
```

Authentication must be configured before opening a session, it cannot be changed at runtime.

You can also write your own provider; it must implement [AuthProvider].


[SASL]: https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer

[Cluster.Builder.withCredentials]:  https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/Cluster.Builder.html#withCredentials-java.lang.String-java.lang.String-
[AuthProvider]:                     https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/AuthProvider.html
[Cluster.Builder.withAuthProvider]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/Cluster.Builder.html#withAuthProvider-com.datastax.driver.core.AuthProvider-
[PlainTextAuthProvider]:            https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/PlainTextAuthProvider.html
