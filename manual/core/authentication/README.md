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
datastax-java-driver {
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

The driver also offers a simple, built-in plain text authentication provider:
[ProgrammaticPlainTextAuthProvider]. The following is equivalent to using `PlainTextAuthProvider` in
the configuration:

```java
AuthProvider authProvider = new ProgrammaticPlainTextAuthProvider("user", "pass");

CqlSession session =
    CqlSession.builder()
        .withAuthProvider(authProvider)
        .build();
```

For convenience, there are shortcuts that take the credentials directly:

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

One downside of the driver's built-in authentication providers is that the credentials are stored in
clear text in memory; this means they are vulnerable to an attacker who is able to perform memory
dumps. If this is not acceptable for you, consider writing your own [AuthProvider] implementation;
[PlainTextAuthProviderBase] is a good starting point.

Similarly, [ProgrammaticDseGssApiAuthProvider] lets you configure GSSAPI programmatically:

```java
import com.datastax.dse.driver.api.core.auth.DseGssApiAuthProviderBase.GssApiOptions;

javax.security.auth.Subject subject = ...; // do your Kerberos configuration here

GssApiOptions options = GssApiOptions.builder().withSubject(subject).build();
CqlSession session = CqlSession.builder()
    .withAuthProvider(new ProgrammaticDseGssApiAuthProvider(options))
    .build();
```

For more complex needs (e.g. if building the options once and reusing them doesn't work for you),
you can subclass [DseGssApiAuthProviderBase].

### Proxy authentication

DSE allows a user to connect as another user or role:

```
-- Allow bob to connect as alice:
GRANT PROXY.LOGIN ON ROLE 'alice' TO 'bob'
```

Once connected, all authorization checks will be performed against the proxy role (alice in this
example).

To use proxy authentication with the driver, you need to provide the **authorization-id**, in other
words the name of the role you want to connect as.

Example for plain text authentication:

```
datastax-java-driver {
  advanced.auth-provider {
      class = PlainTextAuthProvider
      username = bob
      password = bob's password
      authorization-id = alice
   }
 }
```

With the GSSAPI (Kerberos) provider:

```
datastax-java-driver {
  advanced.auth-provider {
      class = DseGssApiAuthProvider
      authorization-id = alice
      login-configuration {
          principal = "user principal here ex bob@DATASTAX.COM"
          useKeyTab = "true"
          refreshKrb5Config = "true"
          keyTab = "Path to keytab file here"
      }
   }
 }
```

### Proxy execution

Proxy execution is similar to proxy authentication, but it applies to a single query, not the whole
session.

```
-- Allow bob to execute queries as alice:
GRANT PROXY.EXECUTE ON ROLE 'alice' TO 'bob'
```

For this scenario, you would **not** add the `authorization-id = alice` to your configuration.
Instead, use [ProxyAuthentication.executeAs] to wrap your query with the correct authorization for
the execution:

```java
import com.datastax.dse.driver.api.core.auth.ProxyAuthentication;

SimpleStatement statement = SimpleStatement.newInstance("some query");
// executeAs returns a new instance, you need to re-assign
statement = ProxyAuthentication.executeAs("alice", statement);
session.execute(statement);
``` 

[SASL]: https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer

[AuthProvider]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/auth/AuthProvider.html
[DriverContext]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/context/DriverContext.html
[PlainTextAuthProviderBase]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/auth/PlainTextAuthProviderBase.html
[ProgrammaticPlainTextAuthProvider]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/auth/ProgrammaticPlainTextAuthProvider.html
[DseGssApiAuthProviderBase]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/auth/DseGssApiAuthProviderBase.html
[ProgrammaticDseGssApiAuthProvider]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/auth/ProgrammaticDseGssApiAuthProvider.html
[ProxyAuthentication.executeAs]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/auth/ProxyAuthentication.html#executeAs-java.lang.String-StatementT-
[SessionBuilder.withAuthCredentials]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withAuthCredentials-java.lang.String-java.lang.String-
[SessionBuilder.withAuthProvider]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withAuthProvider-com.datastax.oss.driver.api.core.auth.AuthProvider-
[reference.conf]: ../configuration/reference/
