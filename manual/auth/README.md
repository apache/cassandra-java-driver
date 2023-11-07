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

[Cluster.Builder.withCredentials]:  https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/Cluster.Builder.html#withCredentials-java.lang.String-java.lang.String-
[AuthProvider]:                     https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/AuthProvider.html
[Cluster.Builder.withAuthProvider]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/Cluster.Builder.html#withAuthProvider-com.datastax.driver.core.AuthProvider-
[PlainTextAuthProvider]:            https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/PlainTextAuthProvider.html
