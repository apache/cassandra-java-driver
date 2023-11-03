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

[AuthProvider]:          https://docs.datastax.com/en/drivers/java/4.1/com/datastax/oss/driver/api/core/auth/AuthProvider.html
[DriverContext]:         https://docs.datastax.com/en/drivers/java/4.1/com/datastax/oss/driver/api/core/context/DriverContext.html
[PlainTextAuthProvider]: https://docs.datastax.com/en/drivers/java/4.1/com/datastax/oss/driver/internal/core/auth/PlainTextAuthProvider.html
