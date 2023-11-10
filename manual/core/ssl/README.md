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

## SSL

### Quick overview

Secure the traffic between the driver and Cassandra.

* `advanced.ssl-engine-factory` in the configuration; defaults to none, also available:
  config-based, or write your own.
* or programmatically:
  [CqlSession.builder().withSslEngineFactory()][SessionBuilder.withSslEngineFactory] or
  [CqlSession.builder().withSslContext()][SessionBuilder.withSslContext].

-----

There are two aspects to SSL:

* **client-to-node encryption**, where the traffic is encrypted, and the client verifies the
  identity of the Cassandra nodes it connects to;
* optionally, **client certificate authentication**, where Cassandra nodes also verify the identity
  of the client.

This section describes the driver-side configuration; it assumes that you've already configured SSL
in Cassandra:

* [the Cassandra documentation][dsClientToNode] covers a basic approach with self-signed
  certificates, which is fine for development and tests.
* [this blog post][pickle] details a more advanced solution based on a Certificate Authority (CA).

### Preparing the certificates

#### Client truststore

This is required for client-to-node encryption.

If you're using self-signed certificates, you need to export the public part of each node's
certificate from that node's keystore:

```
keytool -export -alias cassandra -file cassandranode0.cer -keystore .keystore
```

Then add all public certificates to the client truststore:

```
keytool -import -v -trustcacerts -alias <cassandra_node0> -file cassandranode0.cer -keystore client.truststore
keytool -import -v -trustcacerts -alias <cassandra_node1> -file cassandranode1.cer -keystore client.truststore
...
```

If you're using a Certificate Authority, the client truststore only needs to contain the CA's
certificate:

```
keytool -import -v -trustcacerts -alias CARoot -file ca.cer -keystore client.truststore
```

#### Client keystore

If you also intend to use client certificate authentication, generate the public and private key
pair for the client:

```
keytool -genkey -keyalg RSA -alias client -keystore client.keystore
```

If you're using self-signed certificates, extract the public part of the client certificate, and
import it in the truststore of each Cassandra node:

```
keytool -export -alias client -file client.cer -keystore client.keystore
keytool -import -v -trustcacerts -alias client -file client.cer -keystore server.truststore
```

If you're using a CA, sign the client certificate with it (see the blog post linked at the top of
this page). Then the nodes' truststores only need to contain the CA's certificate (which should
already be the case if you've followed the steps for inter-node encryption).


### Driver configuration

By default, the driver's SSL support is based on the JDK's built-in implementation: JSSE (Java
Secure Socket Extension),.

To enable it, you need to define an engine factory in the [configuration](../configuration/).

#### JSSE, property-based

```
datastax-java-driver {
  advanced.ssl-engine-factory {
    class = DefaultSslEngineFactory
    
    # This property is optional. If it is not present, the driver won't explicitly enable cipher
    # suites on the engine, which according to the JDK documentations results in "a minimum quality
    # of service".
    // cipher-suites = [ "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" ]

    # Whether or not to require validation that the hostname of the server certificate's common
    # name matches the hostname of the server being connected to. If not set, defaults to true.
    // hostname-validation = true

    # The locations and passwords used to access truststore and keystore contents.
    # These properties are optional. If either truststore-path or keystore-path are specified,
    # the driver builds an SSLContext from these files.  If neither option is specified, the
    # default SSLContext is used, which is based on system property configuration.
    // truststore-path = /path/to/client.truststore
    // truststore-password = password123
    // keystore-path = /path/to/client.keystore
    // keystore-password = password123
  }
}
```

Alternatively to storing keystore and truststore information in your configuration, you can instead
use [JSSE system properties]:

```
-Djavax.net.ssl.trustStore=/path/to/client.truststore
-Djavax.net.ssl.trustStorePassword=password123
# If you're using client authentication:
-Djavax.net.ssl.keyStore=/path/to/client.keystore
-Djavax.net.ssl.keyStorePassword=password123
```

#### JSSE, custom factory

If you need more control than what system properties allow, you need to write your own engine
factory. If you just need specific configuration on the `SSLEngine`, you can extend the default
factory and override `newSslEngine`. For example, here is how you would configure custom
`AlgorithmConstraints`:

```java
public class CustomSslEngineFactory extends DefaultSslEngineFactory {

  public CustomSslEngineFactory(DriverContext context) {
    super(context);
  }

  @Override
  public SSLEngine newSslEngine(SocketAddress remoteEndpoint) {
    SSLEngine engine = super.newSslEngine(remoteEndpoint);
    SSLParameters parameters = engine.getSSLParameters();
    parameters.setAlgorithmConstraints(...);
    engine.setSSLParameters(parameters);
    return engine;
  }
}
```

Then declare your custom implementation in the configuration:

```
datastax-java-driver {
  advanced.ssl-engine-factory {
    class = com.mycompany.CustomSslEngineFactory
  }
}
```

#### JSSE, programmatic

You can also provide a factory instance programmatically. This will take precedence over the
configuration:

```java
SslEngineFactory yourFactory = ...
CqlSession session = CqlSession.builder()
    .withSslEngineFactory(yourFactory)
    .build();
```

If you are reusing code that configures SSL programmatically, you can use
[ProgrammaticSslEngineFactory] as an easy way to wrap that into a factory instance:

```java
SSLContext sslContext = ...
String[] cipherSuites = ...
boolean requireHostNameValidation = ...
CqlSession session =
    CqlSession.builder()
        .withSslEngineFactory(
            new ProgrammaticSslEngineFactory(
                sslContext, cipherSuites, requireHostNameValidation))
        .build();
```

Finally, there is a convenient shortcut on the session builder if you just need to pass an
`SSLContext`:

```java
SSLContext sslContext = ...
CqlSession session = CqlSession.builder()
    .withSslContext(sslContext)
    .build();
```

#### Netty-tcnative

Netty supports native integration with OpenSSL / boringssl. The driver does not provide this out of
the box, but with a bit of custom development it is fairly easy to add. See
[SslHandlerFactory](../../developer/netty_pipeline/#ssl-handler-factory) in the developer docs.


[dsClientToNode]: https://docs.datastax.com/en/cassandra/3.0/cassandra/configuration/secureSSLClientToNode.html
[pickle]: http://thelastpickle.com/blog/2015/09/30/hardening-cassandra-step-by-step-part-1-server-to-server.html
[JSSE system properties]: http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#Customization
[SessionBuilder.withSslEngineFactory]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withSslEngineFactory-com.datastax.oss.driver.api.core.ssl.SslEngineFactory-
[SessionBuilder.withSslContext]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withSslContext-javax.net.ssl.SSLContext-
[ProgrammaticSslEngineFactory]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/ssl/ProgrammaticSslEngineFactory.html
