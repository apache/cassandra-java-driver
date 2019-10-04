## SSL

### Quick overview

Secure the traffic between the driver and Cassandra.

* `advanced.ssl-engine-factory` in the configuration; defaults to none, also available: JSSE, or
  write your own.
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

There is also a convenience shortcut if you just want to use an existing `javax.net.ssl.SSLContext`:

```java
SSLContext sslContext = ...
CqlSession session = CqlSession.builder()
    .withSslContext(sslContext)
    .build();
```

#### Netty

Netty provides a more efficient SSL implementation based on native OpenSSL support. It's possible to
customize the driver to use it instead of JSSE.

This is an advanced topic and beyond the scope of this document, but here is an overview:

1. add a dependency to Netty-tcnative: follow
   [these instructions](http://netty.io/wiki/forked-tomcat-native.html);
2. write your own implementation of the driver's `SslHandlerFactory`. This is a higher-level
   abstraction than `SslEngineFactory`, that returns a Netty `SslHandler`. You'll build this handler
   with Netty's own `SslContext`;
3. write a subclass of `DefaultDriverContext` that overrides `buildSslHandlerFactory()` to return
   the custom `SslHandlerFactory` you wrote in step 2. This will cause the driver to completely
   ignore the `ssl-engine-factory` options in the configuration;
4. write a subclass of `SessionBuilder` that overrides `buildContext` to return the custom context
   that you wrote in step 3.
5. build your session with your custom builder.

Note that this approach relies on the driver's [internal API](../../api_conventions).

[dsClientToNode]: https://docs.datastax.com/en/cassandra/3.0/cassandra/configuration/secureSSLClientToNode.html
[pickle]: http://thelastpickle.com/blog/2015/09/30/hardening-cassandra-step-by-step-part-1-server-to-server.html
[JSSE system properties]: http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#Customization
[SessionBuilder.withSslEngineFactory]: https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withSslEngineFactory-com.datastax.oss.driver.api.core.ssl.SslEngineFactory-
[SessionBuilder.withSslContext]: https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withSslContext-javax.net.ssl.SSLContext-
