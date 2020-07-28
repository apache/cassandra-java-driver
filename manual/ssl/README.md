## SSL

You can secure traffic between the driver and Cassandra with SSL. There
are two aspects to that:

* **client-to-node encryption**, where the traffic is encrypted, and the
  client verifies the identity of the Cassandra nodes it connects to;
* optionally, **client certificate authentication**, where Cassandra
  nodes also verify the identity of the client.

This section describes the driver-side configuration; it assumes that
you've already configured SSL in Cassandra:

* [the Cassandra documentation](http://docs.datastax.com/en/cassandra/2.0/cassandra/security/secureSSLClientToNode_t.html)
  covers a basic approach with self-signed certificates, which is fine
  for development and tests.
* [this blog post](http://thelastpickle.com/blog/2015/09/30/hardening-cassandra-step-by-step-part-1-server-to-server.html)
  details a more advanced solution based on a Certificate Authority
  (CA).

### Preparing the certificates

#### Client truststore

This is required for client-to-node encryption.

If you're using self-signed certificates, you need to export the public
part of each node's certificate from that node's keystore:

```
keytool -export -alias cassandra -file cassandranode0.cer -keystore .keystore
```

Then add all public certificates to the client truststore:

```
keytool -import -v -trustcacerts -alias <cassandra_node0> -file cassandranode0.cer -keystore client.truststore
keytool -import -v -trustcacerts -alias <cassandra_node1> -file cassandranode1.cer -keystore client.truststore
...
```

If you're using a Certificate Authority, the client truststore only
needs to contain the CA's certificate:

```
keytool -import -v -trustcacerts -alias CARoot -file ca.cer -keystore client.truststore
```

#### Client keystore

If you also intend to use client certificate authentication, generate
the public and private key pair for the client:

```
keytool -genkey -keyalg RSA -alias client -keystore client.keystore
```

If you're using self-signed certificates, extract the public part of the
client certificate, and import it in the truststore of each Cassandra
node:

```
keytool -export -alias client -file client.cer -keystore client.keystore
keytool -import -v -trustcacerts -alias client -file client.cer -keystore server.truststore
```

If you're using a CA, sign the client certificate with it (see the blog
post linked at the top of this page). Then the nodes' truststores only
need to contain the CA's certificate (which should already be the case
if you've followed the steps for inter-node encryption).

### Driver configuration

The base class to configure SSL is [RemoteEndpointAwareSSLOptions]. It's very generic, but
you don't necessarily need to deal with it directly: the default
instance, or the provided subclasses, might be enough for your needs.

#### JSSE, Property-based

`withSSL()` gives you a basic JSSE configuration:

```java
Cluster cluster = Cluster.builder()
  .addContactPoint("127.0.0.1")
  .withSSL()
  .build();
```

You can then use
[JSSE system properties](http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#Customization)
for specific details, like keystore locations and passwords:

```
-Djavax.net.ssl.trustStore=/path/to/client.truststore
-Djavax.net.ssl.trustStorePassword=password123
# If you're using client authentication:
-Djavax.net.ssl.keyStore=/path/to/client.keystore
-Djavax.net.ssl.keyStorePassword=password123
```

#### JSSE, programmatic

If you need more control than what system properties allow, you can
configure SSL programmatically with [RemoteEndpointAwareJdkSSLOptions]:

```java
SSLContext sslContext = ... // create and configure SSL context

RemoteEndpointAwareJdkSSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder()
  .withSSLContext(context)
  .build();

Cluster cluster = Cluster.builder()
  .addContactPoint("127.0.0.1")
  .withSSL(sslOptions)
  .build();
```

Note that you can also extend the class and override
[newSSLEngine(SocketChannel,InetSocketAddress)][newSSLEngine] if you need specific
configuration on the `SSLEngine`.  For example, to enable hostname verification:

```java
SSLContext sslContext = ... // create and configure SSL context

RemoteEndpointAwareJdkSSLOptions sslOptions = new RemoteEndpointAwareJdkSSLOptions(sslContext, null) {
    protected SSLEngine newSSLEngine(SocketChannel channel, InetSocketAddress remoteEndpoint) {
        SSLEngine engine = super.newSSLEngine(channel, remoteEndpoint);
        SSLParameters parameters = engine.getSSLParameters();
        // HTTPS endpoint identification includes hostname verification against certificate's common name.
        // This API is only available for JDK7+.
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(parameters);
        return engine;
    }
};

Cluster cluster = Cluster.builder()
  .addContactPoint("127.0.0.1")
  .withSSL(sslOptions)
  .build();
```



#### Netty

[RemoteEndpointAwareNettySSLOptions] allows you to use Netty's `SslContext` instead of
the JDK directly. The advantage is that Netty can use OpenSSL directly,
which provides better performance and generates less garbage.  A disadvantage of
using the OpenSSL provider is that it requires platform-specific dependencies,
unlike the JDK provider.


##### Converting your client certificates for OpenSSL

OpenSSL doesn't use keystores, so if you use client authentication and
generated your certificates with keytool, you need to convert them.

* use this command to extract the public certificate chain:

    ```
    keytool -export -keystore client.keystore -alias client -rfc -file client.crt
    ```
* follow
  [this tutorial](http://www.herongyang.com/crypto/Migrating_Keys_keytool_to_OpenSSL_3.html)
  to extract your client's private key from `client.keystore` to a text
  file `client.key` in PEM format.

##### Updating your dependencies

Netty-tcnative provides the native integration with OpenSSL. Follow
[these instructions](http://netty.io/wiki/forked-tomcat-native.html) to
add it to your dependencies.

There are known runtime incompatibilities between newer versions of
netty-tcnative and the version of netty that the driver uses.  For best
results, use version 2.0.7.Final.

Using netty-tcnative requires JDK 1.7 or above and requires the presence of
OpenSSL on the system.  It will not fall back to the JDK implementation.

##### Configuring the context

Use the following Java code to configure OpenSSL with your certificates:

```java
KeyStore ks = KeyStore.getInstance("JKS");
// make sure you close this stream properly (not shown here for brevity)
InputStream trustStore = new FileInputStream("client.truststore");
ks.load(trustStore, "password123".toCharArray());
TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
tmf.init(ks);

SslContextBuilder builder = SslContextBuilder
  .forClient()
  .sslProvider(SslProvider.OPENSSL)
  .trustManager(tmf);
  // only if you use client authentication
  .keyManager(new File("client.crt"), new File("client.key"));

SSLOptions sslOptions = new RemoteEndpointAwareNettySSLOptions(builder.build());

Cluster cluster = Cluster.builder()
  .addContactPoint("127.0.0.1")
  .withSSL(sslOptions)
  .build();
```

[RemoteEndpointAwareSSLOptions]:      https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/RemoteEndpointAwareSSLOptions.html
[RemoteEndpointAwareJdkSSLOptions]:   https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/RemoteEndpointAwareJdkSSLOptions.html
[newSSLEngine]:                       https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/RemoteEndpointAwareJdkSSLOptions.html#newSSLEngine-io.netty.channel.socket.SocketChannel-java.net.InetSocketAddress-
[RemoteEndpointAwareNettySSLOptions]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/RemoteEndpointAwareNettySSLOptions.html
[NettyOptions]:                       https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/NettyOptions.html
