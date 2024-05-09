# How to create cert stores for ReloadingKeyManagerFactoryTest

Need the following cert stores:
- `server.keystore`
- `client-original.keystore`
- `client-alternate.keystore`
- `server.truststore`: trusts `client-original.keystore` and `client-alternate.keystore`
- `client.truststore`: trusts `server.keystore`

We shouldn't need any signing requests or chains of trust, since truststores are just including certs directly.

First create the three keystores:
```
$ keytool -genkeypair -keyalg RSA -alias server -keystore server.keystore -dname "CN=server" -storepass changeit -keypass changeit
$ keytool -genkeypair -keyalg RSA -alias client-original -keystore client-original.keystore -dname "CN=client-original" -storepass changeit -keypass changeit
$ keytool -genkeypair -keyalg RSA -alias client-alternate -keystore client-alternate.keystore -dname "CN=client-alternate" -storepass changeit -keypass changeit
```

Note that we need to use `-keyalg RSA` because keytool's default keyalg is DSA, which TLS 1.3 doesn't support. If DSA is
used, the handshake will fail due to the server not being able to find any authentication schemes compatible with its
x509 certificate ("Unavailable authentication scheme").

Then export all the certs:
```
$ keytool -exportcert -keystore server.keystore -alias server -file server.cert -storepass changeit
$ keytool -exportcert -keystore client-original.keystore -alias client-original -file client-original.cert -storepass changeit
$ keytool -exportcert -keystore client-alternate.keystore -alias client-alternate -file client-alternate.cert -storepass changeit
```

Then create the server.truststore that trusts the two client certs:
```
$ keytool -import -file client-original.cert -alias client-original -keystore server.truststore -storepass changeit
$ keytool -import -file client-alternate.cert -alias client-alternate -keystore server.truststore -storepass changeit
```

Then create the client.truststore that trusts the server cert:
```
$ keytool -import -file server.cert -alias server -keystore client.truststore -storepass changeit
```
