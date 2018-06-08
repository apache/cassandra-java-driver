## Using the shaded JAR

The default driver JAR depends on [Netty](http://netty.io/), which is
used internally for networking.

This explicit dependency can be a problem if your application already
uses another Netty version. To avoid conflicts, we provide a "shaded"
version of the JAR, which bundles the Netty classes under a different
package name:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core-shaded</artifactId>
  <version>4.0.0-alpha3</version>
</dependency>
```

If you also use the query-builder or some other library that depends on java-driver-core, you need to remove its
dependency to the non-shaded JAR:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core-shaded</artifactId>
  <version>4.0.0-alpha3</version>
</dependency>
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-query-builder</artifactId>
  <version>4.0.0-alpha3</version>
  <exclusions>
    <exclusion>
      <groupId>com.datastax.oss</groupId>
      <artifactId>java-driver-core</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```
