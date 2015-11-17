## Using the shaded JAR

The default driver JAR depends on [Netty](http://netty.io/), which is
used internally for networking.

This explicit dependency can be a problem if your application already
uses another Netty version. To avoid conflicts, we provide a "shaded"
version of the JAR, which bundles the Netty classes under a different
package name:

```xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>3.0.0-alpha5</version>
  <classifier>shaded</classifier>
  <!-- Because the shaded JAR uses the original POM, you still need
       to exclude this dependency explicitly: -->
  <exclusions>
    <exclusion>
      <groupId>io.netty</groupId>
      <artifactId>*</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

If you also use the mapper, you need to remove its dependency to the
non-shaded JAR:

```xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>3.0.0-alpha5</version>
  <classifier>shaded</classifier>
  <exclusions>
    <exclusion>
      <groupId>io.netty</groupId>
      <artifactId>*</artifactId>
    </exclusion>
  </exclusions>
</dependency>
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-mapping</artifactId>
  <version>3.0.0-alpha5</version>
  <exclusions>
    <exclusion>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-core</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```
