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
  <version>3.1.3</version>
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
  <version>3.1.3</version>
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
  <version>3.1.3</version>
  <exclusions>
    <exclusion>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-core</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

### Limitations

When using the shaded jar, it is not possible to configure the Netty layer.

Extending [NettyOptions] is not an option because `NettyOptions` API
exposes Netty classes, and as such it should only be extended
by clients using the non-shaded version of driver.
Attempting to extend `NettyOptions` with shaded Netty classes is not supported,
and in particular for OSGi applications,
it is likely that such a configuration would lead to compile and/or runtime errors.

Also, with shaded Netty classes, it is not possible to benefit
from [Netty native transports].

It is therefore normal to see the following log line when the driver starts and
detects that shaded Netty classes are being used:

    Detected shaded Netty classes in the classpath; native epoll transport will not work properly, defaulting to NIO.


[NettyOptions]:http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/NettyOptions.html
[Netty native transports]:http://netty.io/wiki/native-transports.html
