## Compression

Cassandra's binary protocol supports optional compression of requests and responses. This reduces
network traffic at the cost of a slight CPU overhead, therefore it will likely be beneficial when
you have larger payloads, such as:

* requests with many values, or very large values; 
* responses with many rows, or many columns per row, or very large columns.

To enable compression, set the following option in the [configuration](../configuration):

```
datastax-java-driver {
  advanced.protocol.compression = lz4 // or snappy
}
```

Compression must be set before opening a session, it cannot be changed at runtime.


Two algorithms are supported out of the box: [LZ4](https://github.com/jpountz/lz4-java) and
[Snappy](http://google.github.io/snappy/). Both rely on third-party libraries, declared by the
driver as *optional* dependencies; if you enable compression, you need to explicitly depend on the
corresponding library to pull it into your project. 

### LZ4

Dependency:

```xml
<dependency>
  <groupId>org.lz4</groupId>
  <artifactId>lz4-java</artifactId>
  <version>1.4.1</version>
</dependency>
```

Always double-check the exact LZ4 version needed; you can find it in the driver's [parent POM].

LZ4-java has three internal implementations (from fastest to slowest):

* JNI;
* pure Java using `sun.misc.Unsafe`;
* pure Java using only "safe" classes.

It will pick the best implementation depending on what's possible on your platform. To find out
which one was chosen, [enable INFO logs](../logging/) on the category
`com.datastax.oss.driver.internal.core.protocol.Lz4Compressor` and look for the following message:

```
INFO  com.datastax.oss.driver.internal.core.protocol.Lz4Compressor  - Using LZ4Factory:JNI
```

### Snappy

Dependency:

```xml
<dependency>
  <groupId>org.xerial.snappy</groupId>
  <artifactId>snappy-java</artifactId>
  <version>1.1.2.6</version>
</dependency>
```

Always double-check the exact Snappy version needed; you can find it in the driver's [parent POM].

[parent POM]: https://search.maven.org/#artifactdetails%7Ccom.datastax.oss%7Cjava-driver-parent%7C4.0.0-alpha3%7Cpom