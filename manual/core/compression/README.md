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
[Snappy](http://google.github.io/snappy/). The LZ4 implementation is a good first choice; it offers
fallback implementations in case native libraries fail to load and
[benchmarks](http://java-performance.info/performance-general-compression/) suggest that it offers
better performance and compression ratios over Snappy.

Both implementations rely on third-party libraries, declared by the driver as *optional*
dependencies; if you enable compression, you need to explicitly depend on the corresponding library
to pull it into your project. 

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

[parent POM]: https://search.maven.org/#artifactdetails%7Ccom.datastax.oss%7Cjava-driver-parent%7C4.1.0%7Cpom
