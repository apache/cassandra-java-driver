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

## Using the shaded JAR

The default driver JAR depends on [Netty](http://netty.io/), which is
used internally for networking.

This explicit dependency can be a problem if your application already
uses another Netty version. To avoid conflicts, we provide a "shaded"
version of the JAR, which bundles the Netty classes under a different
package name:

```xml
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>3.12.0</version>
  <classifier>shaded</classifier>
  <!-- Because the shaded JAR uses the original POM, you still need
       to exclude this dependency explicitly: -->
  <exclusions>
    <exclusion>
      <groupId>io.netty</groupId>
      <artifactId>*</artifactId>
    </exclusion>
    <exclusion>
      <groupId>io.dropwizard.metrics</groupId>
      <artifactId>metrics-core</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

If you also use the mapper, you need to remove its dependency to the
non-shaded JAR:

```xml
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>3.12.0</version>
  <classifier>shaded</classifier>
  <exclusions>
    <exclusion>
      <groupId>io.netty</groupId>
      <artifactId>*</artifactId>
    </exclusion>
    <exclusion>
      <groupId>io.dropwizard.metrics</groupId>
      <artifactId>metrics-core</artifactId>
    </exclusion>
  </exclusions>
</dependency>
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>cassandra-driver-mapping</artifactId>
  <version>3.12.0</version>
  <exclusions>
    <exclusion>
      <groupId>org.apache.cassandra</groupId>
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


[NettyOptions]:https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/NettyOptions.html
[Netty native transports]:http://netty.io/wiki/native-transports.html
