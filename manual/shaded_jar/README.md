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
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>2.1.10.3</version>
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
  <version>2.1.10.3</version>
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
  <version>2.1.10.3</version>
  <exclusions>
    <exclusion>
      <groupId>com.datastax.cassandra</groupId>
      <artifactId>cassandra-driver-core</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```
