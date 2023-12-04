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

The driver is compatible with all Netty versions in the range [4.1.7, 4.2.0),
that is, it can work with any version equal to or higher than 4.1.7, and
lesser than 4.2.0.

This explicit dependency can be a problem if your application already
uses another Netty version. To avoid conflicts, we provide a "shaded"
version of the JAR, which bundles the Netty classes under a different
package name:

```xml
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>java-driver-core-shaded</artifactId>
  <version>${driver.version}</version>
</dependency>
```

If you also use the query-builder or some other library that depends on java-driver-core, you need to remove its
dependency to the non-shaded JAR:

```xml
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>java-driver-core-shaded</artifactId>
  <version>${driver.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>java-driver-query-builder</artifactId>
  <version>${driver.version}</version>
  <exclusions>
    <exclusion>
      <groupId>org.apache.cassandra</groupId>
      <artifactId>java-driver-core</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```