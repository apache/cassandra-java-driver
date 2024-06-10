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

The default `java-driver-core` JAR depends on a number of [third party
libraries](../integration/#driver-dependencies). This can create conflicts if your application
already uses other versions of those same dependencies.

To avoid this, we provide an alternative core artifact that shades [Netty](../integration/#netty),
[Jackson](../integration/#jackson) and [ESRI](../integration/#esri). To use it, replace the
dependency to `java-driver-core` by:

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>java-driver-core-shaded</artifactId>
  <version>${driver.version}</version>
</dependency>
```

If you also use the query-builder, mapper or some other library that depends on java-driver-core,
you need to remove its dependency to the non-shaded JAR:

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>java-driver-core-shaded</artifactId>
  <version>${driver.version}</version>
</dependency>

<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>java-driver-query-builder</artifactId>
  <version>${driver.version}</version>
  <exclusions>
    <exclusion>
      <groupId>com.scylladb</groupId>
      <artifactId>java-driver-core</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

Notes:

* the shading process works by moving the libraries under a different package name, and bundling
  them directly into the driver JAR. This should be transparent for client applications: the
  impacted dependencies are purely internal, their types are not surfaced in the driver's public
  API.
* the driver is compatible with all Netty versions in the range `[4.1.7, 4.2.0)` (equal to or higher
  than 4.1.7, and lesser than 4.2.0). If you just need a specific version in that range, you can 
  avoid the need for the shaded JAR by declaring an explicit dependency in your POM:
  
    ```xml
    <dependency>
      <groupId>com.scylladb</groupId>
      <artifactId>java-driver-core</artifactId>
      <version>${driver.version}</version>
    </dependency>
  
    <dependency>
      <groupId>io.netty</groupId>
      <artifactId>netty-handler</artifactId>
      <version>4.1.39.Final</version>
    </dependency>
    ```
  
    This only works with Netty: for Jackson and ESRI, only the exact version declared in the driver POM
    is supported.
