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

## GraalVM native images

### Quick overview

* [GraalVM native images](https://www.graalvm.org/reference-manual/native-image/) can be built with 
  no additional configuration starting with driver 4.13.0.
* But extra configurations are required in a few cases:
  * When using [reactive programming](../reactive);
  * When using [Jackson](../integration#Jackson);
  * When using LZ4 [compression](../compression/);
  * Depending on the [logging backend](../logging) in use.
* DSE-specific features:
  * [Geospatial types](../dse/geotypes) are supported.
  * [DSE Graph](../dse/graph) is not officially supported, although it may work.
* The [shaded jar](../shaded_jar) is not officially supported, although it may work.

-----

### Concepts

Starting with version 4.13.0, the driver ships with [embedded GraalVM configuration files] that 
allow GraalVM native images including the driver to be built without hassle, barring a few 
exceptions and caveats listed below.

[embedded GraalVM configuration files]:https://www.graalvm.org/reference-manual/native-image/BuildConfiguration/#embedding-a-configuration-file

### Classes instantiated by reflection

The driver instantiates its components by reflection. The actual classes that will be instantiated 
in this way need to be registered for reflection. All built-in implementations of various driver 
components, such as `LoadBalancingPolicy` or `TimestampGenerator`, are automatically registered for 
reflection, along with a few other internal components tha are also instantiated by reflection.
_You don't need to manually register any of these built-in implementations_.

But if you intend to use a custom implementation in lieu of a driver built-in class, then it is your 
responsibility to register that custom implementation for reflection.

For example, assuming that you have the following load balancing policy implementation:

```java

package com.example.app;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class CustomLoadBalancingPolicy extends DefaultLoadBalancingPolicy {

  public CustomLoadBalancingPolicy(DriverContext context, String profileName) {
    super(context, profileName);
  }
  // rest of class omitted for brevity
}
```

And assuming that you declared the above class in your application.conf file as follows:

```hocon
datastax-java-driver.basic{
  load-balancing-policy.class = com.example.app.CustomLoadBalancingPolicy
}
```

Then you will have to register that class for reflection:

1. Create the following reflection.json file, or add the entry to an existing file:

```json
[
  { "name": "com.example.app.CustomLoadBalancingPolicy", "allPublicConstructors": true }
]
```

2. When invoking the native image builder, add a `-H:ReflectionConfigurationFiles=reflection.json`
   flag and point it to the file created above.

Note: some frameworks allow you to simplify the registration process. For example, Quarkus offers
the `io.quarkus.runtime.annotations.RegisterForReflection` annotation that you can use to annotate
your class:

```java
@RegisterForReflection
public class CustomLoadBalancingPolicy extends DefaultLoadBalancingPolicy {
  //...
}
```

In this case, no other manual configuration is required for the above class to be correctly 
registered for reflection.

### Configuration resources 

The default driver [configuration](../configuration) mechanism is based on the TypeSafe Config
library. TypeSafe Config looks for a few classpath resources when initializing the configuration: 
`reference.conf`, `application.conf`, `application.json`, `application.properties`. _These classpath 
resources are all automatically included in the native image: you should not need to do it 
manually_. See [Accessing Resources in Native Images] for more information on how classpath 
resources are handled in native images.

[Accessing Resources in Native Images]: https://www.graalvm.org/reference-manual/native-image/Resources/

### Configuring the logging backend

When configuring [logging](../logging), the choice of a backend must be considered carefully, as 
most logging backends resort to reflection during their configuration phase. 

By default, GraalVM native images provide support for the java.util.logging (JUL) backend. See 
[this page](https://www.graalvm.org/reference-manual/native-image/Logging/) for more information.

For other logging backends, please refer to the logging library documentation to find out if GraalVM 
native images are supported.

### Using reactive-style programming

The [reactive execution model](../reactive) is compatible with GraalVM native images, but the
following configurations must be added:

1. Create the following reflection.json file, or add the entry to an existing file:

```json
[
  { "name": "org.reactivestreams.Publisher" }
]
```

2. When invoking the native image builder, add a `-H:ReflectionConfigurationFiles=reflection.json`
   flag and point it to the file created above.

### Using the Jackson JSON library

[Jackson](https://github.com/FasterXML/jackson) is used in [a few places](../integration#jackson) in 
the driver, but is an optional dependency; if you intend to use Jackson, the following 
configurations must be added:

1. Create the following reflection.json file, or add these entries to an existing file:

```json
[
  { "name": "com.fasterxml.jackson.core.JsonParser" },
  { "name": "com.fasterxml.jackson.databind.ObjectMapper" }
]
```

**Important**: when using the shaded jar – which is not officially supported on GraalVM native 
images, see below for more details – replace the above entries with the below ones:

```json
[
  { "name": "com.datastax.oss.driver.shaded.fasterxml.jackson.core.JsonParser" },
  { "name": "com.datastax.oss.driver.shaded.fasterxml.jackson.databind.ObjectMapper" }
]
```
2. When invoking the native image builder, add a `-H:ReflectionConfigurationFiles=reflection.json`
   flag and point it to the file created above.

### Enabling compression

When using [compression](../compression/), only LZ4 can be enabled in native images. **Snappy
compression is not supported.**

In order for LZ4 compression to work in a native image, the following additional GraalVM
configuration is required:

1. Create the following reflection.json file, or add these entries to an existing file:

```json
[
  { "name" : "net.jpountz.lz4.LZ4Compressor" },
  {
    "name" : "net.jpountz.lz4.LZ4JNICompressor",
    "allDeclaredConstructors": true,
    "allPublicFields": true
  },
  {
    "name" : "net.jpountz.lz4.LZ4JavaSafeCompressor",
    "allDeclaredConstructors": true,
    "allPublicFields": true
  },
  {
    "name" : "net.jpountz.lz4.LZ4JavaUnsafeCompressor",
    "allDeclaredConstructors": true,
    "allPublicFields": true
  },
  {
    "name" : "net.jpountz.lz4.LZ4HCJavaSafeCompressor",
    "allDeclaredConstructors": true,
    "allPublicFields": true
  },
  {
    "name" : "net.jpountz.lz4.LZ4HCJavaUnsafeCompressor",
    "allDeclaredConstructors": true,
    "allPublicFields": true
  },
  {
    "name" : "net.jpountz.lz4.LZ4JavaSafeSafeDecompressor",
    "allDeclaredConstructors": true,
    "allPublicFields": true
  },
  {
    "name" : "net.jpountz.lz4.LZ4JavaSafeFastDecompressor",
    "allDeclaredConstructors": true,
    "allPublicFields": true
  },
  {
    "name" : "net.jpountz.lz4.LZ4JavaUnsafeSafeDecompressor",
    "allDeclaredConstructors": true,
    "allPublicFields": true
  },
  {
    "name" : "net.jpountz.lz4.LZ4JavaUnsafeFastDecompressor",
    "allDeclaredConstructors": true,
    "allPublicFields": true
  }
]
```

2. When invoking the native image builder, add a `-H:ReflectionConfigurationFiles=reflection.json`
   flag and point it to the file created above.

### Native calls

The driver performs a few [native calls](../integration#native-libraries) using 
[JNR](https://github.com/jnr).

Starting with driver 4.7.0, native calls are also possible in a GraalVM native image, without any
extra configuration.

### Using DataStax Enterprise (DSE) features

#### DSE Geospatial types

DSE [Geospatial types](../dse/geotypes) are supported on GraalVM native images; the following
configurations must be added:

1. Create the following reflection.json file, or add the entry to an existing file:

```json
[
  { "name": "com.esri.core.geometry.ogc.OGCGeometry" }
]
```

**Important**: when using the shaded jar – which is not officially supported on GraalVM native 
images, as stated above – replace the above entry with the below one:

```json
[
  { "name": "com.datastax.oss.driver.shaded.esri.core.geometry.ogc.OGCGeometry" }
]
```

2. When invoking the native image builder, add a `-H:ReflectionConfigurationFiles=reflection.json`
   flag and point it to the file created above.

#### DSE Graph

**[DSE Graph](../dse/graph) is not officially supported on GraalVM native images.**

The following configuration can be used as a starting point for users wishing to build a native
image for a DSE Graph application. DataStax does not guarantee however that the below configuration
will work in all cases. If the native image build fails, a good option is to use GraalVM's
[Tracing Agent](https://www.graalvm.org/reference-manual/native-image/Agent/) to understand why.

1. Create the following reflection.json file, or add these entries to an existing file:

```json
[
  { "name": "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0" },
  { "name": "org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal" },
  { "name": "org.apache.tinkerpop.gremlin.structure.Graph",
    "allDeclaredConstructors": true,
    "allPublicConstructors": true,
    "allDeclaredMethods": true,
    "allPublicMethods": true
  },
  { "name": "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph",
    "allDeclaredConstructors": true,
    "allPublicConstructors": true,
    "allDeclaredMethods": true,
    "allPublicMethods": true
  },
  { "name": " org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph",
    "allDeclaredConstructors": true,
    "allPublicConstructors": true,
    "allDeclaredMethods": true,
    "allPublicMethods": true
  },
  { "name": "org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource",
    "allDeclaredConstructors": true,
    "allPublicConstructors": true,
    "allDeclaredMethods": true,
    "allPublicMethods": true
  }
]
```

2. When invoking the native image builder, add the following flags:

```
-H:ReflectionConfigurationFiles=reflection.json
--initialize-at-build-time=org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0
--initialize-at-build-time=org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer
```

### Using the shaded jar

**The [shaded jar](../shaded_jar) is not officially supported in a GraalVM native image.**

However, it has been reported that the shaded jar can be included in a GraalVM native image as a
drop-in replacement for the regular driver jar for simple applications, without any extra GraalVM
configuration.
