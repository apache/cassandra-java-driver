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

# OSGi

The driver is available as an [OSGi] bundle.  More specifically, the following maven artifacts are
valid OSGi bundles:

- `java-driver-core`
- `java-driver-query-builder`
- `java-driver-mapper-runtime`
- `java-driver-core-shaded`

Note: some of the driver dependencies are not valid OSGi bundles. Most of them are optional, and the
driver can work properly without them (see the
[Integration>Driver dependencies](../core/integration/#driver-dependencies) section for more
details); in such cases, the corresponding packages are declared with optional resolution in
`Import-Package` directives. However, if you need to access such packages in an OSGi container you
MUST wrap the corresponding jar in a valid OSGi bundle and make it available for provisioning to the
OSGi runtime.

## Using the shaded jar

`java-driver-core-shaded` shares the same bundle name as `java-driver-core`
(`com.datastax.oss.driver.core`).  It can be used as a drop-in replacement in cases where you have
an explicit version of dependency in your project different than that of the driver's.  Refer to
[shaded jar](../core/shaded_jar/) for more information.

## Using a custom `ClassLoader`

In several places of the [driver configuration] it is possible to specify the class name of
something to be instantiated by the driver such as the reconnection policy. This is accomplished
using reflection, which uses a `ClassLoader`.  By default, the driver uses its own bundle's 
`ClassLoader` to instantiate classes by reflection. This is typically adequate as long as the driver 
bundle has access to the bundle where the implementing class resides.

However if the default `ClassLoader` cannot load the implementing class, you may encounter an error
like this:

    java.lang.ClassNotFoundException: com.datastax.oss.MyCustomReconnectionPolicy
        
Similarly, it also happens that the default `ClassLoader` is able to load the implementing class but 
is not able to ascertain whether that class implements the expected parent type. In these cases you 
may encounter an error such as:

    java.lang.IllegalArgumentException: Expected class ExponentialReconnectionPolicy
    (specified by advanced.reconnection-policy.class) to be a subtype of
    com.datastax.oss.driver.api.core.connection.ReconnectionPolicy

This is occurring because there is a disparity in the `ClassLoader`s used between the driver code
and the `ClassLoader` used to reflectively load the class (in this case, 
`ExponentialReconnectionPolicy`).

To overcome these issues, you may specify a `ClassLoader` instance when constructing a `Session`
by using [withClassLoader()].

Alternatively, if you have access to the `BundleContext` (for example, if you are creating the 
session in an `Activator` class) you can also obtain the bundle's `ClassLoader` the following way:

```java
BundleContext bundleContext = ...;
Bundle bundle = bundleContext.getBundle();
BundleWiring bundleWiring = bundle.adapt(BundleWiring.class);
ClassLoader classLoader = bundleWiring.getClassLoader();
CqlSession session = CqlSession.builder()
    .withClassLoader(classLoader)
    .build();
```

### Using a custom `ClassLoader` for application-bundled configuration resources

In addition to specifying a `ClassLoader` when constructing a `Session`, you can also specify
a `ClassLoader` instance on certain `DriverConfigLoader` methods for cases when your OSGi
application bundle provides overrides to driver configuration defaults. This is typically done by
including an `application.conf` file in your application bundle.
 
For example, you can use [DriverConfigLoader.fromDefaults(ClassLoader)] to use the driver's default 
configuration mechanism while specifying a different class loader:

```java
BundleContext bundleContext = ...;
Bundle bundle = bundleContext.getBundle();
BundleWiring bundleWiring = bundle.adapt(BundleWiring.class);
ClassLoader classLoader = bundleWiring.getClassLoader();

CqlSession session = CqlSession.builder()
    .withClassLoader(classLoader)
    .withConfigLoader(DriverConfigLoader.fromDefaults(classLoader))
    .build();
```

The above configuration will look for resources named `application.conf` inside the application
bundle, using the right class loader for that.

Similarly, if you want to use programmatic configuration in you application bundle, but still
want to be able to provide some configuration in an `application.conf` file, you can use
[DriverConfigLoader.programmaticBuilder(ClassLoader)]:

```java
BundleContext bundleContext = ...;
Bundle bundle = bundleContext.getBundle();
BundleWiring bundleWiring = bundle.adapt(BundleWiring.class);
ClassLoader classLoader = bundleWiring.getClassLoader();
DriverConfigLoader loader =
    DriverConfigLoader.programmaticBuilder(classLoader)
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
        .startProfile("slow")
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
        .endProfile()
        .build();
CqlSession session = CqlSession.builder()
    .withClassLoader(classLoader)
    .withConfigLoader(loader)
    .build();
```

The above configuration will honor all programmatic settings, but will look for resources named 
`application.conf` inside the application bundle, using the right class loader for that.

## What does the "Error loading libc" DEBUG message mean?

The driver is able to perform native system calls through [JNR] in some cases, for example to
achieve microsecond resolution when [generating timestamps](../core/query_timestamps/).

Unfortunately, some of the JNR artifacts available from Maven are not valid OSGi bundles and cannot
be used in OSGi applications.

[JAVA-1127] has been created to track this issue, and there is currently no simple workaround short
of embedding the dependency, which we've chosen not to do.

Because native calls are not available, it is also normal to see the following log lines when
starting the driver:

    [main] DEBUG - Error loading libc
    java.lang.NoClassDefFoundError: jnr/ffi/LibraryLoader
    ...
    [main] INFO - Could not access native clock (see debug logs for details), falling back to Java
    system clock


[driver configuration]: ../core/configuration
[OSGi]:https://www.osgi.org
[JNR]: https://github.com/jnr/jnr-posix
[withClassLoader()]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withClassLoader-java.lang.ClassLoader-
[JAVA-1127]:https://datastax-oss.atlassian.net/browse/JAVA-1127
[DriverConfigLoader.fromDefaults(ClassLoader)]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/config/DriverConfigLoader.html#fromDefaults-java.lang.ClassLoader-
[DriverConfigLoader.programmaticBuilder(ClassLoader)]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/config/DriverConfigLoader.html#programmaticBuilder-java.lang.ClassLoader-
