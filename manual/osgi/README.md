# OSGi

The driver is available as an [OSGi] bundle.  More specifically, the following maven artifacts are
valid OSGi bundles:

- `java-driver-core`
- `java-driver-query-builder`
- `java-driver-core-shaded`

Note: some of the driver dependencies are not valid OSGi bundles. Most of them are optional, and the
driver can work properly without them; in such cases, the corresponding packages are declared with
optional resolution in `Import-Package` directives. However, if you need to access such packages in 
an OSGi container you MUST wrap the corresponding jar in a valid OSGi bundle and make it available 
for provisioning to the OSGi runtime.

## Using the shaded jar

`java-driver-core-shaded` shares the same bundle name as `java-driver-core`
(`com.datastax.oss.driver.core`).  It can be used as a drop-in replacement in cases where you have
an explicit version of dependency in your project different than that of the driver's.  Refer to
[shaded jar](../core/shaded_jar/) for more information.

## Using a custom `ClassLoader`

In several places of the [driver configuration] it is possible to specify the class name of
something to be instantiated by the driver such as the reconnection policy. This is accomplished
using reflection, which uses a `ClassLoader`.  By default, the driver uses `Thread.currentThread
.getContextClassLoader()` if available, otherwise it uses its own `ClassLoader`.  This is typically
adequate except in environments like application containers or OSGi frameworks where class loading
logic is much more deliberate and libraries are isolated from each other.

If the chosen `ClassLoader` is not able to ascertain whether a loaded class is the same instance
as its expected parent type, you may encounter an error such as:

    java.lang.IllegalArgumentException: Expected class ExponentialReconnectionPolicy
    (specified by advanced.reconnection-policy.class) to be a subtype of
    com.datastax.oss.driver.api.core.connection.ReconnectionPolicy

This is occurring because there is a disparity in the `ClassLoader`s used between the driver code
and the `ClassLoader` used to reflectively load the class (in this case,
`ExponentialReconnectionPolicy`).

You may also encounter `ClassNotFoundException` if the `ClassLoader` does not have access to the
class being loaded.

To overcome these issues, you may specify a `ClassLoader` instance when constructing a `Session`
by using [withClassLoader()].   In a lot of cases, it may be adequate to pass in the `ClassLoader`
from a `Class` that is part of the core driver, i.e.:

```java
CqlSession session = CqlSession.builder()
    .withClassLoader(CqlSession.class.getClassLoader())
    .build();
```

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
[JNR]: https://github.com/jnr/jnr-ffi
[withClassLoader()]: https://docs.datastax.com/en/drivers/java/4.3/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withClassLoader-java.lang.ClassLoader-
[JAVA-1127]:https://datastax-oss.atlassian.net/browse/JAVA-1127