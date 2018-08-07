## Frequently Asked Questions - OSGi

### How to use the Java driver in an OSGi environment?

We have complete examples demonstrating usage of the driver in an [OSGi]
environment; please refer to our [OSGi examples repository].


### How to override Guava's version?

The driver is compatible and tested with all versions of Guava in the range
`[16.0.1,20)`.

If using Maven, you can force a more specific version by re-declaring
the Guava dependency in your project, e.g.:

    <dependency>
        <groupId>com.google.guava</groupId>
        <artifactId>guava</artifactId>
        <version>19.0</version>
    </dependency>

Make sure that your project's manifest is importing the right version
of Guava's packages, e.g. for 19.0:

    Import-Package: com.google.common.base;version="[19.0,20)"


### How to enable compression?

First, read our [manual page on compression](../../manual/compression/)
to understand how to enable compression for the Java driver.
            
OSGi projects can use both Snappy or LZ4 compression algorithms. 

For Snappy, include the following Maven dependency:

    <dependency>
        <groupId>org.xerial.snappy</groupId>
        <artifactId>snappy-java</artifactId>
        <version>1.1.2.6</version>
    </dependency>

For LZ4, include the following Maven dependency:

    <dependency>
        <groupId>net.jpountz.lz4</groupId>
        <artifactId>lz4</artifactId>
        <version>1.3.0</version>
    </dependency>

**IMPORTANT**: versions of LZ4 library below 1.3.0 cannot be used
because they are _not_ valid OSGi bundles.

Because compression libraries are _optional runtime dependencies_, 
most manifest generation tools (such as [BND] and the [Maven bundle plugin]) 
will not reference these libraries in your project's manifest.
This is correct, but could be a problem for some OSGi provisioning tools,
and notably for [Tycho], because it does not consider such
dependencies when computing the target platform.

If you are facing provisioning issues related to compression libraries,
you might need to either add them explicitly to your OSGi runtime,
or explicitly reference them in your project's manifest.
With the [Maven bundle plugin], this second option can be achieved with the following 
[BND] `Import-Package` instruction (the example below is for
LZ4, but the same applies to Snappy as well):

    <Import-Package>net.jpountz.lz4,*</Import-Package>

With Tycho, another option is to explicitly declare an "extra requirement"
to the compression library in the target platform definition:

    <plugin>
        <groupId>org.eclipse.tycho</groupId>
        <artifactId>target-platform-configuration</artifactId>
        <version>0.25.0</version>
        <configuration>
            <dependency-resolution>
                <extraRequirements>
                    <requirement>
                        <id>lz4-java</id>
                        <versionRange>1.3.0</versionRange>
                        <type>eclipse-plugin</type>
                    </requirement>
                </extraRequirements>
            </dependency-resolution>
            ...
        </configuration>
    </plugin>
    
Note that the requirement id is its bundle symbolic name,
_not_ its Maven artifact id.

    
### How to use the driver shaded jar?

The driver [shaded jar](../../manual/shaded_jar/) can be used 
in any OSGi application, although the same limitations explained in
the manual apply.


### How to get proper logs?

The driver uses [SLF4j] for [logging](../../manual/logging/).

You OSGi runtime should therefore include the SLF4J API bundle, and
one valid implementation bundle, such as [Logback].

For Maven-based projects, this can be achieved with the following 
dependencies:

    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
        <version>1.7.12</version>
    </dependency>

    <dependency>
        <groupId>ch.qos.logback</groupId>
        <artifactId>logback-classic</artifactId>
        <version>1.1.2</version>
        <scope>runtime</scope>
    </dependency>

Some OSGi containers might require additional configuration.
Please consult their documentation for further details.
                

### I'm getting the error: "Unable to access `sun.misc.Unsafe`"

Some of the driver dependencies try to access non-API packages
such as `sun.misc`, which is usually not available in an OSGi container.

Most of these libraries are smart enough to check if this package
is available or not before actually trying to use it.

However, the [metrics library](../../manual/metrics/) used by the driver
attempts to load `sun.misc.Unsafe` without
probing for it first, which may lead to bundle activation errors.

[JAVA-1203] has been created to track this issue, which
should be fixed in the next major release of the library.

There are a few workarounds:

- Add `sun.misc` to the list of system packages (i.e., packages
exported by the system bundle), or to the list of boot delegation
packages (i.e., defer loading of `sun.misc` to the boot classloader).
This is the best option as it benefits to other libraries as well.
With Felix, this can be achieved with one of the following start options:

    ```
    --bootDelegation=sun.misc
    --systemPackages=sun.misc
    ```

- Disable metrics altogether:

    ```java
    Cluster cluster = Cluster.builder()
            .addContactPoints(...)
            .withoutMetrics()
            .build();
    ```

### I'm getting the error: "Could not load JNR C Library"

The driver is able to perform native system calls through JNR in some cases,
for example to achieve microsecond resolution when 
[generating timestamps](../../manual/query_timestamps/).

Unfortunately, some of the JNR artifacts available from Maven 
are not valid OSGi bundles and cannot be used in OSGi applications.

[JAVA-1127] has been created to track this issue, and there
is currently no simple workaround.

Note that if you use Maven and include any JNR dependency
in your pom, _these will be silently ignored by Pax Exam when
running integration tests_, and most likely, your tests will
fail with provisioning errors.

Because native calls are not available, 
it is also normal to see the following log lines when starting the driver:

    INFO - Could not load JNR C Library, native system calls through this library will not be available
    INFO - Using java.lang.System clock to generate timestamps.


[OSGi]:https://www.osgi.org
[Felix]:https://cwiki.apache.org/confluence/display/FELIX/Index
[JAVA-1203]:https://datastax-oss.atlassian.net/browse/JAVA-1203
[JAVA-1127]:https://datastax-oss.atlassian.net/browse/JAVA-1127
[BND]:http://bnd.bndtools.org/
[Maven bundle plugin]:https://cwiki.apache.org/confluence/display/FELIX/Apache+Felix+Maven+Bundle+Plugin+%28BND%29
[OSGi examples repository]:https://github.com/datastax/java-driver-examples-osgi
[without metrics]:http://docs.datastax.com/en/drivers/java/3.2/com/datastax/driver/core/Cluster.Builder.html#withoutMetrics--
[SLF4J]:http://www.slf4j.org/
[Logback]:http://logback.qos.ch/
[Tycho]:https://eclipse.org/tycho/
