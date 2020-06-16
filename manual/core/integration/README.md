## Integration

### Quick overview

* sample project structures for Maven and Gradle.
* explanations about [driver dependencies](#driver-dependencies) and when they can be manually
  excluded.

-----

### Which artifact(s) should I use?

There are multiple driver artifacts under the group id
[com.datastax.oss](https://search.maven.org/search?q=g:com.datastax.oss). Here's how to pick the
right dependencies:

<table>
<tr><th>Feature</th><th>Artifact(s)</th><th>Comments</th></tr>
<tr>
  <td>
    Core functionality: executing queries with <code>CqlSession.execute()</code>, processing the
    results with <code>ResultSet</code>, etc.
  </td>
  <td><code>java&#8209;driver&#8209;core</code></td>
  <td></td>
</tr>
<tr>
  <td>
    Same as the above, but without explicit dependencies to <a href="#netty">Netty</a>,
    <a href="#jackson">Jackson</a> or <a href="#esri">ESRI</a>. 
  </td>
  <td><code>java&#8209;driver&#8209;core&#8209;shaded</code></td>
  <td>
    Replaces <code>java&#8209;driver&#8209;core</code>.<br/>
    See <a href="../shaded_jar/">this page</a>.
  </td>
</tr>
<tr>
  <td>
    <a href="../../query_builder">Query builder</a>: generating CQL query strings programmatically. 
  </td>
  <td><code>java&#8209;driver&#8209;query&#8209;builder</code></td>
  <td></td>
</tr>
<tr>
  <td>
    <a href="../../mapper">Object mapper</a>: generating the boilerplate to execute queries and
    convert the results into your own domain classes.
  </td>
  <td>
    <code>java&#8209;driver&#8209;mapper&#8209;processor</code><br/>
    <code>java&#8209;driver&#8209;mapper&#8209;runtime</code>
  </td>
  <td>
    Both artifacts are needed.<br/>
    See <a href="../../mapper/config/">this page</a>.
  </td>
</tr>
<tr>
  <td>
    "Bill Of Materials": can help manage versions if you use multiple driver artifacts.
  </td>
  <td><code>java&#8209;driver&#8209;bom</code></td>
  <td>See <a href="../bom/">this page</a>.</td>
</tr>
<tr>
  <td>
    Writing integration tests that run the driver against Cassandra or <a
    href="https://github.com/datastax/simulacron">Simulacron</a>.
  </td>
  <td><code>java&#8209;driver&#8209;test&#8209;infra</code></td>
  <td>
    Those APIs are not covered in this manual, but you can look at the driver's <a
    href="https://github.com/datastax/java-driver/blob/4.x/CONTRIBUTING.md#integration-tests">contribution
    guidelines</a> and <a
    href="https://github.com/datastax/java-driver/tree/4.x/integration-tests">internal tests</a> for
    guidance.
  </td>
</tr>
</table>

### Minimal project structure

We publish the driver to [Maven central][central_oss]. Most modern build tools can download the
dependency automatically.

#### Maven

Create the following 4 files:

```
$ find . -type f
./pom.xml
./src/main/resources/application.conf
./src/main/resources/logback.xml
./src/main/java/Main.java
```

##### Project descriptor

`pom.xml` is the [Project Object Model][maven_pom] that describes your application. We declare the
dependencies, and tell Maven that we're going to use Java 8:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.example.yourcompany</groupId>
  <artifactId>yourapp</artifactId>
  <version>1.0.0-SNAPSHOT</version>

  <dependencies>
    <dependency>
      <groupId>com.datastax.oss</groupId>
      <artifactId>java-driver-core</artifactId>
      <version>${driver.version}</version>
    </dependency>
    <dependency>
      <groupId>ch.qos.logback</groupId>
      <artifactId>logback-classic</artifactId>
      <version>1.2.3</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <artifactId>maven-compiler-plugin</artifactId>
        <configuration>
          <source>1.8</source>
          <target>1.8</target>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

##### Application configuration

`application.conf` is not stricly necessary, but it illustrates an important point about the
driver's [configuration](../configuration/): you override any of the driver's default options here.

```
datastax-java-driver {
  basic.session-name = poc
}
```

In this case, we just specify a custom name for our session, it will appear in the logs.

##### Logging configuration

For this example, we choose Logback as our [logging framework](../logging/) (we added the dependency
in `pom.xml`). `logback.xml` configures it to send the driver's `INFO` logs to the console.

```xml
<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>
  <root level="WARN">
    <appender-ref ref="STDOUT"/>
  </root>
  <logger name="com.datastax.oss.driver" level= "INFO"/>
</configuration>
```

Again, this is not strictly necessary: a truly minimal example could run without the Logback
dependency, or this file; but the default behavior is a bit verbose. 

##### Main class

`Main.java` is the canonical example introduced in our [quick start](../#quick-start); it connects
to Cassandra, queries the server version and prints it:

```java
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;

public class Main {
  public static void main(String[] args) {
    try (CqlSession session = CqlSession.builder().build()) {
      ResultSet rs = session.execute("SELECT release_version FROM system.local");
      System.out.println(rs.one().getString(0));
    }
  }
}
```

Make sure you have a Cassandra instance running on 127.0.0.1:9042 (otherwise, you use
[CqlSession.builder().addContactPoint()][SessionBuilder.addContactPoint] to use a different
address).

##### Running

To launch the program from the command line, use:

```
$ mvn compile exec:java -Dexec.mainClass=Main
```

You should see output similar to:

```
...
[INFO] ------------------------------------------------------------------------
[INFO] Building yourapp 1.0.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
... (at this point, Maven will download the dependencies the first time) 
[INFO] --- maven-resources-plugin:2.6:resources (default-resources) @ yourapp ---
[WARNING] Using platform encoding (UTF-8 actually) to copy filtered resources, i.e. build is platform dependent!
[INFO] Copying 1 resource
[INFO]
[INFO] --- maven-compiler-plugin:2.5.1:compile (default-compile) @ yourapp ---
[INFO] Nothing to compile - all classes are up to date
[INFO]
[INFO] --- exec-maven-plugin:1.3.1:java (default-cli) @ yourapp ---
11:39:45.355 [Main.main()] INFO  c.d.o.d.i.c.DefaultMavenCoordinates - DataStax Java driver for Apache Cassandra(R) (com.datastax.oss:java-driver-core) version 4.0.1
11:39:45.648 [poc-admin-0] INFO  c.d.o.d.internal.core.time.Clock - Using native clock for microsecond precision
11:39:45.649 [poc-admin-0] INFO  c.d.o.d.i.c.metadata.MetadataManager - [poc] No contact points provided, defaulting to /127.0.0.1:9042
3.11.2
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 11.777 s
[INFO] Finished at: 2018-06-18T11:32:49-08:00
[INFO] Final Memory: 16M/277M
[INFO] ------------------------------------------------------------------------
```

#### Gradle

[Initialize a new project][gradle_init] with Gradle.

Modify `build.gradle` to add the dependencies:

```groovy
group 'com.example.yourcompany'
version '1.0.0-SNAPSHOT'

apply plugin: 'java'

sourceCompatibility = 1.8

repositories {
    mavenCentral()
}

dependencies {
    compile group: 'com.datastax.oss', name: 'java-driver-core', version: '${driver.version}'
    compile group: 'ch.qos.logback', name: 'logback-classic', version: '1.2.3'
}
```

Then place [application.conf](#application-configuration), [logback.xml](#logging-configuration) and
[Main.java](#main-class) in the same locations, and with the same contents, as in the Maven example:

```
./src/main/resources/application.conf
./src/main/resources/logback.xml
./src/main/java/Main.java
```

Optionally, if you want to run from the command line, add the following at the end of
`build.gradle`:

```groovy
task execute(type:JavaExec) {
    main = 'Main'
    classpath = sourceSets.main.runtimeClasspath
}
```

Then launch with:

```
$ ./gradlew execute
```

You should see output similar to:

```
$ ./gradlew execute
:compileJava
:processResources
:classes
:execute
13:32:25.339 [main] INFO  c.d.o.d.i.c.DefaultMavenCoordinates - DataStax Java driver for Apache Cassandra(R) (com.datastax.oss:java-driver-core) version 4.0.1-alpha4-SNAPSHOT
13:32:25.682 [poc-admin-0] INFO  c.d.o.d.internal.core.time.Clock - Using native clock for microsecond precision
13:32:25.683 [poc-admin-0] INFO  c.d.o.d.i.c.metadata.MetadataManager - [poc] No contact points provided, defaulting to /127.0.0.1:9042
3.11.2

BUILD SUCCESSFUL
```

#### Manually (from the binary tarball)

If your build tool can't fetch dependencies from Maven central, we publish a binary tarball on the 
[DataStax download server][downloads].

The driver and its dependencies must be in the compile-time classpath. Application resources, such
as `application.conf` and `logback.xml` in our previous examples, must be in the runtime classpath.

### JPMS support

All the driver's artifacts are JPMS automatic modules.

Note that TinkerPop cannot currently be used in a JPMS application. You will get the following
error:

```
Error occurred during initialization of boot layer
java.lang.module.FindException: Unable to derive module descriptor for /path/to/gremlin-shaded-3.4.5.jar
Caused by: java.lang.module.InvalidModuleDescriptorException: Provider class com.fasterxml.jackson.core.JsonFactory not in module
```

This is a known issue that will be resolved in TinkerPop 3.4.7. The driver will upgrade as soon as
possible, see [JAVA-2726](https://datastax-oss.atlassian.net/browse/JAVA-2726).

Unfortunately, the only workaround in the meantime is to exclude TinkerPop dependencies, as
explained [here](#tinker-pop). Graph functionality won't be available.

### Driver dependencies

The driver depends on a number of third-party libraries; some of those dependencies are opt-in,
while others are present by default, but may be excluded under specific circumstances.

Here's a rundown of what you can customize:

#### Netty

[Netty](https://netty.io/) is the NIO framework that powers the driver's networking layer.

It is a required dependency, but we provide a a [shaded JAR](../shaded_jar/) that relocates it to a
different Java package; this is useful to avoid dependency hell if you already use Netty in another
part of your application.

#### Typesafe config

[Typesafe config](https://lightbend.github.io/config/) is used for our file-based
[configuration](../configuration/).

It is a required dependency if you use the driver's built-in configuration loader, but this can be
[completely overridden](../configuration/#bypassing-typesafe-config) with your own implementation,
that could use a different framework or an ad-hoc solution.

In that case, you can exclude the dependency:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>${driver.version}</version>
  <exclusions>
    <exclusion>
      <groupId>com.typesafe</groupId>
      <artifactId>config</artifactId>
    </exclusion>
  </exclusions>
</dependency>
``` 

#### Native libraries

The driver performs native calls with [JNR](https://github.com/jnr). This is used in two cases:

* to access a microsecond-precision clock in [timestamp generators](../query_timestamps/);
* to get the process ID when generating [UUIDs][Uuids].

In both cases, this is completely optional; if system calls are not available on the current
platform, or the library fails to load for any reason, the driver falls back to pure Java
workarounds.

If you don't want to use system calls, or already know (from looking at the driver's logs) that they
are not available on your platform, you can exclude the following dependency:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>${driver.version}</version>
  <exclusions>
    <exclusion>
      <groupId>com.github.jnr</groupId>
      <artifactId>jnr-posix</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

#### Compression libraries

The driver supports compression with either [LZ4](https://github.com/jpountz/lz4-java) or
[Snappy](http://google.github.io/snappy/).

These dependencies are optional; you have to add them explicitly in your application in order to
enable compression. See the [Compression](../compression/) page for more details.

#### Metrics

The driver exposes [metrics](../metrics/) through the
[Dropwizard](http://metrics.dropwizard.io/4.0.0/manual/index.html) library.

The dependency is declared as required, but metrics are optional. If you've disabled all metrics,
and never call [Session.getMetrics] anywhere in your application, you can remove the dependency:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>${driver.version}</version>
  <exclusions>
    <exclusion>
      <groupId>io.dropwizard.metrics</groupId>
      <artifactId>metrics-core</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

In addition, "timer" metrics use [HdrHistogram](http://hdrhistogram.github.io/HdrHistogram/) to
record latency percentiles. At the time of writing, these metrics are: `cql-requests`,
`throttling.delay` and `cql-messages`; you can also identify them by reading the comments in the
[configuration reference](../configuration/reference/) (look for "exposed as a Timer").

If all of these metrics are disabled, you can remove the dependency:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>${driver.version}</version>
  <exclusions>
    <exclusion>
      <groupId>org.hdrhistogram</groupId>
      <artifactId>HdrHistogram</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

#### Jackson

[Jackson](https://github.com/FasterXML/jackson) is used:

* when connecting to [Datastax Astra](../../cloud/);
* when Insights monitoring is enabled.
 
If you don't use either of those features, you can safely exclude the dependency:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>${driver.version}</version>
  <exclusions>
    <exclusion>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-core</artifactId>
    </exclusion>
    <exclusion>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

#### Esri

Our [geospatial types](../dse/geotypes/) implementation is based on the [Esri Geometry
API](https://github.com/Esri/geometry-api-java).

If you don't use geospatial types anywhere in your application, you can exclude the dependency:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>${driver.version}</version>
  <exclusions>
   <exclusion>
     <groupId>com.esri.geometry</groupId>
     <artifactId>esri-geometry-api</artifactId>
   </exclusion>
  </exclusions>
</dependency>
```

#### TinkerPop

[Apache TinkerPop™](http://tinkerpop.apache.org/) is used in our [graph API](../dse/graph/).

If you don't use DSE graph at all, you can exclude the dependencies:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>${driver.version}</version>
  <exclusions>
    <exclusion>
      <groupId>org.apache.tinkerpop</groupId>
      <artifactId>gremlin-core</artifactId>
    </exclusion>
    <exclusion>
      <groupId>org.apache.tinkerpop</groupId>
      <artifactId>tinkergraph-gremlin</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

If you do use graph, it is important to keep the precise TinkerPop version that the driver depends
on: unlike the driver, TinkerPop does not follow semantic versioning, so even a patch version change
(e.g. 3.3.0 vs 3.3.3) could introduce incompatibilities. So do not declare an explicit dependency in
your application, let the driver pull it transitively.

#### Reactive Streams

[Reactive Streams](https://www.reactive-streams.org/) types are referenced in our [reactive
API](../reactive/).

If you never call any of the `executeReactive` methods, you can exclude the dependency:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>${driver.version}</version>
  <exclusions>
    <exclusion>
      <groupId>org.reactivestreams</groupId>
      <artifactId>reactive-streams</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

#### Documenting annotations

The driver team uses annotations to document certain aspects of the code:

* thread safety with [Java Concurrency in Practice](http://jcip.net/annotations/doc/index.html)
  annotations `@Immutable`, `@ThreadSafe`, `@NotThreadSafe` and `@GuardedBy`;
* nullability with [SpotBugs](https://spotbugs.github.io/) annotations `@Nullable` and `@NonNull`.

This is mostly used during development; while these annotations are retained in class files, they
serve no purpose at runtime. If you want to minimize the number of JARs in your classpath, you can
exclude them:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>${driver.version}</version>
  <exclusions>
    <exclusion>
      <groupId>com.github.stephenc.jcip</groupId>
      <artifactId>jcip-annotations</artifactId>
    </exclusion>
    <exclusion>
      <groupId>com.github.spotbugs</groupId>
      <artifactId>spotbugs-annotations</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

However, there is one case when excluding those dependencies won't work: if you use [annotation
processing] in your build, the Java compiler scans the entire classpath -- including the driver's
classes -- and tries to load all declared annotations. If it can't find the class for an annotation,
you'll get a compiler error:

```
error: cannot access ThreadSafe
  class file for net.jcip.annotations.ThreadSafe not found
1 error
```

The workaround is to keep the dependencies.

Sometimes annotation scanning can be triggered involuntarily, if one of your dependencies declares
a processor via the service provider mechanism (check the `META-INF/services` directory in the
JARs). If you are sure that you don't need any annotation processing, you can compile with the
`-proc:none` option and still exclude the dependencies. 

#### Mandatory dependencies

The remaining core driver dependencies are the only ones that are truly mandatory:

* the [native protocol](https://github.com/datastax/native-protocol) layer. This is essentially part
  of the driver code, but was externalized for reuse in other projects;
* `java-driver-shaded-guava`, a shaded version of [Guava](https://github.com/google/guava). It is
  relocated to a different package, and only used by internal driver code, so it should be
  completely transparent to third-party code;
* the [SLF4J](https://www.slf4j.org/) API for [logging](../logging/).

[central_oss]: https://search.maven.org/#search%7Cga%7C1%7Ccom.datastax.oss
[maven_pom]: https://maven.apache.org/guides/introduction/introduction-to-the-pom.html
[gradle_init]: https://guides.gradle.org/creating-new-gradle-builds/
[downloads]: http://downloads.datastax.com/java-driver/
[guava]: https://github.com/google/guava/issues/2721
[annotation processing]: https://docs.oracle.com/javase/8/docs/technotes/tools/windows/javac.html#sthref65

[Session.getMetrics]:             https://docs.datastax.com/en/drivers/java/4.7/com/datastax/oss/driver/api/core/session/Session.html#getMetrics--
[SessionBuilder.addContactPoint]: https://docs.datastax.com/en/drivers/java/4.7/com/datastax/oss/driver/api/core/session/SessionBuilder.html#addContactPoint-java.net.InetSocketAddress-
[Uuids]:                          https://docs.datastax.com/en/drivers/java/4.7/com/datastax/oss/driver/api/core/uuid/Uuids.html
