## Bill of Materials (BOM)

A "Bill Of Materials" is a special Maven descriptor that defines the versions of a set of related
artifacts.

To import the driver's BOM, add the following section in your application's own POM:

```xml
<project>
  ...
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>com.datastax.oss</groupId>
        <artifactId>java-driver-bom</artifactId>
        <version>4.13.0</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>
```

This allows you to omit the version when you later reference the driver artifacts:

```xml
<project>
  ...
  <dependencies>
    <dependency>
      <groupId>com.datastax.oss</groupId>
      <artifactId>java-driver-query-builder</artifactId>
    </dependency>
  </dependencies>
```

The advantage is that this also applies to transitive dependencies. For example, if there is a
third-party library X that depends on `java-driver-core`, and you add a dependency to X in this
project, `java-driver-core` will be set to the BOM version, regardless of which version X declares
in its POM. The driver artifacts are always in sync, however they were pulled into the project.

### BOM and mapper processor

If you are using the driver's [object mapper](../../mapper), our recommendation is to declare the
mapper processor in the [annotationProcessorPaths](../../mapper/config/#maven) section of the
compiler plugin configuration. Unfortunately, `<dependencyManagement>` versions don't work there,
this is a known Maven issue ([MCOMPILER-391]).

As a workaround, you can either declare the mapper processor as a regular dependency in the provided
scope:

```xml
  <dependencies>
    <dependency>
      <groupId>com.datastax.oss</groupId>
      <artifactId>java-driver-mapper-processor</artifactId>
      <scope>provided</scope>
    </dependency>
  </dependencies>
```

Or keep it in the compiler plugin, but repeat the version explicitly. In that case, it's probably a
good idea to extract a property to keep it in sync with the BOM:

```xml
<project>
  <properties>
    <java-driver.version>4.13.0</java-driver.version>
  </properties>
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>com.datastax.oss</groupId>
        <artifactId>java-driver-bom</artifactId>
        <version>${java-driver.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>
  <dependencies>
    <!-- Regular dependency, no need to repeat the version: -->
    <dependency>
      <groupId>com.datastax.oss</groupId>
      <artifactId>java-driver-mapper-runtime</artifactId>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <artifactId>maven-compiler-plugin</artifactId>
        <configuration>
          <annotationProcessorPaths>
            <!-- Annotation processor, can't use the BOM => explicit version -->
            <path>
              <groupId>com.datastax.oss</groupId>
              <artifactId>java-driver-mapper-processor</artifactId>
              <version>${java-driver.version}</version>
            </path>
          </annotationProcessorPaths>
        </configuration>
      </plugin>
    </plugins>
  </build>
```

[MCOMPILER-391]: https://issues.apache.org/jira/browse/MCOMPILER-391