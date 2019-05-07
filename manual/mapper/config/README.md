## Configuring the annotation processor

The mapper's annotation processor hooks into the Java compiler, and generates additional source
files from your annotated classes before the main compilation happens. It is contained in the
`java-driver-mapper-processor` artifact.

As a reminder, there is also a `java-driver-mapper-runtime` artifact, which contains the annotations
and a few utility classes. This one is a regular dependency, and it is required at runtime.

### Maven

The best approach is to add the `annotationProcessorPaths` option to the compiler plugin's
configuration:

```xml
<properties>
  <java-driver.version>...</java-driver.version>
</properties>

<dependencies>
  <dependency>
    <groupId>com.datastax.oss</groupId>
    <artifactId>java-driver-mapper-runtime</artifactId>
    <version>${java-driver.version}</version>
  </dependency>
</dependencies>

<build>
  <plugins>
    <plugin>
      <artifactId>maven-compiler-plugin</artifactId>
      <configuration>
        <annotationProcessorPaths>
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

The processor runs every time you execute the `mvn compile` phase. It normally supports incremental
builds, but if something looks off you can try a full rebuild with `mvn clean compile`.

One of the advantages of annotation processing is that the generated code is produced as regular
source files, that you can read and debug like the rest of your application. With the above
configuration, these files are in `target/generated-sources/annotations`. They follow the same
package structure as your annotated types. Most end in a special `__MapperGenerated` suffix, in
order to clearly identify them in stack traces (one exception is the mapper builder, because it is
referenced directly from your code).

Do not edit the generated files directly: your changes would be overwritten during the next full
rebuild.

### Gradle

Use the following configuration (Gradle 4.6 and above):

```groovy
apply plugin: 'java'

def javaDriverVersion = '...'

dependencies {
    annotationProcessor group: 'com.datastax.oss', name: 'java-driver-mapper-processor', version: javaDriverVersion
    compile group: 'com.datastax.oss', name: 'java-driver-mapper-runtime', version: javaDriverVersion
}
```

You will find the generated files in `build/generated/sources/annotationProcessor`.
