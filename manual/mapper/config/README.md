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

## Integration

### Builds tools

The `java-driver-mapper-processor` artifact contains the annotation processor. It hooks into the
Java compiler, and generates additional source files from your annotated classes before the main
compilation happens. It is only required in the compile classpath.

The `java-driver-mapper-runtime` artifact contains the annotations and a few utility classes. It is
a regular dependency, required at runtime.

#### Maven

The best approach is to add the `annotationProcessorPaths` option to the compiler plugin's
configuration (make sure you use version 3.5 or higher):

```xml
<properties>
  <java-driver.version>...</java-driver.version>
</properties>

<dependencies>
  <dependency>
    <groupId>org.apache.cassandra</groupId>
    <artifactId>java-driver-mapper-runtime</artifactId>
    <version>${java-driver.version}</version>
  </dependency>
</dependencies>

<build>
  <plugins>
    <plugin>
      <artifactId>maven-compiler-plugin</artifactId>
      <version>3.8.1</version>
      <configuration>
        <source>1.8</source> <!-- (or higher) -->
        <target>1.8</target> <!-- (or higher) -->
        <annotationProcessorPaths>
          <path>
            <groupId>org.apache.cassandra</groupId>
            <artifactId>java-driver-mapper-processor</artifactId>
            <version>${java-driver.version}</version>
          </path>
          <!-- Optional: add this if you want to avoid the SLF4J warning "Failed to load class
            StaticLoggerBinder, defaulting to no-operation implementation" when compiling. -->
          <path>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-nop</artifactId>
            <version>1.7.26</version>
          </path>
        </annotationProcessorPaths>
      </configuration>
    </plugin>
  </plugins>
</build>
```

Alternatively (e.g. if you are using the [BOM](../../core/bom/)), you may also declare the processor
as a regular dependency in the "provided" scope:

```xml
<dependencies>
  <dependency>
    <groupId>org.apache.cassandra</groupId>
    <artifactId>java-driver-mapper-processor</artifactId>
    <version>${java-driver.version}</version>
    <scope>provided</scope>
  </dependency>
  <dependency>
    <groupId>org.apache.cassandra</groupId>
    <artifactId>java-driver-mapper-runtime</artifactId>
    <version>${java-driver.version}</version>
  </dependency>
</dependencies>
```

The processor runs every time you execute the `mvn compile` phase. It normally supports incremental
builds, but if something looks off you can try a full rebuild with `mvn clean compile`.

One of the advantages of annotation processing is that the generated code is produced as regular
source files, that you can read and debug like the rest of your application. With the above
configuration, these files are in `target/generated-sources/annotations`. Make sure that
directory is marked as a source folder in your IDE (for example, in IntelliJ IDEA, this might
require right-clicking on your `pom.xml` and selecting "Maven > Reimport").

Generated sources follow the same package structure as your annotated types. Most end in a special
`__MapperGenerated` suffix, in order to clearly identify them in stack traces (one exception is the
mapper builder, because it is referenced directly from your code).

Do not edit those files files directly: your changes would be overwritten during the next full
rebuild.

#### Gradle

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

### Integration with other languages and libraries

* <a name="kotlin"></a>[Kotlin](kotlin/)
* <a name="lombok"></a>[Lombok](lombok/)
* [Java 14 records](record/)
* [Scala](scala/)
