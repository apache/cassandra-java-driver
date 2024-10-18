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

## Configuring the annotation processor

The mapper's annotation processor hooks into the Java compiler, and generates additional source
files from your annotated classes before the main compilation happens. It is contained in the
`java-driver-mapper-processor` artifact.

As a reminder, there is also a `java-driver-mapper-runtime` artifact, which contains the annotations
and a few utility classes. This one is a regular dependency, and it is required at runtime.

### Builds tools

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

#### Lombok

[Lombok](https://projectlombok.org/) is a popular library that automates boilerplate code, such as
getters and setters. This can be convenient for mapped entities:

```java
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Entity
@EqualsAndHashCode
@ToString
public class Product {
  @PartitionKey @Getter @Setter private int id;
  @Getter @Setter private String description;
}
```

The mapper can process Lombok-annotated classes just like regular code. The only requirement is that
Lombok's annotation processor must run *before* the mapper's.

With Maven, declaring Lombok as a provided dependency is not enough; you must also redeclare it in
the `<annotationProcessorPaths>` section, before the mapper:

```xml
<properties>
  <java-driver.version>...</java-driver.version>
  <lombok.version>...</lombok.version>
</properties>

<dependencies>
  <dependency>
    <groupId>com.datastax.oss</groupId>
    <artifactId>java-driver-mapper-runtime</artifactId>
    <version>${java-driver.version}</version>
  </dependency>
  <dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <version>${lombok.version}</version>
    <scope>provided</scope>
  </dependency>
</dependencies>

<build>
  <plugins>
    <plugin>
      <artifactId>maven-compiler-plugin</artifactId>
      <version>3.8.1</version>
      <configuration>
        <source>1.8</source>
        <target>1.8</target>
        <annotationProcessorPaths>
          <path>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>${lombok.version}</version>
          </path>
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

With Gradle, a similar result can be achieved with:

```groovy
apply plugin: 'java'

def javaDriverVersion = '...'
def lombokVersion = '...'

dependencies {
    annotationProcessor group: 'org.projectlombok', name: 'lombok', version: lombokVersion
    annotationProcessor group: 'com.datastax.oss', name: 'java-driver-mapper-processor', version: javaDriverVersion
    compile group: 'com.datastax.oss', name: 'java-driver-mapper-runtime', version: javaDriverVersion
    compileOnly group: 'org.projectlombok', name: 'lombok', version: lombokVersion
}
```

You'll also need to install a Lombok plugin in your IDE (for IntelliJ IDEA, [this
one](https://plugins.jetbrains.com/plugin/6317-lombok) is available in the marketplace).

#### Kotlin

[Kotlin](https://kotlinlang.org/) is an alternative language for the JVM. Its compact syntax and
native support for annotation processing make it a good fit for the mapper.

To set up your project, refer to the Kotlin website:

* Maven: configure [dual compilation][maven_kotlin_java] of Kotlin and Java sources. In addition,
  you'll need an additional execution of the [kotlin-maven-plugin:kapt][maven_kapt] goal with the
  mapper processor before compilation:

    ```xml
    <plugin>
      <groupId>org.jetbrains.kotlin</groupId>
      <artifactId>kotlin-maven-plugin</artifactId>
      <version>${kotlin.version}</version>  
      <executions>  
        <execution>
          <id>kapt</id>
          <goals><goal>kapt</goal></goals>
          <configuration>
            <sourceDirs>
              <sourceDir>src/main/kotlin</sourceDir>
              <sourceDir>src/main/java</sourceDir>
            </sourceDirs>
            <annotationProcessorPaths>
              <annotationProcessorPath>
                <groupId>com.datastax.oss</groupId>
                <artifactId>java-driver-mapper-processor</artifactId>
                <version>${java-driver.version}</version>
              </annotationProcessorPath>
            </annotationProcessorPaths>
          </configuration>
        </execution>
        <execution>
          <id>compile</id>
          <goals><goal>compile</goal></goals>
          ...
        </execution>
      </executions>
    </plugin>
    ```
    
* Gradle: configure the [kotlin][gradle_kotlin] and [kotlin_kapt][gradle_kapt] plugins in your build
  script. In addition, declare the dependency to the mapper processor with `kapt` instead of
  `annotationProcessor`:
  
    ```groovy
    apply plugin: 'kotlin'
    apply plugin: 'kotlin-kapt'
    
    dependencies {
        kapt group: 'com.datastax.oss', name: 'java-driver-mapper-processor', version: javaDriverVersion
        ...
    }
    ```

You can use Kotlin [data classes] for your entities. Just keep in mind that the mapper expects a
no-arg constructor, which means that you must define default values; and setters, which means that
properties must be declared with `var`, not `val`.

```kotlin
@Entity
data class Product(@PartitionKey var id: Int? = null, var description: String? = null)
```

All of the [property annotations](../entities/#property-annotations) can be declared directly on the
constructor properties.

If you want to take advantage of [null saving strategies](../daos/null_saving/), your properties
should be nullable.

The other mapper interfaces are pretty similar to the Java versions:

```kotlin
@Dao
interface ProductDao {
  @Insert
  fun insert(product: Product)
}
```

[maven_kotlin_java]: https://kotlinlang.org/docs/reference/using-maven.html#compiling-kotlin-and-java-sources
[maven_kapt]: https://kotlinlang.org/docs/reference/kapt.html#using-in-maven
[gradle_kotlin]: https://kotlinlang.org/docs/reference/using-gradle.html
[gradle_kapt]: https://kotlinlang.org/docs/reference/kapt.html#using-in-gradle
[data classes]: https://kotlinlang.org/docs/reference/data-classes.html
