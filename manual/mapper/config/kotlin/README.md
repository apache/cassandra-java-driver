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

## Kotlin

[Kotlin](https://kotlinlang.org/) is an alternative language for the JVM. Its compact syntax and
native support for annotation processing make it a good fit for the mapper.

We have a full example at [DataStax-Examples/object-mapper-jvm/kotlin].

### Writing the model

You can use Kotlin [data classes] for your entities. Data classes are usually
[immutable](../../entities/#mutability), but you don't need to declare that explicitly with
[@PropertyStrategy]: the mapper detects that it's processing Kotlin code, and will assume `mutable =
false` by default:

```kotlin
@Entity
data class Product(@PartitionKey val id: Int?, val description: String?)
```

Data classes may also be made mutable (by declaring the components with `var` instead of `val`). If
you choose that approach, you'll have to annotate your entities with [@PropertyStrategy], and also
declare a default value for every component in order to generate a no-arg constructor:

```kotlin
@Entity
@PropertyStrategy(mutable = true)
data class Product(@PartitionKey var id: Int? = null, var description: String? = null)
```

All of the [property annotations](../../entities/#property-annotations) can be declared directly on
the components.

If you want to take advantage of [null saving strategies](../../daos/null_saving/), your components
should be nullable.

The other mapper interfaces are direct translations of the Java versions:

```kotlin
@Dao
interface ProductDao {
  @Insert
  fun insert(product: Product)
}
```

Known limitation: because of a Kotlin bug ([KT-4779]), you can't use default interface methods. They
will appear as abstract methods to the mapper processor, which will generate an error since they are
not properly annotated. As a workaround, you can use a companion object method that takes the DAO as
an argument (as shown in [UserDao.kt]), or query provider methods.

### Building

#### Gradle

See the example's [build.gradle].

You enable Kotlin support with [kotlin][gradle_kotlin] and [kotlin_kapt][gradle_kapt], and declare
the mapper processor with the `kapt` directive.

#### Maven

Configure [dual compilation][maven_kotlin_java] of Kotlin and Java sources. In addition, you'll need
an additional execution of the [kotlin-maven-plugin:kapt][maven_kapt] goal with the mapper processor
before compilation:

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
            <groupId>com.scylladb</groupId>
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

[maven_kotlin_java]: https://kotlinlang.org/docs/reference/using-maven.html#compiling-kotlin-and-java-sources
[maven_kapt]: https://kotlinlang.org/docs/reference/kapt.html#using-in-maven
[gradle_kotlin]: https://kotlinlang.org/docs/reference/using-gradle.html
[gradle_kapt]: https://kotlinlang.org/docs/reference/kapt.html#using-in-gradle
[data classes]: https://kotlinlang.org/docs/reference/data-classes.html
[KT-4779]: https://youtrack.jetbrains.com/issue/KT-4779

[DataStax-Examples/object-mapper-jvm/kotlin]: https://github.com/DataStax-Examples/object-mapper-jvm/tree/master/kotlin
[build.gradle]: https://github.com/DataStax-Examples/object-mapper-jvm/blob/master/kotlin/build.gradle
[UserDao.kt]: https://github.com/DataStax-Examples/object-mapper-jvm/blob/master/kotlin/src/main/kotlin/com/datastax/examples/mapper/killrvideo/user/UserDao.kt

[@PropertyStrategy]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/mapper/annotations/PropertyStrategy.html
