## Lombok

[Lombok](https://projectlombok.org/) is a popular library that automates repetitive code, such as
getters and setters. You can use it in conjunction with the mapper to eliminate even more
boilerplate.

We have a full example at [DataStax-Examples/object-mapper-jvm/lombok].

### Writing the model

You can either map mutable "data" classes:

```java
import lombok.Data;
import com.datastax.oss.driver.api.mapper.annotations.*;

@Data
@Entity
public class Product {
  @PartitionKey private int id;
  private String description;
}
```

Or immutable "value" classes:

```java
import lombok.Value;
import com.datastax.oss.driver.api.mapper.annotations.*;

@Value
@Entity
@PropertyStrategy(mutable = false)
public class Product {
  @PartitionKey private int id;
  private String description;
}
```

You can also use Lombok's fluent accessors if you configure the mapper accordingly:

```java
import lombok.Data;
import lombok.experimental.Accessors;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.datastax.oss.driver.api.mapper.entity.naming.*;

@Data
@Accessors(fluent = true)
@Entity
@PropertyStrategy(getterStyle = GetterStyle.FLUENT, setterStyle = SetterStyle.FLUENT)
public static class Product {
  @PartitionKey private int id;
  private String description;
}
```

### Building

You'll need to configure the Lombok annotation processor in your build. The only requirement is that
it must run *before* the mapper's.

#### Maven

See the compiler plugin's configuration in the example's [pom.xml].

#### Gradle

A similar result can be achieved with:

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


[DataStax-Examples/object-mapper-jvm/lombok]: https://github.com/DataStax-Examples/object-mapper-jvm/tree/master/lombok
[pom.xml]: https://github.com/DataStax-Examples/object-mapper-jvm/blob/master/lombok/pom.xml
