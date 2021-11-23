## Java 14 Records

Java 14 introduced [Record] as a lightweight, immutable alternative to POJOs. You can map annotated
records as entities.

We have a full example at [DataStax-Examples/object-mapper-jvm/record].

Note: records are a **preview feature** of Java 14. As such the mapper's support for them is also
provided as a preview.

### Writing the model

Annotate your records like regular classes:

```java
@Entity
record Product(@PartitionKey int id, String description) {}
```

Records are immutable and use the [fluent getter style](../../entities#getter-style), but you don't
need to declare that explicitly with [@PropertyStrategy]: the mapper detects when it's processing a
record, and will assume `mutable = false, getterStyle = FLUENT` by default.

### Building

You need to build with Java 14, and pass the `--enable-preview` flag to both the compiler and the
runtime JVM. See [pom.xml] in the example.


[@PropertyStrategy]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/PropertyStrategy.html

[DataStax-Examples/object-mapper-jvm/record]: https://github.com/DataStax-Examples/object-mapper-jvm/tree/master/record
[pom.xml]: https://github.com/DataStax-Examples/object-mapper-jvm/blob/master/record/pom.xml

[Record]: https://docs.oracle.com/en/java/javase/14/docs/api/java.base/java/lang/Record.html
