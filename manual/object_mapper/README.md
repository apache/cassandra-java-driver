# Object Mapper

The driver provides a simple object mapper, which
avoids most of the boilerplate when converting your domain classes to
and from query results. It handles basic CRUD operations in Cassandra tables
containing UDTs, collections and all native CQL types.

The mapper is published as a separate Maven artifact:

```xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-mapping</artifactId>
  <version>3.0.7</version>
</dependency>
```

See the child pages for more information:

* [definition of mapped classes](creating/)
* [using the mapper](using/)
* [using custom codecs](custom_codecs/)
