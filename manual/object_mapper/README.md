# Object Mapper

Version 2.1 of the driver introduces a simple object mapper, which
avoids most of the boilerplate when converting your domain classes to
and from query results. It handles basic CRUD operations in Cassandra tables
containing UDTs, collections and all native CQL types.

The mapper is published as a separate Maven artifact:

```xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-mapping</artifactId>
  <version>3.0.0-rc1</version>
</dependency>
```

This documentation is organized in subsections describing mapper features.
