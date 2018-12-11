# Cassandra Java Driver Archetype Project

If you are reading this, you've successfully created the Cassandra Java Driver archetype project.

## Running the project

### Ensure Cassandra is running

Cassandra is configured at ${cassandra-host}:${cassandra-port}. Ensure that Cassandra is up and
running on that host and port. If the Cassandra host/port to which you want to connect has changed,
or was not set correctly when the project was generated, you will have to edit the `Main.java`
source file and alter this line accordingly:

```
builder.addContactPoint(InetSocketAddress.createUnresolved("${cassandra-host}", ${cassandra-port}));
```

### Compiling and running the example

Once you have a local Cassandra running (or have changed `Main.java` to point to some other instance
that is running), you can build this project by executing the following:

```
mvn clean install
```

Then run the CQL demo with:

```
mvn exec:java -Dexec.mainClass=${package}.Main -pl ${artifactId}-cql
```

Running this example project should connect to Cassandra and log out the version:

```
15:01:13.319 [${package}.Main.main()] INFO  c.d.o.d.i.c.DefaultMavenCoordinates - DataStax Java driver for Apache Cassandra(R) (com.datastax.oss:java-driver-core) version 4.0.0-beta3-SNAPSHOT
15:01:13.593 [${artifactId}-admin-0] INFO  c.d.o.d.internal.core.time.Clock - Using native clock for microsecond precision
15:01:13.814 [${package}.Main.main()] INFO  ${package}.Main - Cassandra release version: 3.11.3
```

You will be left at a simple prompt that supports some basic CQL queries and commands.

```
This is a CQL demo. See https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlCommandsTOC.html for more info on CQL commands. Type 'EXIT' to quit.

cql-demo>
```

Please refer to the [cql command docs][1] for more on CQL queries and syntax.

### Customizing the Driver

#### Driver Configuration

A basic `application.conf` file is included in the `resources` directory of the `core` project. This
file can be modified to customize driver configuration to suit your needs. Please reference the
[Java Driver configuration documentation][2] for configuring this file.

[1]: https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlCommandsTOC.html
[2]: https://docs.datastax.com/en/developer/java-driver/4.0-beta/manual/core/configuration/