# Cassandra Java Driver Archetype Project

If you are reading this, you've successfully created the Cassandra Java Driver archetype project.

## Running the project

### Ensure Cassandra is running

When generating this project, values for `cassandra-host` and `cassandra-port` were provided. The
default values are `127.0.0.1` (localhost) and `9042` respectively. Ensure that Cassandra is up and
running on that host and port. If the Cassandra host/port to which you want to connect has changed,
or was not set correctly when the project was generated, you will have to edit the `Main.java`
source file and alter this line accordingly:

```
builder.addContactPoint(InetSocketAddress.createUnresolved("127.0.0.1", 9042));
```

### Compiling and running the example

Once you have a local Cassandra running (or have changed `Main.java` to point to some other instance
that is running), you can build and run this project by executing the following:

```
mvn compile exec:java -Dexec.mainClass=com.mycompany.group.Main
```

where `com.mycompany.group` is the value you used when generating the project from the archetype. If
you didn't override the default, the package should be the same as the `groupId`.

Running this example project should connect to Cassandra and log out the version:

```
15:01:13.319 [com.mycompany.group.Main.main()] INFO  c.d.o.d.i.c.DefaultMavenCoordinates - DataStax Java driver for Apache Cassandra(R) (com.datastax.oss:java-driver-core) version 4.0.0-beta3-SNAPSHOT
15:01:13.593 [myArtifact-admin-0] INFO  c.d.o.d.internal.core.time.Clock - Using native clock for microsecond precision
15:01:13.814 [com.mycompany.group.Main.main()] INFO  com.mycompany.group.Main - Cassandra release version: 3.11.3
```

### Customizing the Driver

#### Driver Configuration

A basic `application.conf` file is included in the `resources` directory of this project. This file
can be modified to customize driver configuration to suit your needs. Please reference the
[Java Driver configuration documentation][1] for configuring this file.

#### Logging framework

By default, this project is setup to use SLF4J for logging, with `logback-classic` as the
implementation. If you would prefer to use `log4j`, simply provide `-Dlogging-log4j` when you run
the project:

```
mvn compile exec:java -Dexec.mainClass=com.mycompany.group.Main -Dlogging=log4j
```

Both `logback-classic` and `log4j` example configuration files are included in `src/main/resources/`
of this project. Feel free to adjust them to your needs.

[1]: https://docs.datastax.com/en/developer/java-driver/4.0-beta/manual/core/configuration/