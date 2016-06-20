# OSGi Tests

A collection of simple tests for the Java Driver in an OSGi environment.

This project is mainly intended as a tool for validating
that each new release of the driver is fully OSGi-compatible. 
It is _not_ meant as an example application.

If you are looking for examples demonstrating usage of the driver in an OSGi
environment, please refer to our [OSGi examples repository].

[OSGi examples repository]:https://github.com/datastax/java-driver-examples-osgi

## Usage

`MailboxService` is an OSGi service that uses Cassandra to
store messages that can be retrieved by email address.

To build the bundle and run tests, execute the following Maven goal:

    mvn verify -P short

The "short" profile needs to be activated since the tests run under
this group.

Note: tests will try to load the jars of 3 dependent modules:
`driver-core`, `driver-mapping` and `driver-extras`. 
For this to succeed, you need to run `mvn package` 
first for these modules and make sure the jars are present
in each module's `target/` subdirectory.

Once `mvn verify` completes, the bundle jar will be present in the `target/` directory.

The project includes integration tests that verify the service can
be activated and used in an OSGi container.  It also verifies that
the Java driver can be used in an OSGi container in the following
configurations:

1. Default (default classifier with all dependencies)
2. Netty-Shaded (shaded classifier with all dependencies w/o Netty)
5. Guava 17
6. Guava 18
7. Guava 19
