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

Before running the tests, there are some prerequisites that must be met:

The tests in this module will try to locate and load the following artifacts:

1. The jars of 3 dependent modules:
   `driver-core`, `driver-mapping` and `driver-extras`. 
   These are expected to be found in their respective `target` directory;
2. The test-jar for `driver-core`. Tests will attempt to load it from your local Maven repository.

Therefore, before you can launch the tests, it is required to run `mvn install` 
first _on the entire project_ to make sure all the above artifacts will be present.

If the above prerequisites are met, then it is possible to build 
this project and run its tests by executing the following Maven goal:

    mvn verify -P short

The "short" profile needs to be activated since the tests run under
this group.

Once `mvn verify` completes, the bundle jar will be present in the `target/` directory.

The project includes integration tests that verify that the service can
be activated and used in an OSGi container.  It also verifies that
the Java driver can be used in an OSGi container in the following
configurations:

1. Default (default classifier with all dependencies)
2. Netty-Shaded (shaded classifier with all dependencies w/o Netty)
5. Guava 17
6. Guava 18
7. Guava 19
