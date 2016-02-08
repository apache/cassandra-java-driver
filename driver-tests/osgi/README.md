# OSGi Example

A simple test for the Java Driver in an OSGi environment.

MailboxService is an activated service that uses Cassandra to
persist a Mailbox by email address.

## Usage

To build the bundle and run tests execute the following maven task:

    mvn integration-test -P short

The short profile needs to be activated since the tests run under
this group.

After which the bundle jar will be present in the target/ directory.

The project includes an integration test that verifies the service can
be activated and used in an OSGi container.  It also verifies that
driver-core can be used in an OSGi container in the following
configurations:

1. Default (default classifier with all dependencies)
2. Netty-Shaded (shaded classifier with all depedencies w/o Netty)
4. Guava 16
5. Guava 17
6. Guava 18
