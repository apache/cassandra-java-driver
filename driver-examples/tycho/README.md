# Java Driver examples

This project demonstrates how to integrate the Java Driver in an Eclipse RCP application
built with Maven and Tycho.

## Packaging the Java Driver in an Eclipse RCP application

The Java Driver is built and distributed as an OSGi bundle, but integrating it in an RCP application is not trivial.

If your project/company benefits from a dedicated P2 update site, it is best to place the driver bundle, 
along with its dependencies, in your update site, and have Tycho resolve the dependencies from there.

Alternatively, it is also possible to "wrap" the Java Driver jar, along with its dependencies,
in a plugin submodule in your application, and have Tycho build it along with your application.

The current project uses a third option: with the [p2-maven-plugin](http://projects.reficio.org/p2-maven-plugin),
it is possible to build a local P2 installation directory, then use Jetty to publish it.

## Prerequisites

In order to run the tests, please ensure that the following prerequisites are met:

1. You need a recent version of [CCM](https://github.com/pcmanus/ccm).
2. The CCM executable must be available in your `PATH` environment variable.

The tests will use CCM to launch a single-node Cassandra cluster (see `MailboxTestsActivator`).
The node will attempt to bind to the addreess 127.0.1.1 on port 9042, please ensure that this interface and port are available.

On a Mac, you can bring this interface up by running the following command:

    sudo ifconfig lo0 alias 127.0.1.1 up

## Creating the local P2 installation

Use the standalone project `com.datastax.driver.examples.repository` to create the local P2 installation,
then launch Jetty to publish it:

    cd com.datastax.driver.examples.repository
    mvn p2:site
    mvn jetty:run

The P2 update site must be published when you build the project with Tycho.

## Installing/Running with Tycho
    
Once the P2 update site is built and published, in another terminal, build the reactor project `com.datastax.driver.examples.rcp` to run the tests:

    cd com.datastax.driver.examples.rcp
    mvn integration-test
    
    
## Installing/Running with Eclipse

Installation steps:

1. Import the projects via File -> Import... -> Existing projects into Workspace.
2. Set the JDK compliance level for all projects to 1.7.
3. Open the file `com.datastax.driver.examples.rcp.platform.target` and make it the active 
target platform for your workspace by clicking on "Set as Target Platform".

To run the tests, use the provided launch configuration:
`Mailbox Service Tests.launch`.

Alternatively, you can right-click on `MailboxServiceTest` and choose Run... -> JUnit Plugin Test.

Please note: the tests require an OSGi container; you cannot run them as regular JUnit tests.

## Troubleshooting

1) The tests fail with the following error:

    Caused by: java.io.IOException: Cannot run program "ccm": error=2, No such file or directory

Adjust your `PATH` variable so that the CCM executable can be found. In Eclipse, edit your launch configuration,
go to the Environment tab, and add (or adjust) the `PATH` environment variable.

2) The tests fail with the following error:

    ccmlib.common.UnavailableSocketError: Inet address 127.0.1.1:9042 is not available: [Errno 49] Can't assign requested address 
    
Make sure the address 127.0.1.1 and port 9042 are both available.

3) Can't see driver logs during tests.

Add the following VM parameter to your launch configuration:

    -Dlogback.configurationFile=/path/to/logback.xml
    
In Eclipse, edit your test launch configuration, got to the Arguments tab, then enter the appropriate arguments under VM Arguments.

4) Tycho build fails with the following error:

    [WARNING] Failed to access p2 repository http://localhost:8080/site, use local cache. Unable to connect to repository http://localhost:8080/site/artifacts.xml
    
Make sure your local P2 installation has been properly built and that it is currently published by a Jetty server at http://localhost:8080.

5) Eclipse is unable to load the target platform definition file and displays the following error:

    The target definition did not update successfully
    Unable to connect to repository http://localhost:8080/site/content.xml

Make sure your local P2 installation has been properly built and that it is currently published by a Jetty server at http://localhost:8080.

