# Java Driver examples

This project demonstrates how to integrate the Java Driver in an Eclipse RCP application
built with Maven and Tycho.

## Packaging the Java Driver in an Eclipse RCP application

The Java Driver is built and distributed as an OSGi bundle, but integrating it in an RCP application is not trivial.

If your project/company benefits from a dedicated P2 update site, it is best to place the driver bundle, 
along with its dependencies, in your update site, and have Tycho resolve the dependencies from there.

Alternatively, it is also possible to "wrap" the Java Driver jar, along with its dependencies,
in a plugin submodule in your application, and have Tycho build it along with your application.

The current project uses a third option: using the [p2-maven-plugin](http://projects.reficio.org/p2-maven-plugin),
it is possible to build a local P2 installation directory, then use Jetty to serve it.

## Creating the local P2 installation

First, use the standalone project `com.datastax.driver.examples.repository` to create the local P2 installation,
then launch Jetty to serve it:

    cd com.datastax.driver.examples.repository
    mvn p2:site
    mvn jetty:run

## Installing/Running with Tycho
    
Then, in another terminal, build the project and run the tests from the reactor project `com.datastax.driver.examples.rcp` :

    cd ../com.datastax.driver.examples.rcp
    mvn integration-test
    
Please note: currently the tests require an instance of Cassandra running on localhost:9042.
    
## Installing/Running with Eclipse RCP

Installation steps:

1. Import the projects via File -> Import... -> Existing projects into Workspace.
2. Set the JDK compliance level for all projects to 1.7.
3. Open the file `com.datastax.driver.examples.rcp.platform.target` and make it the active 
target platform for your workspace by clicking on "Set as Target Platform".

To run the tests, right-click on `MailboxServiceTest` and choose Run... -> JUnit Plugin Test.

Alternatively, you can use the provided launch configuration:
`Mailbox Service Tests.launch`

Please note: currently the tests require an instance of Cassandra running on localhost:9042.
