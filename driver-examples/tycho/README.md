# Java Driver examples

This project demonstrates how to integrate the Java Driver in an Eclipse RCP application
built with Maven and Tycho.

## Packaging the Java Driver in an Eclipse RCP application

The Java Driver is built and distributed as an OSGi bundle, but integrating it in an RCP application is not trivial.

If you are using a "manifest-first" approach and your project/company benefits from a dedicated P2 update site, it is best to place the driver bundle, 
along with its dependencies, in your update site, and have Tycho resolve the dependencies from there.

Alternatively, it is also possible to "wrap" the Java Driver jar, along with its dependencies,
in a plugin submodule in your application, and have Tycho build it along with your application.

The current project uses a third option: a "pom-first" approach, where the driver dependencies are pulled directly from Maven repositories.
For this to work, Tycho has been configured to consider pom dependencies when resolving dependencies.

## Installing/Running with Tycho

To build the project and run the tests, simply run

    cd com.datastax.driver.examples.rcp
    mvn integration-test
    
Please note: currently the tests require an instance of Cassandra running on localhost:9042.
    
## Installing/Running with Eclipse RCP

In order for Eclipse to be able to locate the Java Driver and its dependencies, this project
provides a special submodule, `com.datastax.driver.examples.rcp.repository`, that must be built 
with Tycho at least once. 

This submodule creates a local P2 installation under 
`com.datastax.driver.examples.rcp.repository/target/repository` containing the driver bundle and its dependencies.
(The Tycho build itself does not require this P2 installation to be available since dependencies
are resolved directly from Maven repositories - it' really only for Eclipse.)

Installation steps:

1. Build the project with Tycho at least once in order for the local P2 installation to be created.
2. Import the projects via File -> Import... -> Existing projects into Workspace.
3. Set the JDK compliance level for all projects to 1.7.
4. Open the file  `driver-examples-rcp.target` and make it the active 
target platform for your workspace by clicking on "Set as Target Platform".

To run the tests, right-click on `MailboxServiceTest` and choose Run... -> JUnit Plugin Test.
Please note: currently the tests require an instance of Cassandra running on localhost:9042.
