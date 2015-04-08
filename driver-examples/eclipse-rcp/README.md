# Java Driver examples

This project demonstrates how to integrate the Java Driver in an Eclipse RCP application
built with Maven and Tycho.

## Packaging the Java Driver in an Eclipse RCP application

The Java Driver is built and distributed as an OSGi bundle in Maven Central. All of its
dependencies are also available as OSGi bundles.

This example application leverages Tycho's ability to resolve OSGi bundles from
Maven artifacts. The dependency to the driver is declared as a regular Maven dependency
in the pom file.

However, to be able to open and run this application in Eclipse, you will have to create a target
platform that includes the driver and its dependencies, and then compile the project against that
platform.

If your project/company benefits from a dedicated P2 repository, the simplest way to achieve this
is to deploy the driver bundle there, along with its dependencies, 
and create a target platform definition with Eclipse that includes that P2 repository.

Alternatively, it is also possible to "wrap" the Java Driver jar, along with its dependencies,
in a plugin submodule in your application.

## Installing/Running with Tycho

RUN CCM

Build the parent project `com.datastax.driver.examples.rcp`; use `mvn verify` to package and run the tests
(by default, tests with Tycho are run in the integration-test phase, because a full OSGi container
is launched and tests are run inside that container) :

    cd com.datastax.driver.examples.rcp
    mvn verify
    
## Installing/Running with Eclipse

### Importing the projects

1. Import the projects via File -> Import... -> Maven -> Existing Maven projects.
2. Click 'Next';
3. Click 'Browse...': point to the `eclipse-rcp` folder that contains all the Eclipse RCP projects to import;
4. Click 'Finish'.

Check that all projects have been correctly imported:

1. All projects should have the Maven nature;
2. All projects but the parent should have the PDE nature (plug-in project) and the Java nature.


VERIFIER resources

If you have compilation errors, see below.

### Creating a target platform

If you do not have yet a target platform that includes the Java driver as a plugin,
you might see compilation errors in some projects.

To create a suitable target platform, do the following steps:

1. Download version 3.0.0 of the driver from http://downloads.datastax.com/java-driver;
2. Unpack the binary tarball;
3. Assemble the driver jar and its dependencies (found under /lib subdirectory) into a single place;
EXCLUDE SLF4J
4. In Eclipse, go to Preferences... -> Plug-in Development -> Target Platform
5. Click on 'Add...';
6. Choose 'Default' and click 'Next';
7. Name your target platform 'Driver RCP Example Target Platform';
8. Under the Locations tab, click on Add...;
9. Choose 'Directory' and click 'Next';
10. Click on 'Browse...' and point the location to the directory where you placed the driver jar with its dependencies;
11. Click 'Next' and check the list of found plugins: you should see 16 plug-ins in total;
12. Click 'Finish' and click 'Finish' again;
13. Check the newly-created target platform and make sure it becomes the active target platform;
14. Click 'Ok'.

### Running the tests

To run the tests, right-click on `MailboxServiceTest` and choose Run... -> JUnit Plugin Test.

Please note: the tests require an OSGi container; you cannot run them as regular JUnit tests.

### Troubleshooting

1) Can't see driver logs when running tests with Eclipse or logs are in DEBUG level.

Add the following VM parameter to your test launch configuration:

    -Dlogback.configurationFile=${workspace_loc:com.datastax.driver.examples.rcp.ui}/logback.xml
    
To edit your test launch configuration, choose 'Run -> Edit Configurations...' then 
go to the 'Arguments' tab, then enter the appropriate arguments under VM Arguments.

