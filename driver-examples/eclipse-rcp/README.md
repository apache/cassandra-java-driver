# Java Driver examples

This project demonstrates how to integrate the Java Driver in an Eclipse RCP application
built with Maven and Tycho.

The Java Driver is built and distributed as an OSGi bundle in Maven Central. All of its
mandatory dependencies are also available as OSGi bundles.

This example application leverages Tycho's ability to resolve OSGi bundles from
Maven repositories. The dependency to the driver is declared as a regular Maven dependency
in the pom file.

## Installing/Running with Tycho

Important: the tests assume that there is a Cassandra node listening on 127.0.0.1:9042.
You can use [CCM](https://github.com/pcmanus/ccm) to start a local node.

To package the projects and run the tests, simply build the parent project using `mvn verify`
(by default, tests with Tycho are run in the integration-test phase, because a full OSGi container
is launched and tests are run inside that container).
    
## Installing/Running with Eclipse

### Creating a target platform

To be able to open and run this application in Eclipse, you will first have to create a target
platform that includes the driver bundle and its dependencies.

1) If your project/company benefits from a hosted P2 repository, the simplest way to achieve this
is to deploy the driver bundle there, along with its dependencies, 
and create a target platform definition that includes that P2 repository.

2) Otherwise, you can create a target platform that includes the driver bundle in a local
folder on your machine. To create such a target platform, do the following steps:

1. Download version 3.0.0 of the driver from http://downloads.datastax.com/java-driver;
2. Unpack the binary tarball;
3. Assemble the driver jar and its dependencies (found under /lib subdirectory) into a single place;
4. In Eclipse, go to Preferences... -> Plug-in Development -> Target Platform
5. Click on 'Add...';
6. Choose 'Default' and click 'Next';
7. Name your target platform 'Driver Eclipse RCP Example Target Platform';
8. Under the Locations tab, click on Add...;
9. Choose 'Directory' and click 'Next';
10. Click on 'Browse...' and point the location to the directory where you placed the driver jar with its dependencies;
11. Click 'Next' and check the list of found plugins: you should see 16 plug-ins in total;
12. Click 'Finish' and click 'Finish' again;
13. Check the newly-created target platform and make sure it becomes the active target platform;
14. Click 'Ok'.

3) A third alternative is to "wrap" the Java Driver jar, along with its dependencies,
in a standalone plugin project, then include that plugin as a bundle requirement in your application.

### Importing the projects

1. Import the projects via File -> Import... -> Maven -> Existing Maven projects.
2. Click 'Next';
3. Click 'Browse...': point to the `eclipse-rcp` folder that contains all the Eclipse RCP projects to import;
4. Click 'Finish'.

If it is the first time you are importing Maven/Tycho projects into your Eclipse workspace,
Eclipse will prompt you to install additional Maven plugin connectors to handle the Maven Tycho plugin.

Check that all projects have been correctly imported:

1. All projects should have the Maven nature;
2. All projects but the parent should have the PDE nature (plug-in project) and the Java nature;
3. There should be no compilation errors.

If you have compilation errors, see the Troubleshooting section below.

### Running the tests

To run the tests, go to Run... -> Run Configurations... -> JUnit Plug-in Test -> MailboxServiceTests,
then click on 'Run'.

Please note: the tests require an OSGi container; you cannot run them as regular JUnit tests.

### Troubleshooting

1) I see compilation errors in some projects in Eclipse.

This is certainly due to a bad target platform definition. Check your active target platform
definition and make sure it includes all the required software: Eclipse runtime plug-ins and,
of course, the driver plug-in.

2) Can't see driver logs when running tests with Eclipse, or logs are in DEBUG level.

Add the following VM parameter to your test launch configuration:

    -Dlogback.configurationFile=${resource_loc:/com.datastax.driver.examples.rcp.mailbox.tests/src/main/resources/logback.xml}
    
To edit your test launch configuration, choose 'Run -> Edit Configurations...' then 
go to the 'Arguments' tab, then enter the appropriate arguments under VM Arguments.

