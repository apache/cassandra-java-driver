## Connecting to Apollo (Cloud)

Using the DataStax Java Driver to connect to a DataStax Apollo database is almost identical to using
the driver to connect to any normal Apache Cassandra® database. The only differences are in how the
driver is configured in an application and that you will need to obtain a `secure connect bundle`.

The following is a Quick Start guide to writing a simple application that can connect to an Apollo
database.

### Prerequisites

1. [Download][Download Maven] and [install][Install Maven] Maven.
1. Create an Apollo database on [GCP][Create an Apollo database - GCP] or
   [AWS][Create an Apollo database - AWS]; alternatively, have a team member provide access to their
   Apollo database (instructions for [GCP][Access an Apollo database - GCP] and 
   [AWS][Access an Apollo database - AWS]) to obtain database connection details.
1. Download the secure connect bundle (instructions for
   [GCP][Download the secure connect bundle - GCP] and 
   [AWS][Download the secure connect bundle - AWS]) to obtain connection credentials for your 
   database.
1. Ensure you are using Java 8 or higher. The cloud connect api does not support java 6 or 7.

### Procedure

1. Include the driver artifacts in your `pom.xml` file according to this [pom.xml dependency].

1. Initialize the DataStax Java Driver.

    a. Create a `ConnectDatabase.java` file in the `/src/main/java` directory for your Java project.

      ```sh
      $ cd javaProject/src/main/java
      ```
      ```sh
      $ touch ConnectDatabase.java
      ```

    b. Copy the following code for your DataStax Driver into the `ConnectDatabase.java` file.  
    The following example implements a `ConnectDatabase` class to connect to your Apollo database,
    runs a CQL query, and prints the output to the console.

      **Note:** With the `Cluster.builder()` object, make sure to set the path to the secure
      connect bundle for your Apollo database (**"/path/to/secure-connect-database_name.zip"**) in
      the `withCloudSecureConnectBundle()` method as shown in the following example.  
      * DataStax Java Driver for Apache Cassandra 3.x

          ```java
          import com.datastax.driver.core.Cluster;
          import com.datastax.driver.core.ResultSet;
          import com.datastax.driver.core.Row;
          import com.datastax.driver.core.Session;
          import java.io.File;

          public class ConnectDatabase {

            public static void main(String[] args) {
              // Create the Cluster object:
              Cluster cluster = null;
              try {
                cluster = Cluster.builder()
                  // make sure you change the path to the secure connect bundle below
                  .withCloudSecureConnectBundle(new File("/path/to/secure-connect-database_name.zip"))
                  .withCredentials("user_name", "password")
                  .build();
                Session session = cluster.connect();
                // Select the release_version from the system.local table:
                ResultSet rs = session.execute("select release_version from system.local");
                Row row = rs.one();
                //Print the results of the CQL query to the console:
                if (row != null) {
                  System.out.println(row.getString("release_version"));
                } else {
                  System.out.println("An error occurred.");
                }
              } finally {
                if (cluster != null) cluster.close();
              }
            }
          }
          ```

    c. Save and close the ConnectDatabase.java file.

### Apollo Differences

In most circumstances, the client code for interacting with an Apollo cluster will be the same as
interacting with any other Cassandra cluster. The exceptions being:

  * An SSL connection will be established automatically. Manual SSL configuration is not necessary.

  * A Cluster’s contact points attribute should not be used. The cloud config contains all of the
  necessary contact information (i.e. don't use any of the `addContactPoint()` or
  `addContactPoints()` methods on the Builder)

  * If a consistency level is not specified for an execution profile or query, then
  `ConsistencyLevel.LOCAL_QUORUM` will be used as the default.

[Download Maven]: https://maven.apache.org/download.cgi
[Install Maven]: https://maven.apache.org/install.html
[Create an Apollo database - GCP]: https://helpdocs.datastax.com/gcp/dscloud/apollo/dscloudGettingStarted.html#dscloudCreateCluster
[Create an Apollo database - AWS]: https://helpdocs.datastax.com/aws/dscloud/apollo/dscloudGettingStarted.html#dscloudCreateCluster
[Access an Apollo database - GCP]: https://helpdocs.datastax.com/gcp/dscloud/apollo/dscloudShareClusterDetails.html
[Access an Apollo database - AWS]: https://helpdocs.datastax.com/aws/dscloud/apollo/dscloudShareClusterDetails.html
[Download the secure connect bundle - GCP]: https://helpdocs.datastax.com/gcp/dscloud/apollo/dscloudObtainingCredentials.html
[Download the secure connect bundle - AWS]: https://helpdocs.datastax.com/aws/dscloud/apollo/dscloudObtainingCredentials.html
[pom.xml dependency]: ../../#getting-the-driver
