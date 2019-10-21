## Connecting to Apollo (Cloud)

Using the DataStax Java Driver to connect to a DataStax Apollo database is almost identical to using
the driver to connect to any normal Apache Cassandra® database. The only differences are in how the
driver is configured in an application and that you will need to obtain a `secure connect bundle`.

The following is a Quick Start guide to writing a simple application that can connect to an Apollo
database.

   **Tip**: DataStax recommends using the DataStax Java Driver for Apache Cassandra. You can also
   use the DataStax Enterprise (DSE) Java Driver, which exposes the same API for connecting to
   Cassandra databases.

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

### Procedure

1. Edit the `pom.xml` file at the root of your and according to this [Example pom.xml file].

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

      **Note:** With the `CqlSession.builder()` object, make sure to set the path to the secure
      connect bundle for your Apollo database (**"/path/to/secure-connect-database_name.zip"**) in
      the `withCloudSecureConnectBundle()` method as shown in the following example.  
      If converting from using the open source Cassandra Java Driver to the DSE Java Driver, ensure
      that you change `CqlSession` to `DseSession`.
      * DataStax Java Driver for Apache Cassandra 4.x (recommended)

          ```java
          import com.datastax.oss.driver.api.core.CqlSession;
          import com.datastax.oss.driver.api.core.cql.ResultSet;
          import com.datastax.oss.driver.api.core.cql.Row;
          import java.nio.file.Paths;
          
          public class ConnectDatabase {
          
          public static void main(String[] args) {
              // Create the CqlSession object:
              try (CqlSession session = CqlSession.builder()
                  // make sure you change the path to the secure connect bundle below
                  .withCloudSecureConnectBundle(Paths.get("/path/to/secure-connect-database_name.zip"))
                  .withAuthCredentials("user_name","password")
                  .withKeyspace("keyspace_name")
                  .build()) {
                      // Select the release_version from the system.local table:
                      ResultSet rs = session.execute("select release_version from system.local");
                      Row row = rs.one();
                      //Print the results of the CQL query to the console:
                      if (row != null) {
                          System.out.println(row.getString("release_version"));
                      } else {
                          System.out.println("An error occurred.");
                      }
                 }
              }
          }
          ```
      * DSE Java 2.x

          ```java
          import com.datastax.dse.driver.api.core.DseSession;
          import com.datastax.oss.driver.api.core.cql.ResultSet;
          import com.datastax.oss.driver.api.core.cql.Row;
          import java.nio.file.Paths;

          public class ConnectDatabase {

             public static void main(String[] args) {
                 // Create the DseSession object:
                 try (DseSession session = DseSession.builder()
                     // make sure you change the path to the secure connect bundle below
                     .withCloudSecureConnectBundle(Paths.get("/path/to/secure-connect-database_name.zip"))
                     .withAuthCredentials("user_name","password")
                     .withKeyspace("keyspace_name")
                     .build()) {
                     // Select the release_version from the system.local table:
                     ResultSet rs = session.execute("select release_version from system.local");
                     Row row = rs.one();
                     //Print the results of the CQL query to the console:
                     if (row != null) {
                         System.out.println(row.getString("release_version"));
                     } else {
                         System.out.println("An error occurred.");
                     }
                 }
             }
          }
          ```

    c. Save and close the ConnectDatabase.java file.

[Download Maven]: https://maven.apache.org/download.cgi
[Install Maven]: https://maven.apache.org/install.html
[Create an Apollo database - GCP]: https://helpdocs.datastax.com/gcp/dscloud/apollo/dscloudGettingStarted.html#dscloudCreateCluster
[Create an Apollo database - AWS]: https://helpdocs.datastax.com/aws/dscloud/apollo/dscloudGettingStarted.html#dscloudCreateCluster
[Access an Apollo database - GCP]: https://helpdocs.datastax.com/gcp/dscloud/apollo/dscloudShareClusterDetails.html
[Access an Apollo database - AWS]: https://helpdocs.datastax.com/aws/dscloud/apollo/dscloudShareClusterDetails.html
[Download the secure connect bundle - GCP]: https://helpdocs.datastax.com/gcp/dscloud/apollo/dscloudObtainingCredentials.html
[Download the secure connect bundle - AWS]: https://helpdocs.datastax.com/aws/dscloud/apollo/dscloudObtainingCredentials.html
[Example pom.xml file]: ../core/integration/#minimal-project-structure
