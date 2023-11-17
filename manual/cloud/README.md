<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Connecting to Astra (Cloud)

Using the Java Driver to connect to a DataStax Astra database is almost identical to using
the driver to connect to any normal Apache Cassandra® database. The only differences are in how the
driver is configured in an application and that you will need to obtain a *secure connect bundle*.

### Prerequisites

1. [Download][Download Maven] and [install][Install Maven] Maven.
2. Create an Astra database on [AWS/Azure/GCP][Create an Astra database - AWS/Azure/GCP];
   alternatively, have a team member provide access to their
   Astra database (see instructions for [AWS/Azure/GCP][Access an Astra database - AWS/Azure/GCP]) to
   obtain database connection details.
3. Download the secure connect bundle (see instructions for 
   [AWS/Azure/GCP][Download the secure connect bundle - AWS/Azure/GCP]) that contains connection
   information such as contact points and certificates.

### Procedure

Create a minimal project structure as explained [here][minimal project structure]. Then modify
`Main.java` using one of the following approaches:

#### Programmatic configuration

You can pass the connection information directly to `CqlSession.builder()`:

```java
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.nio.file.Paths;

public class Main {
  
    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder()
            // make sure you change the path to the secure connect bundle below
            .withCloudSecureConnectBundle(Paths.get("/path/to/secure-connect-database_name.zip"))
            .withAuthCredentials("user_name","password")
            .withKeyspace("keyspace_name")
            .build()) {

                // For the sake of example, run a simple query and print the results
                ResultSet rs = session.execute("select release_version from system.local");
                Row row = rs.one();
                if (row != null) {
                    System.out.println(row.getString("release_version"));
                } else {
                    System.out.println("An error occurred.");
                }
           }
        }
    }
```

The path to the secure connect bundle for your Astra database is specified with
`withCloudSecureConnectBundle()`. The authentication credentials must be specified separately with
`withAuthCredentials()`, and match the username and password that were configured when creating the
Astra database.

Note the following:

* an SSL connection will be established automatically. Manual SSL configuration is not allowed, any
  settings in the driver configuration (`advanced.ssl-engine-factory`) will be ignored;
* the secure connect bundle contains all of the necessary contact information. Specifying contact
  points manually is not allowed, and will result in an error;
* if the driver configuration does not specify an explicit consistency level, it will default to
  `LOCAL_QUORUM` (instead of `LOCAL_ONE` when connecting to a normal Cassandra database). 

#### File-based configuration

Alternatively, the connection information can be specified in the driver's configuration file
(`application.conf`). Merge the following options with any content already present:
      
```properties
datastax-java-driver {
  basic {
    # change this to match the target keyspace
    session-keyspace = keyspace_name
    cloud {
      # change this to match bundle's location; can be either a path on the local filesystem
      # or a valid URL, e.g. http://acme.com/path/to/secure-connect-database_name.zip
      secure-connect-bundle = /path/to/secure-connect-database_name.zip
    }
  }
  advanced {
    auth-provider {
      class = PlainTextAuthProvider
      # change below to match the appropriate credentials
      username = user_name 
      password = password
    }
  }
}
```

For more information about the driver configuration mechanism, refer to the [driver documentation].
    
With the above configuration, your main Java class can be simplified as shown below:
    
```java
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
    
public class Main {
    
  public static void main(String[] args) {
    // Create the CqlSession object; it will read the configuration file and pick the right
    // values to connect to the Astra database.
    try (CqlSession session = CqlSession.builder().build()) {

      ResultSet rs = session.execute("select release_version from system.local");
      Row row = rs.one();
      if (row != null) {
        System.out.println(row.getString("release_version"));
      } else {
        System.out.println("An error occurred.");
      }
    }
  }
}
```

[Download Maven]: https://maven.apache.org/download.cgi
[Install Maven]: https://maven.apache.org/install.html
[Create an Astra database - AWS/Azure/GCP]: https://docs.datastax.com/en/astra/docs/creating-your-astra-database.html
[Access an Astra database - AWS/Azure/GCP]: https://docs.datastax.com/en/astra/docs/obtaining-database-credentials.html#_sharing_your_secure_connect_bundle
[Download the secure connect bundle - AWS/Azure/GCP]: https://docs.datastax.com/en/astra/docs/obtaining-database-credentials.html
[minimal project structure]: ../core/integration/#minimal-project-structure
[driver documentation]: ../core/configuration/
