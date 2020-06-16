/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.examples.astra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.nio.file.Paths;

/**
 * Connects to a DataStax Astra cluster and extracts basic information from it.
 *
 * <p>Preconditions:
 *
 * <ul>
 *   <li>A DataStax Astra cluster is running and accessible.
 *   <li>A DataStax Astra secure connect bundle for the running cluster.
 * </ul>
 *
 * <p>Side effects: none.
 *
 * @see <a
 *     href="https://docs.datastax.com/en/astra/gcp/doc/dscloud/astra/dscloudGettingStarted.html#dscloudCreateCluster">
 *     Creating an Astra Database (GCP)</a>
 * @see <a
 *     href="https://docs.datastax.com/en/astra/gcp/doc/dscloud/astra/dscloudShareClusterDetails.html">
 *     Providing access to Astra databases (GCP)</a>
 * @see <a
 *     href="https://docs.datastax.com/en/astra/gcp/doc/dscloud/astra/dscloudObtainingCredentials.html">
 *     Obtaining Astra secure connect bundle (GCP)</a>
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest">Java driver online
 *     manual</a>
 */
public class AstraReadCassandraVersion {

  public static void main(String[] args) {

    // The Session is what you use to execute queries. It is thread-safe and should be
    // reused.
    try (CqlSession session =
        CqlSession.builder()
            // Change the path here to the secure connect bundle location (see javadocs above)
            .withCloudSecureConnectBundle(Paths.get("/path/to/secure-connect-database_name.zip"))
            // Change the user_name and password here for the Astra instance
            .withAuthCredentials("user_name", "fakePasswordForTests")
            // Uncomment the next line to use a specific keyspace
            // .withKeyspace("keyspace_name")
            .build()) {

      // We use execute to send a query to Cassandra. This returns a ResultSet, which
      // is essentially a collection of Row objects.
      ResultSet rs = session.execute("select release_version from system.local");
      //  Extract the first row (which is the only one in this case).
      Row row = rs.one();

      // Extract the value of the first (and only) column from the row.
      assert row != null;
      String releaseVersion = row.getString("release_version");
      System.out.printf("Cassandra version is: %s%n", releaseVersion);
    }
    // The try-with-resources block automatically close the session after we’re done with it.
    // This step is important because it frees underlying resources (TCP connections, thread
    // pools...). In a real application, you would typically do this at shutdown
    // (for example, when undeploying your webapp).
  }
}
