/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.examples.basic;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Connects to a Cassandra cluster and extracts basic information from it.
 *
 * <p>Preconditions:
 *
 * <ul>
 *   <li>An Apache Cassandra(R) cluster is running and accessible through the contacts points
 *       identified by basic.contact-points (see application.conf).
 * </ul>
 *
 * <p>Side effects: none.
 *
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/4.0">Java Driver online
 *     manual</a>
 */
public class ReadCassandraVersion {

  public static void main(String[] args) {

    // The Session is what you use to execute queries. It is thread-safe and should be
    // reused.
    try (CqlSession session = CqlSession.builder().build()) {
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
