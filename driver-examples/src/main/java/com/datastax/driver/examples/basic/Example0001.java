/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.examples.basic;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example shows how to connect to a Cassandra cluster and
 * extract some basic information from it.
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class Example0001 {

    private static final Logger logger = LoggerFactory.getLogger("com.datastax.driver.examples");

    public static void main(String[] args) {

        /*
         * (1) The Cluster object is the main entry point of the driver.         *
         * It holds the known state of the actual Cassandra cluster (notably the Metadata).
         * This class is thread-safe, you should create a single instance (per target Cassandra cluster),
         * and share it throughout your application;
         *
         * (2) The Session is what you use to execute queries. Likewise, it is thread-safe and should be reused;
         *
         * (3) We use execute to send a query to Cassandra. This returns a ResultSet,
         * which is essentially a collection of Row objects. On the next line, we extract the first row (which is the only one in this case);
         *
         * (4) We extract the value of the first (and only) column from the row;
         *
         * (5) Finally, we close the cluster after we’re done with it. This will also close any session that was created from this cluster.
         * This step is important because it frees underlying resources (TCP connections, thread pools…).
         * In a real application, you would typically do this at shutdown (for example, when undeploying your webapp).
         */

        Cluster cluster = null;

        try {

            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .build();

            Session session = cluster.connect();                                           // (2)

            ResultSet rs = session.execute("select release_version from system.local");    // (3)
            Row row = rs.one();

            String releaseVersion = row.getString("release_version");
            logger.info("Cassandra version is: {}", releaseVersion);                      // (4)

        } finally {

            if (cluster != null)
                cluster.close();                                                          // (5)

        }

    }

}
