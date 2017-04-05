/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.examples.basic;

import com.datastax.driver.core.*;

/**
 * Gathers information about a Cassandra cluster's topology (which nodes belong to the cluster) and schema (what
 * keyspaces, tables, etc. exist in this cluster).
 * <p/>
 * Preconditions:
 * - a Cassandra cluster is running and accessible through the contacts points identified by CONTACT_POINTS and PORT.
 * <p/>
 * Side effects: none.
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class ReadTopologyAndSchemaMetadata {

    static String[] CONTACT_POINTS = {"127.0.0.1"};
    static int PORT = 9042;

    public static void main(String[] args) {

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoints(CONTACT_POINTS).withPort(PORT)
                    .build();

            Metadata metadata = cluster.getMetadata();
            System.out.printf("Connected to cluster: %s%n", metadata.getClusterName());

            for (Host host : metadata.getAllHosts()) {
                System.out.printf("Datatacenter: %s; Host: %s; Rack: %s%n",
                        host.getDatacenter(), host.getAddress(), host.getRack());
            }

            for (KeyspaceMetadata keyspace : metadata.getKeyspaces()) {
                for (TableMetadata table : keyspace.getTables()) {
                    System.out.printf("Keyspace: %s; Table: %s%n",
                            keyspace.getName(), table.getName());
                }
            }

        } finally {
            if (cluster != null)
                cluster.close();
        }
    }
}
