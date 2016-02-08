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

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example shows how to inspect the Cluster's metadata
 * to gather information about the cluster topology (which nodes
 * belong to the cluster) and schema (what keyspaces, tables, etc.
 * exist in this cluster).
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class Example0002 {

    private static final Logger logger = LoggerFactory.getLogger("com.datastax.driver.examples");

    public static void main(String[] args) {

        Cluster cluster = null;

        try {

            cluster = Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .build();

            Metadata metadata = cluster.getMetadata();
            logger.info("Connected to cluster: {}", metadata.getClusterName());

            for (Host host : metadata.getAllHosts()) {

                logger.info("Datatacenter: {}; Host: {}; Rack: {}",
                        host.getDatacenter(), host.getAddress(), host.getRack());

            }

            for (KeyspaceMetadata keyspace : metadata.getKeyspaces()) {

                for (TableMetadata table : keyspace.getTables()) {

                    logger.info("Keyspace: {}; Table: {}",
                            keyspace.getName(), table.getName());

                }

            }


        } finally {

            if (cluster != null)
                cluster.close();

        }
    }


}
