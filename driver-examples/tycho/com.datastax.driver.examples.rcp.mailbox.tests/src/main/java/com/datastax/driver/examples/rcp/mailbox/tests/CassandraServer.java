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
package com.datastax.driver.examples.rcp.mailbox.tests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.CCMBridge;

public class CassandraServer {

    public static final String VERSION = "2.0.10";

    public static final String KEYSPACE = "test";

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraServer.class);

    private static CCMBridge ccmBridge;

    public static void start() {
        if (ccmBridge == null) {
            System.setProperty("cassandra.version", VERSION);
            System.setProperty("cassandra.keyspace", KEYSPACE);
            System.setProperty("cassandra.contactpoints", "127.0.1.1");
            LOGGER.info("Starting Cassandra...");
            try {
                ccmBridge = CCMBridge.create("test", 1);
                LOGGER.info("Cassandra successfully started.");
            } catch (RuntimeException e) {
                LOGGER.error("Could not start Cassandra", e);
                throw e;
            }
        }
    }

    public static void stop() {
        if (ccmBridge != null) {
            LOGGER.info("Stopping Cassandra...");
            ccmBridge.remove();
            LOGGER.info("Cassandra successfully stopped.");
            ccmBridge = null;
            // FIXME can't delete CCM temp dir, the field is private in CCMBridge
        }
    }

}
