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

import java.util.Collection;
import java.util.Collections;

import org.junit.rules.ExternalResource;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraRule extends ExternalResource {

    private Cluster cluster;

    private Session session;

    @Override
    protected void before() {
        startCluster();
    }

    @Override
    protected void after() {
        stopCluster();
    }

    private void startCluster() {
        cluster = Cluster.builder().addContactPoints(CCMBridge.ipOfNode(1)).build();
        session = cluster.connect();
        session.execute("USE " + CassandraServer.KEYSPACE);
        for (String tableDef : getTableDefinitions()) {
            session.execute(tableDef);
        }
    }

    private void stopCluster() {
        if (session != null) {
            session.execute("DROP KEYSPACE " + CassandraServer.KEYSPACE);
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    protected Collection<String> getTableDefinitions() {
        return Collections.emptyList();
    }

}
