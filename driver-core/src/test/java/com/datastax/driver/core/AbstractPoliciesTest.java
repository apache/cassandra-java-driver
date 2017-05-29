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
package com.datastax.driver.core;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.datastax.driver.core.ConditionChecker.check;
import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_GENERIC_FORMAT;
import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;
import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;

public abstract class AbstractPoliciesTest extends CCMTestsSupport {

    private static final Logger logger = LoggerFactory.getLogger(AbstractPoliciesTest.class);
    private String tableName;

    private static class SchemaInAgreement implements Callable<Boolean> {

        private final Cluster cluster;

        private SchemaInAgreement(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public Boolean call() throws Exception {
            return cluster.getMetadata().checkSchemaAgreement();
        }
    }

    protected Map<InetAddress, Integer> coordinators = new HashMap<InetAddress, Integer>();

    protected PreparedStatement prepared;

    protected void createSchema(int replicationFactor) {
        final String ks = TestUtils.generateIdentifier("ks_");
        tableName = TestUtils.generateIdentifier("table_");
        session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, ks, replicationFactor));
        useKeyspace(ks);
        session().execute(String.format("CREATE TABLE %s (k int PRIMARY KEY, i int)", tableName));
        check().before(5, MINUTES).that(new SchemaInAgreement(cluster())).becomesTrue();
    }

    protected void createMultiDCSchema(int dc1RF, int dc2RF) {
        final String ks = TestUtils.generateIdentifier("ks_");
        tableName = TestUtils.generateIdentifier("table_");
        session().execute(String.format(CREATE_KEYSPACE_GENERIC_FORMAT, ks, "NetworkTopologyStrategy", String.format("'dc1' : %d, 'dc2' : %d", dc1RF, dc2RF)));
        useKeyspace(ks);
        session().execute(String.format("CREATE TABLE %s (k int PRIMARY KEY, i int)", tableName));
        check().before(5, MINUTES).that(new SchemaInAgreement(cluster())).becomesTrue();
    }

    /**
     * Coordinator management/count
     */
    protected void addCoordinator(ResultSet rs) {
        InetAddress coordinator = rs.getExecutionInfo().getQueriedHost().getAddress();
        Integer n = coordinators.get(coordinator);
        coordinators.put(coordinator, n == null ? 1 : n + 1);
    }

    @BeforeMethod(groups = "long")
    protected void resetCoordinators() {
        coordinators = new HashMap<InetAddress, Integer>();
    }

    @AfterMethod(groups = "long")
    protected void pause() {
        // pause before engaging in another expensive CCM cluster creation
        Uninterruptibles.sleepUninterruptibly(1, MINUTES);
    }

    private String queriedMapString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<InetAddress, Integer> entry : coordinators.entrySet())
            sb.append(entry.getKey()).append(" : ").append(entry.getValue()).append(", ");
        return sb.append("}").toString();
    }

    /**
     * Helper test methods
     */
    protected void assertQueried(String host, int n) {
        try {
            Integer queried = coordinators.get(InetAddress.getByName(host));
            if (logger.isDebugEnabled())
                logger.debug(String.format("Expected: %s\tReceived: %s", n, queried));
            else
                assertEquals(queried == null ? 0 : queried, n, queriedMapString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Init methods that handle writes using batch and consistency options.
     */
    protected void init(int n) {
        init(n, false, ConsistencyLevel.ONE);
    }

    protected void init(int n, boolean batch) {
        init(n, batch, ConsistencyLevel.ONE);
    }

    protected void init(int n, ConsistencyLevel cl) {
        write(n, false, cl);
    }

    protected void init(int n, boolean batch, ConsistencyLevel cl) {
        write(n, batch, cl);
        prepared = session().prepare("SELECT * FROM " + tableName + " WHERE k = ?").setConsistencyLevel(cl);
    }

    protected void write(int n) {
        write(n, false, ConsistencyLevel.ONE);
    }

    protected void write(int n, boolean batch) {
        write(n, batch, ConsistencyLevel.ONE);
    }

    protected void write(int n, ConsistencyLevel cl) {
        write(n, false, cl);
    }

    protected void write(int n, boolean batch, ConsistencyLevel cl) {
        // We don't use insert for our test because the resultSet don't ship the queriedHost
        // Also note that we don't use tracing because this would trigger requests that screw up the test
        for (int i = 0; i < n; ++i)
            if (batch)
                // BUG: WriteType == SIMPLE
                session().execute(batch()
                        .add(insertInto(tableName).values(new String[]{"k", "i"}, new Object[]{0, 0}))
                        .setConsistencyLevel(cl));
            else
                session().execute(new SimpleStatement(String.format("INSERT INTO %s(k, i) VALUES (0, 0)", tableName)).setConsistencyLevel(cl));
    }


    /**
     * Query methods that handle reads based on PreparedStatements and/or ConsistencyLevels.
     */
    protected void query(int n) {
        query(n, false, ConsistencyLevel.ONE);
    }

    protected void query(int n, boolean usePrepared) {
        query(n, usePrepared, ConsistencyLevel.ONE);
    }

    protected void query(int n, ConsistencyLevel cl) {
        query(n, false, cl);
    }

    protected void query(int n, boolean usePrepared, ConsistencyLevel cl) {
        if (usePrepared) {
            BoundStatement bs = prepared.bind(0);
            for (int i = 0; i < n; ++i)
                addCoordinator(session().execute(bs));
        } else {
            ByteBuffer routingKey = ByteBuffer.allocate(4);
            routingKey.putInt(0, 0);
            for (int i = 0; i < n; ++i)
                addCoordinator(session().execute(new SimpleStatement(String.format("SELECT * FROM %s WHERE k = 0", tableName)).setRoutingKey(routingKey).setConsistencyLevel(cl)));
        }
    }

}
