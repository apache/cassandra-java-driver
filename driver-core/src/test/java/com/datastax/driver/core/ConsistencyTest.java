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

import org.scassandra.Scassandra;
import org.scassandra.http.client.BatchExecution;
import org.scassandra.http.client.PreparedStatementExecution;
import org.scassandra.http.client.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static org.testng.Assert.*;

public class ConsistencyTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsistencyTest.class);
    private ScassandraCluster sCluster;

    @BeforeClass(groups = "short")
    public void setUp() {
        sCluster = ScassandraCluster.builder().withNodes(1).build();
        sCluster.init();
    }

    @AfterClass(groups = "short")
    public void tearDownClass() {
        sCluster.stop();
    }

    @AfterMethod(groups = "short")
    public void tearDown() {
        clearActivityLog();
    }

    public void clearActivityLog() {
        for (Scassandra node : sCluster.nodes()) {
            node.activityClient().clearAllRecordedActivity();
        }
    }

    public Cluster.Builder builder() {
        //Note: nonQuietClusterCloseOptions is used to speed up tests
        return Cluster.builder()
                .addContactPoints(sCluster.address(1).getAddress())
                .withPort(sCluster.getBinaryPort()).withNettyOptions(nonQuietClusterCloseOptions);
    }

    /**
     * This method checks the expected/sent serial consistency level against that which is received.
     * ConsistencyLevel.SERIAL is the default serial consistency level, so even when sent it will return
     * as null.
     */
    public void checkSerialCLMatch(ConsistencyLevel expected, String received) {
        if (expected.equals(ConsistencyLevel.SERIAL)) {
            assertNull(received);
        } else {
            assertTrue(received.equals(expected.toString()));
        }
    }

    public PreparedStatementExecution executePrepared(Session session, String statement, ConsistencyLevel level, ConsistencyLevel serialLevel) {
        PreparedStatement ps = session.prepare(statement);
        BoundStatement bound = ps.bind();
        if (level != null) {
            bound.setConsistencyLevel(level);
        }
        if (serialLevel != null) {
            bound.setSerialConsistencyLevel(serialLevel);
        }
        session.execute(bound);
        List<PreparedStatementExecution> pses = sCluster.node(1).activityClient().retrievePreparedStatementExecutions();
        PreparedStatementExecution pse = pses.get(0);
        assertTrue(pse.getPreparedStatementText().equals(statement));
        return pse;
    }

    public BatchExecution executeBatch(Session session, String statement, ConsistencyLevel level, ConsistencyLevel serialLevel) {
        BatchStatement batch = new BatchStatement();
        batch.add(new SimpleStatement(statement));

        if (level != null) {
            batch.setConsistencyLevel(level);
        }
        if (serialLevel != null) {
            batch.setSerialConsistencyLevel(serialLevel);
        }
        session.execute(batch);
        List<BatchExecution> batches = sCluster.node(1).activityClient().retrieveBatches();
        assertEquals(batches.size(), 1);
        return batches.get(0);
    }

    public Query executeSimple(Session session, String statement, ConsistencyLevel level, ConsistencyLevel serialLevel) {
        SimpleStatement simpleStatement = new SimpleStatement(statement);
        if (level != null) {
            simpleStatement.setConsistencyLevel(level);
        }
        if (serialLevel != null) {
            simpleStatement.setSerialConsistencyLevel(serialLevel);
        }
        session.execute(simpleStatement);
        //Find the unique query in the activity log.
        List<Query> queries = sCluster.node(1).activityClient().retrieveQueries();
        for (Query query : queries) {
            if (query.getQuery().equals(statement))
                return query;
        }
        return null;
    }

    /**
     * When no consistency level is defined the default of LOCAL_ONE should be used.
     *
     * @test_category consistency
     */
    @Test(groups = "short")
    public void should_use_global_default_cl_when_none_specified() throws Throwable {
        //Build a cluster with no CL level set in the query options.
        Cluster cluster = builder().build();
        try {
            Session session = cluster.connect();

            //Construct unique simple statement query, with no CL defined.
            //Check to ensure
            String queryString = "default_cl";
            Query clQuery = executeSimple(session, queryString, null, null);
            assertTrue(clQuery.getConsistency().equals(ConsistencyLevel.LOCAL_ONE.toString()));

            //Check prepared statement default CL
            String prepareString = "prepared_default_cl";
            PreparedStatementExecution pse = executePrepared(session, prepareString, null, null);
            assertTrue(pse.getConsistency().equals(ConsistencyLevel.LOCAL_ONE.toString()));

            //Check batch statement default CL
            String batchStateString = "batch_default_cl";
            BatchExecution batch = executeBatch(session, batchStateString, null, null);
            assertTrue(batch.getConsistency().equals(ConsistencyLevel.LOCAL_ONE.toString()));
        } finally {
            cluster.close();
        }
    }

    /**
     * Exhaustively tests all consistency levels when they are set via QueryOptions.
     *
     * @test_category consistency
     */
    @Test(groups = "short", dataProvider = "consistencyLevels", dataProviderClass = DataProviders.class)
    public void should_use_query_option_cl(ConsistencyLevel cl) throws Throwable {
        //Build a cluster with a CL level set in the query options.
        Cluster cluster = builder().withQueryOptions(new QueryOptions().setConsistencyLevel(cl)).build();
        try {
            Session session = cluster.connect();
            //Construct unique query, with no CL defined.
            String queryString = "query_cl";
            Query clQuery = executeSimple(session, queryString, null, null);
            assertTrue(clQuery.getConsistency().equals(cl.toString()));

            //Check prepared statement CL
            String prepareString = "preapred_query_cl";
            PreparedStatementExecution pse = executePrepared(session, prepareString, null, null);
            assertTrue(pse.getConsistency().equals(cl.toString()));

            //Check batch statement CL
            String batchStateString = "batch_query_cl";
            BatchExecution batch = executeBatch(session, batchStateString, null, null);
            assertTrue(batch.getConsistency().equals(cl.toString()));
        } finally {
            cluster.close();
        }
    }

    /**
     * Exhaustively tests all consistency levels when they are set at the statement level.
     *
     * @test_category consistency
     */
    @Test(groups = "short", dataProvider = "consistencyLevels", dataProviderClass = DataProviders.class)
    public void should_use_statement_cl(ConsistencyLevel cl) throws Throwable {
        //Build a cluster with no CL set in the query options.
        //Note: nonQuietClusterCloseOptions is used to speed up tests
        Cluster cluster = builder().build();
        try {
            Session session = cluster.connect();
            //Construct unique query statement with a CL defined.
            String queryString = "statement_cl";
            Query clQuery = executeSimple(session, queryString, cl, null);
            assertTrue(clQuery.getConsistency().equals(cl.toString()));

            //Check prepared statement CL
            String prepareString = "preapred_statement_cl";
            PreparedStatementExecution pse = executePrepared(session, prepareString, cl, null);
            assertTrue(pse.getConsistency().equals(cl.toString()));

            //Check batch statement CL
            String batchStateString = "batch_statement_cl";
            BatchExecution batch = executeBatch(session, batchStateString, cl, null);
            assertTrue(batch.getConsistency().equals(cl.toString()));
        } finally {
            cluster.close();
        }
    }

    /**
     * Tests that order of precedence is followed when defining CLs.
     * Statement level CL should be honored above QueryOptions.
     * QueryOptions should be honored above default CL.
     *
     * @test_category consistency
     */
    @Test(groups = "short")
    public void should_use_appropriate_cl_when_multiple_defined() throws Throwable {
        ConsistencyLevel cl_one = ConsistencyLevel.ONE;
        //Build a cluster with no CL set in the query options.
        Cluster cluster = builder().withQueryOptions(new QueryOptions().setConsistencyLevel(cl_one)).build();
        try {

            Session session = cluster.connect();

            //Check order of precedence for simple statements
            //Construct unique query statement with no CL defined.
            String queryString = "opts_cl";
            Query clQuery = executeSimple(session, queryString, null, null);
            assertTrue(clQuery.getConsistency().equals(cl_one.toString()));

            //Construct unique query statement with a CL defined.
            ConsistencyLevel cl_all = ConsistencyLevel.ALL;
            queryString = "stm_cl";
            clQuery = executeSimple(session, queryString, cl_all, null);
            assertTrue(clQuery.getConsistency().equals(cl_all.toString()));

            //Check order of precedence for prepared statements
            //Construct unique prepared statement with no CL defined.
            String prepareString = "prep_opts_cl";
            PreparedStatementExecution pse = executePrepared(session, prepareString, null, null);
            assertTrue(pse.getConsistency().equals(cl_one.toString()));
            clearActivityLog();

            //Construct unique prepared statement with a CL defined.
            prepareString = "prep_stm_cl";
            pse = executePrepared(session, prepareString, cl_all, null);
            assertTrue(pse.getConsistency().equals(cl_all.toString()));

            //Check order of precedence for batch statements
            //Construct unique batch statement with no CL defined.
            String batchString = "batch_opts_cl";
            BatchExecution batch = executeBatch(session, batchString, null, null);
            assertTrue(batch.getConsistency().equals(cl_one.toString()));
            clearActivityLog();

            //Construct unique prepared statement with a CL defined.
            batchString = "prep_stm_cl";
            batch = executeBatch(session, batchString, cl_all, null);
            assertTrue(batch.getConsistency().equals(cl_all.toString()));
        } finally {
            cluster.close();
        }
    }

    /**
     * Exhaustively tests all serial consistency levels when they are set via QueryOptions.
     *
     * @test_category consistency
     */
    @Test(groups = "short", dataProvider = "serialConsistencyLevels", dataProviderClass = DataProviders.class)
    public void should_use_query_option_serial_cl(ConsistencyLevel cl) throws Throwable {
        //Build a cluster with a CL level set in the query options.
        Cluster cluster = builder().withQueryOptions(new QueryOptions().setSerialConsistencyLevel(cl)).build();
        try {
            Session session = cluster.connect();
            //Construct unique query, with no CL defined.
            String queryString = "serial_query_cl";
            Query clQuery = executeSimple(session, queryString, null, cl);
            checkSerialCLMatch(cl, clQuery.getSerialConsistency());

            //Check prepared statement CL
            String prepareString = "preapred_statement_serial_cl";
            PreparedStatementExecution pse = executePrepared(session, prepareString, null, null);
            checkSerialCLMatch(cl, pse.getSerialConsistency());

            //Check batch statement CL
            String batchStateString = "batch_statement_serial_cl";
            BatchExecution batch = executeBatch(session, batchStateString, null, null);
            checkSerialCLMatch(cl, batch.getSerialConsistency());
        } finally {
            cluster.close();
        }
    }

    /**
     * Exhaustively tests all serial consistency levels when they are set at the statement level.
     *
     * @test_category consistency
     */
    @Test(groups = "short", dataProvider = "serialConsistencyLevels", dataProviderClass = DataProviders.class)
    public void should_use_statement_serial_cl(ConsistencyLevel cl) throws Throwable {
        //Build a cluster with no CL set in the query options.
        Cluster cluster = builder().build();
        try {
            Session session = cluster.connect();
            //Construct unique query statement with a CL defined.
            String queryString = "statement_serial_cl";
            Query clQuery = executeSimple(session, queryString, null, cl);
            checkSerialCLMatch(cl, clQuery.getSerialConsistency());

            //Check prepared statement CL
            String prepareString = "preapred_statement_serial_cl";
            PreparedStatementExecution pse = executePrepared(session, prepareString, null, cl);
            checkSerialCLMatch(cl, pse.getSerialConsistency());

            //Check batch statement CL
            String batchStateString = "batch_statement_serial_cl";
            BatchExecution batch = executeBatch(session, batchStateString, null, cl);
            checkSerialCLMatch(cl, batch.getSerialConsistency());
        } finally {
            cluster.close();
        }
    }
}
