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

import com.datastax.driver.core.Metrics.Errors;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests the behavior of client-provided timestamps with protocol v3.
 */
@CassandraVersion("2.1.0")
public class QueryTimestampTest extends CCMTestsSupport {

    private volatile long timestampFromGenerator;

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE foo (k int PRIMARY KEY, v int)");
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder()
                .withTimestampGenerator(new TimestampGenerator() {
                    @Override
                    public long next() {
                        return timestampFromGenerator;
                    }
                })
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
    }

    @BeforeMethod(groups = "short")
    public void cleanData() {
        session().execute("TRUNCATE foo");
    }

    @Test(groups = "short")
    public void should_use_CQL_timestamp_over_anything_else() {
        timestampFromGenerator = 10;
        String query = "INSERT INTO foo (k, v) VALUES (1, 1) USING TIMESTAMP 20";
        session().execute(new SimpleStatement(query).setDefaultTimestamp(30));

        long writeTime = session().execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        assertEquals(writeTime, 20);
    }

    @Test(groups = "short")
    public void should_use_statement_timestamp_over_generator() {
        timestampFromGenerator = 10;
        String query = "INSERT INTO foo (k, v) VALUES (1, 1)";
        session().execute(new SimpleStatement(query).setDefaultTimestamp(30));

        long writeTime = session().execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        assertEquals(writeTime, 30);
    }

    @Test(groups = "short")
    public void should_use_generator_timestamp_if_none_other_specified() {
        timestampFromGenerator = 10;
        String query = "INSERT INTO foo (k, v) VALUES (1, 1)";
        session().execute(query);

        long writeTime = session().execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        assertEquals(writeTime, 10);
    }

    @Test(groups = "short")
    public void should_use_server_side_timestamp_if_none_specified() {
        timestampFromGenerator = Long.MIN_VALUE;
        long clientTime = System.currentTimeMillis() * 1000;
        String query = "INSERT INTO foo (k, v) VALUES (1, 1)";
        session().execute(query);

        long writeTime = session().execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        assertTrue(writeTime >= clientTime);
    }

    @Test(groups = "short")
    public void should_apply_statement_timestamp_only_to_batched_queries_without_timestamp() {
        BatchStatement batch = new BatchStatement();
        batch.add(new SimpleStatement("INSERT INTO foo (k, v) VALUES (1, 1)"));
        batch.add(new SimpleStatement("INSERT INTO foo (k, v) VALUES (2, 1) USING TIMESTAMP 20"));
        batch.setDefaultTimestamp(10);
        session().execute(batch);

        long writeTime1 = session().execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        long writeTime2 = session().execute("SELECT writeTime(v) FROM foo WHERE k = 2").one().getLong(0);
        assertEquals(writeTime1, 10);
        assertEquals(writeTime2, 20);
    }

    @Test(groups = "short")
    public void should_apply_generator_timestamp_only_to_batched_queries_without_timestamp() {
        timestampFromGenerator = 10;
        BatchStatement batch = new BatchStatement();
        batch.add(new SimpleStatement("INSERT INTO foo (k, v) VALUES (1, 1)"));
        batch.add(new SimpleStatement("INSERT INTO foo (k, v) VALUES (2, 1) USING TIMESTAMP 20"));
        session().execute(batch);

        long writeTime1 = session().execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        long writeTime2 = session().execute("SELECT writeTime(v) FROM foo WHERE k = 2").one().getLong(0);
        assertEquals(writeTime1, 10);
        assertEquals(writeTime2, 20);
    }

    @Test(groups = "short")
    public void should_apply_server_side_timestamp_only_to_batched_queries_without_timestamp() {
        timestampFromGenerator = Long.MIN_VALUE;
        long clientTime = System.currentTimeMillis() * 1000;
        BatchStatement batch = new BatchStatement();
        batch.add(new SimpleStatement("INSERT INTO foo (k, v) VALUES (1, 1)"));
        batch.add(new SimpleStatement("INSERT INTO foo (k, v) VALUES (2, 1) USING TIMESTAMP 20"));
        session().execute(batch);

        long writeTime1 = session().execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        long writeTime2 = session().execute("SELECT writeTime(v) FROM foo WHERE k = 2").one().getLong(0);
        assertTrue(writeTime1 >= clientTime);
        assertEquals(writeTime2, 20);
    }

    @Test(groups = "short")
    public void should_preserve_timestamp_when_retrying() {
        SimpleStatement statement = new SimpleStatement("INSERT INTO foo (k, v) VALUES (1, 1)");
        statement.setDefaultTimestamp(10);
        // This will fail since we test against a single-host cluster. The DowngradingConsistencyRetryPolicy
        // will retry it at ONE.
        statement.setConsistencyLevel(ConsistencyLevel.TWO);

        session().execute(statement);

        Errors metrics = session().getCluster().getMetrics().getErrorMetrics();
        assertEquals(metrics.getRetriesOnUnavailable().getCount(), 1);

        long writeTime = session().execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        assertEquals(writeTime, 10);
    }
}
