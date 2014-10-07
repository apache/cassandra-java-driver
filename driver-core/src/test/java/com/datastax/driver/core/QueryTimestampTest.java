package com.datastax.driver.core;

import java.util.Collection;

import com.google.common.collect.Lists;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Metrics.Errors;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

import static com.datastax.driver.core.TestUtils.versionCheck;

/**
 * Tests the behavior of client-provided timestamps with protocol v3.
 */
public class QueryTimestampTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        versionCheck(2.1, 0, "This will only work with Cassandra 2.1.0");

        return Lists.newArrayList("CREATE TABLE foo (k int PRIMARY KEY, v int)");
    }

    private volatile long timestampFromGenerator;

    @Override
    protected Builder configure(Builder builder) {
        return builder
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
        session.execute("TRUNCATE foo");
    }

    @Test(groups = "short")
    public void should_use_CQL_timestamp_over_anything_else() {
        timestampFromGenerator = 10;
        String query = "INSERT INTO foo (k, v) VALUES (1, 1) USING TIMESTAMP 20";
        session.execute(new SimpleStatement(query).setDefaultTimestamp(30));

        long writeTime = session.execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        assertEquals(writeTime, 20);
    }

    @Test(groups = "short")
    public void should_use_statement_timestamp_over_generator() {
        timestampFromGenerator = 10;
        String query = "INSERT INTO foo (k, v) VALUES (1, 1)";
        session.execute(new SimpleStatement(query).setDefaultTimestamp(30));

        long writeTime = session.execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        assertEquals(writeTime, 30);
    }

    @Test(groups = "short")
    public void should_use_generator_timestamp_if_none_other_specified() {
        timestampFromGenerator = 10;
        String query = "INSERT INTO foo (k, v) VALUES (1, 1)";
        session.execute(query);

        long writeTime = session.execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        assertEquals(writeTime, 10);
    }

    @Test(groups = "short")
    public void should_use_server_side_timestamp_if_none_specified() {
        timestampFromGenerator = Long.MIN_VALUE;
        long clientTime = System.currentTimeMillis() * 1000;
        String query = "INSERT INTO foo (k, v) VALUES (1, 1)";
        session.execute(query);

        long writeTime = session.execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        assertTrue(writeTime >= clientTime);
    }

    @Test(groups = "short")
    public void should_apply_statement_timestamp_only_to_batched_queries_without_timestamp() {
        BatchStatement batch = new BatchStatement();
        batch.add(new SimpleStatement("INSERT INTO foo (k, v) VALUES (1, 1)"));
        batch.add(new SimpleStatement("INSERT INTO foo (k, v) VALUES (2, 1) USING TIMESTAMP 20"));
        batch.setDefaultTimestamp(10);
        session.execute(batch);

        long writeTime1 = session.execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        long writeTime2 = session.execute("SELECT writeTime(v) FROM foo WHERE k = 2").one().getLong(0);
        assertEquals(writeTime1, 10);
        assertEquals(writeTime2, 20);
    }

    @Test(groups = "short")
    public void should_apply_generator_timestamp_only_to_batched_queries_without_timestamp() {
        timestampFromGenerator = 10;
        BatchStatement batch = new BatchStatement();
        batch.add(new SimpleStatement("INSERT INTO foo (k, v) VALUES (1, 1)"));
        batch.add(new SimpleStatement("INSERT INTO foo (k, v) VALUES (2, 1) USING TIMESTAMP 20"));
        session.execute(batch);

        long writeTime1 = session.execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        long writeTime2 = session.execute("SELECT writeTime(v) FROM foo WHERE k = 2").one().getLong(0);
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
        session.execute(batch);

        long writeTime1 = session.execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        long writeTime2 = session.execute("SELECT writeTime(v) FROM foo WHERE k = 2").one().getLong(0);
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

        session.execute(statement);

        Errors metrics = session.getCluster().getMetrics().getErrorMetrics();
        assertEquals(metrics.getRetriesOnUnavailable().getCount(), 1);

        long writeTime = session.execute("SELECT writeTime(v) FROM foo WHERE k = 1").one().getLong(0);
        assertEquals(writeTime, 10);
    }
}
