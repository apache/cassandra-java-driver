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
package com.datastax.driver.core;

import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class StatementWrapperTest extends CCMTestsSupport {

    private static final String INSERT_QUERY = "insert into test (k, v) values (?, ?)";
    private static final String SELECT_QUERY = "select * from test where k = ?";

    CustomLoadBalancingPolicy loadBalancingPolicy = new CustomLoadBalancingPolicy();
    CustomSpeculativeExecutionPolicy speculativeExecutionPolicy = new CustomSpeculativeExecutionPolicy();
    CustomRetryPolicy retryPolicy = new CustomRetryPolicy();

    @Override
    public void onTestContextInitialized() {
        execute("create table test (k text primary key, v int)");
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder()
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withSpeculativeExecutionPolicy(speculativeExecutionPolicy)
                .withRetryPolicy(retryPolicy);
    }

    @Test(groups = "short")
    public void should_pass_wrapped_statement_to_load_balancing_policy() {
        loadBalancingPolicy.customStatementsHandled.set(0);

        SimpleStatement s = new SimpleStatement("select * from system.local");
        session().execute(s);
        assertThat(loadBalancingPolicy.customStatementsHandled.get()).isEqualTo(0);

        session().execute(new CustomStatement(s));
        assertThat(loadBalancingPolicy.customStatementsHandled.get()).isEqualTo(1);
    }

    @Test(groups = "short")
    public void should_pass_wrapped_statement_to_speculative_execution_policy() {
        speculativeExecutionPolicy.customStatementsHandled.set(0);

        SimpleStatement s = new SimpleStatement("select * from system.local");
        session().execute(s);
        assertThat(speculativeExecutionPolicy.customStatementsHandled.get()).isEqualTo(0);

        session().execute(new CustomStatement(s));
        assertThat(speculativeExecutionPolicy.customStatementsHandled.get()).isEqualTo(1);
    }

    @Test(groups = "short")
    public void should_pass_wrapped_statement_to_retry_policy() {
        retryPolicy.customStatementsHandled.set(0);

        // Set CL TWO with only one node, so the statement will always cause UNAVAILABLE,
        // which our custom policy ignores.
        Statement s = new SimpleStatement("select * from system.local")
                .setConsistencyLevel(ConsistencyLevel.TWO);

        session().execute(s);
        assertThat(retryPolicy.customStatementsHandled.get()).isEqualTo(0);

        session().execute(new CustomStatement(s));
        assertThat(retryPolicy.customStatementsHandled.get()).isEqualTo(1);
    }

    @CassandraVersion(major = 2.0)
    @Test(groups = "short")
    public void should_execute_wrapped_simple_statement() {
        session().execute(new CustomStatement(new SimpleStatement(INSERT_QUERY, "key_simple", 1)));

        ResultSet rs = session().execute(new CustomStatement(new SimpleStatement(SELECT_QUERY, "key_simple")));
        assertThat(rs.one().getInt("v")).isEqualTo(1);
    }

    @Test(groups = "short")
    public void should_execute_wrapped_bound_statement() {
        PreparedStatement preparedStatement = session().prepare(new SimpleStatement(INSERT_QUERY));
        session().execute(new CustomStatement(preparedStatement.bind("key_bound", 1)));

        preparedStatement = session().prepare(new SimpleStatement(SELECT_QUERY));
        ResultSet rs = session().execute(new CustomStatement(preparedStatement.bind("key_bound")));
        assertThat(rs.one().getInt("v")).isEqualTo(1);
    }

    @CassandraVersion(major = 2.0)
    @Test(groups = "short")
    public void should_execute_wrapped_batch_statement() {
        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(new SimpleStatement(INSERT_QUERY, "key_batch", 1));

        session().execute(new CustomStatement(batchStatement));

        ResultSet rs = session().execute(SELECT_QUERY, "key_batch");
        assertThat(rs.one().getInt("v")).isEqualTo(1);
    }

    @CassandraVersion(major = 2.0)
    @Test(groups = "short")
    public void should_add_wrapped_batch_statement_to_batch_statement() {
        BatchStatement batchStatementForWrapping = new BatchStatement();
        batchStatementForWrapping.add(new SimpleStatement(INSERT_QUERY, "key1", 1));

        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(new CustomStatement(new SimpleStatement(INSERT_QUERY, "key2", 2)));
        batchStatement.add(new CustomStatement(batchStatementForWrapping));

        session().execute(batchStatement);

        ResultSet rs = session().execute(SELECT_QUERY, "key1");
        assertThat(rs.one().getInt("v")).isEqualTo(1);

        rs = session().execute(SELECT_QUERY, "key2");
        assertThat(rs.one().getInt("v")).isEqualTo(2);
    }

    /**
     * A custom wrapper that's just used to mark statements.
     */
    static class CustomStatement extends StatementWrapper {
        protected CustomStatement(Statement wrapped) {
            super(wrapped);
        }
    }

    /**
     * A load balancing policy that counts how many times it has seen the custom wrapper
     */
    static class CustomLoadBalancingPolicy extends DelegatingLoadBalancingPolicy {
        final AtomicInteger customStatementsHandled = new AtomicInteger();

        public CustomLoadBalancingPolicy() {
            super(new RoundRobinPolicy());
        }

        @Override
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            if (statement instanceof CustomStatement)
                customStatementsHandled.incrementAndGet();
            return super.newQueryPlan(loggedKeyspace, statement);
        }
    }

    /**
     * A speculative execution policy that counts how many times it has seen the custom wrapper
     */
    static class CustomSpeculativeExecutionPolicy extends DelegatingSpeculativeExecutionPolicy {
        final AtomicInteger customStatementsHandled = new AtomicInteger();

        protected CustomSpeculativeExecutionPolicy() {
            super(NoSpeculativeExecutionPolicy.INSTANCE);
        }

        @Override
        public SpeculativeExecutionPlan newPlan(String loggedKeyspace, Statement statement) {
            if (statement instanceof CustomStatement)
                customStatementsHandled.incrementAndGet();
            return super.newPlan(loggedKeyspace, statement);
        }
    }

    /**
     * A retry policy that counts how many times it has seen the custom wrapper for UNAVAILABLE errors.
     */
    static class CustomRetryPolicy implements ExtendedRetryPolicy {
        final AtomicInteger customStatementsHandled = new AtomicInteger();

        @Override
        public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            if (statement instanceof CustomStatement)
                customStatementsHandled.incrementAndGet();
            return RetryDecision.ignore();
        }

        @Override
        public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            return RetryDecision.rethrow();
        }

        @Override
        public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            return RetryDecision.rethrow();
        }

        @Override
        public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, Exception e, int nbRetry) {
            return RetryDecision.tryNextHost(cl);
        }

    }
}
