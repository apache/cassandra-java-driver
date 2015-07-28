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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.policies.*;

public class StatementWrapperTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList();
    }

    CustomLoadBalancingPolicy loadBalancingPolicy = new CustomLoadBalancingPolicy();
    CustomSpeculativeExecutionPolicy speculativeExecutionPolicy = new CustomSpeculativeExecutionPolicy();
    CustomRetryPolicy retryPolicy = new CustomRetryPolicy();

    @Override
    protected Cluster.Builder configure(Cluster.Builder builder) {
        return builder
            .withLoadBalancingPolicy(loadBalancingPolicy)
            .withSpeculativeExecutionPolicy(speculativeExecutionPolicy)
            .withRetryPolicy(retryPolicy);
    }

    @Test(groups = "short")
    public void should_pass_wrapped_statement_to_load_balancing_policy() {
        loadBalancingPolicy.customStatementsHandled.set(0);

        SimpleStatement s = session.newSimpleStatement("select * from system.local");
        session.execute(s);
        assertThat(loadBalancingPolicy.customStatementsHandled.get()).isEqualTo(0);

        session.execute(new CustomStatement(s));
        assertThat(loadBalancingPolicy.customStatementsHandled.get()).isEqualTo(1);
    }

    @Test(groups = "short")
    public void should_pass_wrapped_statement_to_speculative_execution_policy() {
        speculativeExecutionPolicy.customStatementsHandled.set(0);

        SimpleStatement s = session.newSimpleStatement("select * from system.local");
        session.execute(s);
        assertThat(speculativeExecutionPolicy.customStatementsHandled.get()).isEqualTo(0);

        session.execute(new CustomStatement(s));
        assertThat(speculativeExecutionPolicy.customStatementsHandled.get()).isEqualTo(1);
    }

    @Test(groups = "short")
    public void should_pass_wrapped_statement_to_retry_policy() {
        retryPolicy.customStatementsHandled.set(0);

        // Set CL TWO with only one node, so the statement will always cause UNAVAILABLE,
        // which our custom policy ignores.
        Statement s = session.newSimpleStatement("select * from system.local")
            .setConsistencyLevel(ConsistencyLevel.TWO);

        session.execute(s);
        assertThat(retryPolicy.customStatementsHandled.get()).isEqualTo(0);

        session.execute(new CustomStatement(s));
        assertThat(retryPolicy.customStatementsHandled.get()).isEqualTo(1);
    }

    /** A custom wrapper that's just used to mark statements. */
    static class CustomStatement extends StatementWrapper {
        protected CustomStatement(Statement wrapped) {
            super(wrapped);
        }
    }

    /** A load balancing policy that counts how many times it has seen the custom wrapper */
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

    /** A speculative execution policy that counts how many times it has seen the custom wrapper */
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

    /** A retry policy that counts how many times it has seen the custom wrapper for UNAVAILABLE errors. */
    static class CustomRetryPolicy implements RetryPolicy {
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
        public void init(Cluster cluster) {
            // nothing to do
        }

        @Override
        public void close() {
            // nothing to do
        }
    }
}