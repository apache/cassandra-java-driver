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

import java.nio.ByteBuffer;

import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;

/**
 * Base class for custom {@link Statement} implementations that wrap another statement.
 * <p>
 * This is intended for use with a custom {@link RetryPolicy}, {@link LoadBalancingPolicy} or
 * {@link SpeculativeExecutionPolicy}. The client code can wrap a statement to "mark" it, or
 * add information that will lead to special handling in the policy.
 * <p>
 * Example:
 * <pre>
 * {@code
 * // Define your own subclass
 * public class MyCustomStatement extends StatementWrapper {
 *     public MyCustomStatement(Statement wrapped) {
 *         super(wrapped);
 *     }
 * }
 *
 * // In your load balancing policy, add a special case for that new type
 * public class MyLoadBalancingPolicy implements LoadBalancingPolicy {
 *     public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
 *         if (statement instanceof MyCustomStatement) {
 *             // return specially crafted plan
 *         } else {
 *             // return default plan
 *         }
 *     }
 * }
 *
 * // The client wraps whenever it wants to trigger the special plan
 * Statement s = new SimpleStatement("...");
 * session.execute(s);                         // will use default plan
 * session.execute(new MyCustomStatement(s));  // will use special plan
 * }
 * </pre>
 */
public abstract class StatementWrapper extends Statement {
    private final Statement wrapped;

    /**
     * Builds a new instance.
     *
     * @param wrapped the wrapped statement.
     */
    protected StatementWrapper(Statement wrapped) {
        this.wrapped = wrapped;
    }

    Statement getWrappedStatement() {
        // Protect against multiple levels of wrapping (even though there is no practical reason for that)
        return (wrapped instanceof StatementWrapper)
            ? ((StatementWrapper)wrapped).getWrappedStatement()
            : wrapped;
    }

    @Override
    public Statement setConsistencyLevel(ConsistencyLevel consistency) {
        return wrapped.setConsistencyLevel(consistency);
    }

    @Override
    public Statement disableTracing() {
        return wrapped.disableTracing();
    }

    @Override
    public Statement setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
        return wrapped.setSerialConsistencyLevel(serialConsistency);
    }

    @Override
    public Statement enableTracing() {
        return wrapped.enableTracing();
    }

    @Override
    public ByteBuffer getPagingState() {
        return wrapped.getPagingState();
    }

    @Override
    public boolean isTracing() {
        return wrapped.isTracing();
    }

    @Override
    public RetryPolicy getRetryPolicy() {
        return wrapped.getRetryPolicy();
    }

    @Override
    public ByteBuffer getRoutingKey() {
        return wrapped.getRoutingKey();
    }

    @Override
    public Statement setRetryPolicy(RetryPolicy policy) {
        return wrapped.setRetryPolicy(policy);
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return wrapped.getConsistencyLevel();
    }

    @Override
    public Statement setPagingState(PagingState pagingState) {
        return wrapped.setPagingState(pagingState);
    }

    @Override
    public ConsistencyLevel getSerialConsistencyLevel() {
        return wrapped.getSerialConsistencyLevel();
    }

    @Override
    public String getKeyspace() {
        return wrapped.getKeyspace();
    }

    @Override
    public int getFetchSize() {
        return wrapped.getFetchSize();
    }

    @Override
    public Statement setFetchSize(int fetchSize) {
        return wrapped.setFetchSize(fetchSize);
    }
}
