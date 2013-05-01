/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.util.List;

/**
 * Basic information on the execution of a query.
 * <p>
 * This provides the following information on the execution of a (successful)
 * query:
 * <ul>
 *   <li>The list of Cassandra hosts tried in order (usually just one, unless
 *   a node has been tried but was dead/in error or a timeout provoked a retry
 *   (which depends on the RetryPolicy)).</li>
 *   <li>The consistency level achieved by the query (usually the one asked,
 *   though some specific RetryPolicy may allow this to be different).</li>
 *   <li>The query trace recorded by Cassandra if tracing had been set for the
 *   query.</li>
 * </ul>
 */
public class ExecutionInfo
{
    private final List<Host> triedHosts;
    private final ConsistencyLevel achievedConsistency;
    private final QueryTrace trace;

    private ExecutionInfo(List<Host> triedHosts, ConsistencyLevel achievedConsistency, QueryTrace trace) {
        this.triedHosts = triedHosts;
        this.achievedConsistency = achievedConsistency;
        this.trace = trace;
    }

    ExecutionInfo(List<Host> triedHosts) {
        this(triedHosts, null, null);
    }

    ExecutionInfo withTrace(QueryTrace newTrace) {
        return new ExecutionInfo(triedHosts, achievedConsistency, newTrace);
    }

    ExecutionInfo withAchievedConsistency(ConsistencyLevel newConsistency) {
        return new ExecutionInfo(triedHosts, newConsistency, trace);
    }

    /**
     * The list of tried hosts for this query.
     * <p>
     * In general, this will be a singleton list with the host that coordinated
     * that query. However, if an host is tried by the driver but is dead or in
     * error, that host is recorded and the query is retry. Also, on a timeout
     * or unavailable exception, some
     * {@link com.datastax.driver.core.policies.RetryPolicy} may retry the
     * query on the same host, so the same host might appear twice.
     * <p>
     * If you are only interested in fetching the final (and often only) node
     * coordinating the query, {@link #getQueriedHost} provides a shortcut to
     * fetch the last element of the list returned by this method.
     *
     * @return the list of tried hosts for this query, in the order tried.
     */
    public List<Host> getTriedHosts() {
        return triedHosts;
    }

    /**
     * Return the Cassandra host that coordinated this query.
     * <p>
     * This is a shortcut for {@code getTriedHosts().get(getTriedHosts().size())}.
     *
     * @return return the Cassandra host that coordinated this query.
     */
    public Host getQueriedHost() {
        return triedHosts.get(triedHosts.size() - 1);
    }

    /**
     * If the query returned without achieving the requested consistency level
     * due to the {@link com.datastax.driver.core.policies.RetryPolicy}, this
     * return the biggest consistency level that has been actually achieved by
     * the query.
     * <p>
     * Note that the default {@code RetryPolicy}
     * ({@link com.datastax.driver.core.policies.DefaultRetryPolicy})
     * will never allow a query to be successful without achieving the
     * initially requested consistency level and hence with that default
     * policy, this method will <b>always</b> return {@code null}. However, it
     * might occasionally return a non-{@code null} with say,
     * {@link com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy}.
     */
    public ConsistencyLevel getAchievedConsistencyLevel() {
        return achievedConsistency;
    }

    /**
     * The query trace if tracing was enabled on this query.
     *
     * @return the {@code QueryTrace} object for this query if tracing was
     * enable for this query, or {@code null} otherwise.
     */
    public QueryTrace getQueryTrace() {
        return trace;
    }
}
