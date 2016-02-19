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

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A configurable {@link LatencyTracker} that logs all executed statements.
 * <p/>
 * Typically, client applications would instantiate one single query logger (using its {@link Builder}),
 * configure it and register it on the relevant {@link Cluster} instance, e.g.:
 * <p/>
 * <pre>
 * Cluster cluster = ...
 * QueryLogger queryLogger = QueryLogger.builder()
 *     .withConstantThreshold(...)
 *     .withMaxQueryStringLength(...)
 *     .build();
 * cluster.register(queryLogger);
 * </pre>
 * <p/>
 * Refer to the {@link Builder} documentation for more information on
 * configuration settings for the query logger.
 * <p/>
 * Once registered, the query logger will log every {@link RegularStatement}, {@link BoundStatement} or {@link BatchStatement}
 * executed by the driver;
 * note that it will never log other types of statement, null statements nor any special statement used internally by the driver.
 * <p/>
 * There is one log for each request to a Cassandra node; because the driver sometimes retries the same statement on multiple nodes,
 * a single statement execution (for example, a single call to {@link Session#execute(Statement)}) can produce multiple logs on
 * different nodes.
 * <p/>
 * For more flexibility, the query logger uses 3 different {@link Logger} instances:
 * <p/>
 * <ol>
 * <li>{@link #NORMAL_LOGGER}: used to log normal queries, i.e., queries that completed successfully
 * within a configurable threshold in milliseconds.</li>
 * <li>{@link #SLOW_LOGGER}: used to log slow queries, i.e., queries that completed successfully
 * but that took longer than a configurable threshold in milliseconds to complete.</li>
 * <li>{@link #ERROR_LOGGER}: used to log unsuccessful queries, i.e.,
 * queries that did not completed normally and threw an exception.
 * Note this this logger will also print the full stack trace of the reported exception.</li>
 * </ol>
 * <p/>
 * <p/>
 * The appropriate logger is chosen according to the following algorithm:
 * <ol>
 * <li>if an exception has been thrown: use {@link #ERROR_LOGGER};</li>
 * <li>otherwise, if the reported latency is greater than the configured threshold in milliseconds: use {@link #SLOW_LOGGER};</li>
 * <li>otherwise, use {@link #NORMAL_LOGGER}.</li>
 * </ol>
 * <p/>
 * <p/>
 * All loggers are activated by setting their levels to {@code DEBUG} or {@code TRACE} (including {@link #ERROR_LOGGER}).
 * If the level is set to {@code TRACE} and the statement being logged is a {@link BoundStatement},
 * then the query parameters (if any) will be logged as well (names and actual values).
 * <p/>
 * <p/>
 * <strong>Constant thresholds vs. Dynamic thresholds</strong>
 * <p/>
 * Currently the QueryLogger can track slow queries in two different ways:
 * using a constant threshold in milliseconds (which is the default behavior),
 * or using a dynamic threshold based on per-host percentiles computed by
 * {@link PerHostPercentileTracker}.
 * <p/>
 * <b>Note that the dynamic threshold version is currently provided as a beta preview: it hasn't been extensively
 * tested yet, and the API is still subject to change.</b> To use it, you must first obtain and register
 * an instance of {@link PerHostPercentileTracker}, then create your QueryLogger as follows:
 * <p/>
 * <pre>
 * Cluster cluster = ...
 * // create an instance of PerHostPercentileTracker and register it
 * PerHostPercentileTracker tracker = ...;
 * cluster.register(tracker);
 * // create an instance of QueryLogger and register it
 * QueryLogger queryLogger = QueryLogger.builder()
 *     .withDynamicThreshold(tracker, ...)
 *     .withMaxQueryStringLength(...)
 *     .build();
 * cluster.register(queryLogger);
 * </pre>
 * <p/>
 * <p/>
 * This class is thread-safe.
 *
 * @since 2.0.10
 */
public abstract class QueryLogger implements LatencyTracker {

    /**
     * The default latency threshold in milliseconds beyond which queries are considered 'slow'
     * and logged as such by the driver.
     */
    public static final long DEFAULT_SLOW_QUERY_THRESHOLD_MS = 5000;

    /**
     * The default latency percentile beyond which queries are considered 'slow'
     * and logged as such by the driver.
     */
    public static final double DEFAULT_SLOW_QUERY_THRESHOLD_PERCENTILE = 99.0;

    /**
     * The default maximum length of a CQL query string that can be logged verbatim
     * by the driver. Query strings longer than this value will be truncated
     * when logged.
     */
    public static final int DEFAULT_MAX_QUERY_STRING_LENGTH = 500;

    /**
     * The default maximum length of a query parameter value that can be logged verbatim
     * by the driver. Parameter values longer than this value will be truncated
     * when logged.
     */
    public static final int DEFAULT_MAX_PARAMETER_VALUE_LENGTH = 50;

    /**
     * The default maximum number of query parameters that can be logged
     * by the driver. Queries with a number of parameters higher than this value
     * will not have all their parameters logged.
     */
    public static final int DEFAULT_MAX_LOGGED_PARAMETERS = 50;

    // Loggers

    /**
     * The logger used to log normal queries, i.e., queries that completed successfully
     * within a configurable threshold in milliseconds.
     * <p/>
     * This logger is activated by setting its level to {@code DEBUG} or {@code TRACE}.
     * Additionally, if the level is set to {@code TRACE} and the statement being logged is a {@link BoundStatement},
     * then the query parameters (if any) will be logged as well (names and actual values).
     * <p/>
     * The name of this logger is {@code com.datastax.driver.core.QueryLogger.NORMAL}.
     */
    public static final Logger NORMAL_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.NORMAL");

    /**
     * The logger used to log slow queries, i.e., queries that completed successfully
     * but whose execution time exceeded a configurable threshold in milliseconds.
     * <p/>
     * This logger is activated by setting its level to {@code DEBUG} or {@code TRACE}.
     * Additionally, if the level is set to {@code TRACE} and the statement being logged is a {@link BoundStatement},
     * then the query parameters (if any) will be logged as well (names and actual values).
     * <p/>
     * The name of this logger is {@code com.datastax.driver.core.QueryLogger.SLOW}.
     */
    public static final Logger SLOW_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.SLOW");

    /**
     * The logger used to log unsuccessful queries, i.e., queries that did not complete normally and threw an exception.
     * <p/>
     * This logger is activated by setting its level to {@code DEBUG} or {@code TRACE}.
     * Additionally, if the level is set to {@code TRACE} and the statement being logged is a {@link BoundStatement},
     * then the query parameters (if any) will be logged as well (names and actual values).
     * Note this this logger will also print the full stack trace of the reported exception.
     * <p/>
     * The name of this logger is {@code com.datastax.driver.core.QueryLogger.ERROR}.
     */
    public static final Logger ERROR_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.ERROR");

    // Message templates

    private static final String NORMAL_TEMPLATE = "[%s] [%s] Query completed normally, took %s ms: %s";

    private static final String SLOW_TEMPLATE_MILLIS = "[%s] [%s] Query too slow, took %s ms: %s";

    private static final String SLOW_TEMPLATE_PERCENTILE = "[%s] [%s] Query too slow, took %s ms (%s percentile = %s ms): %s";

    private static final String ERROR_TEMPLATE = "[%s] [%s] Query error after %s ms: %s";

    @VisibleForTesting
    static final String TRUNCATED_OUTPUT = "... [truncated output]";

    @VisibleForTesting
    static final String FURTHER_PARAMS_OMITTED = " [further parameters omitted]";

    protected volatile Cluster cluster;

    private volatile ProtocolVersion protocolVersion;

    protected volatile int maxQueryStringLength;

    protected volatile int maxParameterValueLength;

    protected volatile int maxLoggedParameters;

    /**
     * Private constructor. Instances of QueryLogger should be obtained via the {@link #builder()} method.
     */
    private QueryLogger(int maxQueryStringLength, int maxParameterValueLength, int maxLoggedParameters) {
        this.maxQueryStringLength = maxQueryStringLength;
        this.maxParameterValueLength = maxParameterValueLength;
        this.maxLoggedParameters = maxLoggedParameters;
    }

    /**
     * Creates a new {@link QueryLogger.Builder} instance.
     * <p/>
     * This is a convenience method for {@code new QueryLogger.Builder()}.
     *
     * @return the new QueryLogger builder.
     * @throws NullPointerException if {@code cluster} is {@code null}.
     */
    public static QueryLogger.Builder builder() {
        return new QueryLogger.Builder();
    }

    @Override
    public void onRegister(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void onUnregister(Cluster cluster) {
        // nothing to do
    }

    /**
     * A QueryLogger that uses a constant threshold in milliseconds
     * to track slow queries.
     * This implementation is the default and should be preferred to {@link DynamicThresholdQueryLogger}
     * which is still in beta state.
     */
    public static class ConstantThresholdQueryLogger extends QueryLogger {

        private volatile long slowQueryLatencyThresholdMillis;

        private ConstantThresholdQueryLogger(int maxQueryStringLength, int maxParameterValueLength, int maxLoggedParameters, long slowQueryLatencyThresholdMillis) {
            super(maxQueryStringLength, maxParameterValueLength, maxLoggedParameters);
            this.setSlowQueryLatencyThresholdMillis(slowQueryLatencyThresholdMillis);
        }

        /**
         * Return the threshold in milliseconds beyond which queries are considered 'slow'
         * and logged as such by the driver.
         * The default value is {@link #DEFAULT_SLOW_QUERY_THRESHOLD_MS}.
         *
         * @return The threshold in milliseconds beyond which queries are considered 'slow'
         * and logged as such by the driver.
         */
        public long getSlowQueryLatencyThresholdMillis() {
            return slowQueryLatencyThresholdMillis;
        }

        /**
         * Set the threshold in milliseconds beyond which queries are considered 'slow'
         * and logged as such by the driver.
         *
         * @param slowQueryLatencyThresholdMillis Slow queries threshold in milliseconds.
         *                                        It must be strictly positive.
         * @throws IllegalArgumentException if {@code slowQueryLatencyThresholdMillis <= 0}.
         */
        public void setSlowQueryLatencyThresholdMillis(long slowQueryLatencyThresholdMillis) {
            if (slowQueryLatencyThresholdMillis <= 0)
                throw new IllegalArgumentException("Invalid slowQueryLatencyThresholdMillis, should be > 0, got " + slowQueryLatencyThresholdMillis);
            this.slowQueryLatencyThresholdMillis = slowQueryLatencyThresholdMillis;
        }

        @Override
        protected void maybeLogNormalOrSlowQuery(Host host, Statement statement, long latencyMs) {
            if (latencyMs > slowQueryLatencyThresholdMillis) {
                maybeLogSlowQuery(host, statement, latencyMs);
            } else {
                maybeLogNormalQuery(host, statement, latencyMs);
            }
        }

        protected void maybeLogSlowQuery(Host host, Statement statement, long latencyMs) {
            if (SLOW_LOGGER.isDebugEnabled()) {
                String message = String.format(SLOW_TEMPLATE_MILLIS, cluster.getClusterName(), host, latencyMs, statementAsString(statement));
                logQuery(statement, null, SLOW_LOGGER, message);
            }
        }
    }

    /**
     * A QueryLogger that uses a dynamic threshold in milliseconds
     * to track slow queries.
     * <p/>
     * Dynamic thresholds are based on per-host latency percentiles, as computed
     * by {@link PerHostPercentileTracker}.
     * <p/>
     * <b>This class is currently provided as a beta preview: it hasn't been extensively tested yet, and the API is still subject
     * to change.</b>
     */
    @Beta
    public static class DynamicThresholdQueryLogger extends QueryLogger {

        private volatile double slowQueryLatencyThresholdPercentile;

        private volatile PerHostPercentileTracker perHostPercentileLatencyTracker;

        private DynamicThresholdQueryLogger(int maxQueryStringLength, int maxParameterValueLength, int maxLoggedParameters, double slowQueryLatencyThresholdPercentile, PerHostPercentileTracker perHostPercentileLatencyTracker) {
            super(maxQueryStringLength, maxParameterValueLength, maxLoggedParameters);
            this.setSlowQueryLatencyThresholdPercentile(slowQueryLatencyThresholdPercentile);
            this.setPerHostPercentileLatencyTracker(perHostPercentileLatencyTracker);
        }

        /**
         * Return the {@link PerHostPercentileTracker} instance to use for recording per-host latency histograms.
         * Cannot be {@code null}.
         *
         * @return the {@link PerHostPercentileTracker} instance to use.
         */
        public PerHostPercentileTracker getPerHostPercentileLatencyTracker() {
            return perHostPercentileLatencyTracker;
        }

        /**
         * Set the {@link PerHostPercentileTracker} instance to use for recording per-host latency histograms.
         * Cannot be {@code null}.
         *
         * @param perHostPercentileLatencyTracker the {@link PerHostPercentileTracker} instance to use.
         * @throws IllegalArgumentException if {@code perHostPercentileLatencyTracker == null}.
         */
        public void setPerHostPercentileLatencyTracker(PerHostPercentileTracker perHostPercentileLatencyTracker) {
            if (perHostPercentileLatencyTracker == null)
                throw new IllegalArgumentException("perHostPercentileLatencyTracker cannot be null");
            this.perHostPercentileLatencyTracker = perHostPercentileLatencyTracker;
        }

        /**
         * Return the threshold percentile beyond which queries are considered 'slow'
         * and logged as such by the driver.
         * The default value is {@link #DEFAULT_SLOW_QUERY_THRESHOLD_PERCENTILE}.
         *
         * @return threshold percentile beyond which queries are considered 'slow'
         * and logged as such by the driver.
         */
        public double getSlowQueryLatencyThresholdPercentile() {
            return slowQueryLatencyThresholdPercentile;
        }

        /**
         * Set the threshold percentile beyond which queries are considered 'slow'
         * and logged as such by the driver.
         *
         * @param slowQueryLatencyThresholdPercentile Slow queries threshold percentile.
         *                                            It must be comprised between 0 inclusive and 100 exclusive.
         * @throws IllegalArgumentException if {@code slowQueryLatencyThresholdPercentile < 0 || slowQueryLatencyThresholdPercentile >= 100}.
         */
        public void setSlowQueryLatencyThresholdPercentile(double slowQueryLatencyThresholdPercentile) {
            if (slowQueryLatencyThresholdPercentile < 0.0 || slowQueryLatencyThresholdPercentile >= 100.0)
                throw new IllegalArgumentException("Invalid slowQueryLatencyThresholdPercentile, should be >= 0 and < 100, got " + slowQueryLatencyThresholdPercentile);
            this.slowQueryLatencyThresholdPercentile = slowQueryLatencyThresholdPercentile;
        }

        @Override
        protected void maybeLogNormalOrSlowQuery(Host host, Statement statement, long latencyMs) {
            long threshold = perHostPercentileLatencyTracker.getLatencyAtPercentile(host, slowQueryLatencyThresholdPercentile);
            if (threshold >= 0 && latencyMs > threshold) {
                maybeLogSlowQuery(host, statement, latencyMs, threshold);
            } else {
                maybeLogNormalQuery(host, statement, latencyMs);
            }
        }

        protected void maybeLogSlowQuery(Host host, Statement statement, long latencyMs, long threshold) {
            if (SLOW_LOGGER.isDebugEnabled()) {
                String message = String.format(SLOW_TEMPLATE_PERCENTILE, cluster.getClusterName(), host, latencyMs, slowQueryLatencyThresholdPercentile, threshold, statementAsString(statement));
                logQuery(statement, null, SLOW_LOGGER, message);
            }
        }
    }

    /**
     * Helper class to build {@link QueryLogger} instances with a fluent API.
     */
    public static class Builder {

        private int maxQueryStringLength = DEFAULT_MAX_QUERY_STRING_LENGTH;

        private int maxParameterValueLength = DEFAULT_MAX_PARAMETER_VALUE_LENGTH;

        private int maxLoggedParameters = DEFAULT_MAX_LOGGED_PARAMETERS;

        private long slowQueryLatencyThresholdMillis = DEFAULT_SLOW_QUERY_THRESHOLD_MS;

        private double slowQueryLatencyThresholdPercentile = DEFAULT_SLOW_QUERY_THRESHOLD_PERCENTILE;

        private PerHostPercentileTracker perHostPercentileLatencyTracker;

        private boolean constantThreshold = true;

        /**
         * Enables slow query latency tracking based on constant thresholds.
         * <p/>
         * Note: You should either use {@link #withConstantThreshold(long) constant thresholds}
         * or {@link #withDynamicThreshold(PerHostPercentileTracker, double) dynamic thresholds},
         * not both.
         *
         * @param slowQueryLatencyThresholdMillis The threshold in milliseconds beyond which queries are considered 'slow'
         *                                        and logged as such by the driver.
         *                                        The default value is {@link #DEFAULT_SLOW_QUERY_THRESHOLD_MS}
         * @return this {@link Builder} instance (for method chaining).
         */
        public Builder withConstantThreshold(long slowQueryLatencyThresholdMillis) {
            this.slowQueryLatencyThresholdMillis = slowQueryLatencyThresholdMillis;
            constantThreshold = true;
            return this;
        }

        /**
         * Enables slow query latency tracking based on dynamic thresholds.
         * <p/>
         * Dynamic thresholds are based on per-host latency percentiles, as computed
         * by {@link PerHostPercentileTracker}.
         * <p/>
         * Note: You should either use {@link #withConstantThreshold(long) constant thresholds}
         * or {@link #withDynamicThreshold(PerHostPercentileTracker, double) dynamic thresholds},
         * not both.
         * <p/>
         * <b>This feature is currently provided as a beta preview: it hasn't been extensively tested yet, and the API is still subject
         * to change.</b>
         *
         * @param perHostPercentileLatencyTracker     the {@link PerHostPercentileTracker} instance to use for recording per-host latency histograms.
         *                                            Cannot be {@code null}.
         * @param slowQueryLatencyThresholdPercentile Slow queries threshold percentile.
         *                                            It must be comprised between 0 inclusive and 100 exclusive.
         *                                            The default value is {@link #DEFAULT_SLOW_QUERY_THRESHOLD_PERCENTILE}
         * @return this {@link Builder} instance (for method chaining).
         */
        @Beta
        public Builder withDynamicThreshold(PerHostPercentileTracker perHostPercentileLatencyTracker, double slowQueryLatencyThresholdPercentile) {
            this.perHostPercentileLatencyTracker = perHostPercentileLatencyTracker;
            this.slowQueryLatencyThresholdPercentile = slowQueryLatencyThresholdPercentile;
            constantThreshold = false;
            return this;
        }

        /**
         * Set the maximum length of a CQL query string that can be logged verbatim
         * by the driver. Query strings longer than this value will be truncated
         * when logged.
         *
         * @param maxQueryStringLength The maximum length of a CQL query string
         *                             that can be logged verbatim by the driver.
         *                             It must be strictly positive or {@code -1},
         *                             in which case the query is never truncated
         *                             (use with care).
         *                             The default value is {@link #DEFAULT_MAX_QUERY_STRING_LENGTH}.
         * @return this {@link Builder} instance (for method chaining).
         */
        public Builder withMaxQueryStringLength(int maxQueryStringLength) {
            this.maxQueryStringLength = maxQueryStringLength;
            return this;
        }

        /**
         * Set the maximum length of a query parameter value that can be logged verbatim
         * by the driver. Parameter values longer than this value will be truncated
         * when logged.
         *
         * @param maxParameterValueLength The maximum length of a query parameter value
         *                                that can be logged verbatim by the driver.
         *                                It must be strictly positive or {@code -1},
         *                                in which case the parameter value is never truncated
         *                                (use with care).
         *                                The default value is {@link #DEFAULT_MAX_PARAMETER_VALUE_LENGTH}.
         * @return this {@link Builder} instance (for method chaining).
         */
        public Builder withMaxParameterValueLength(int maxParameterValueLength) {
            this.maxParameterValueLength = maxParameterValueLength;
            return this;
        }

        /**
         * Set the maximum number of query parameters that can be logged
         * by the driver. Queries with a number of parameters higher than this value
         * will not have all their parameters logged.
         *
         * @param maxLoggedParameters The maximum number of query parameters that can be logged
         *                            by the driver. It must be strictly positive or {@code -1},
         *                            in which case all parameters will be logged, regardless of their number
         *                            (use with care).
         *                            The default value is {@link #DEFAULT_MAX_LOGGED_PARAMETERS}.
         * @return this {@link Builder} instance (for method chaining).
         */
        public Builder withMaxLoggedParameters(int maxLoggedParameters) {
            this.maxLoggedParameters = maxLoggedParameters;
            return this;
        }

        /**
         * Build the {@link QueryLogger} instance.
         *
         * @return the {@link QueryLogger} instance.
         * @throws IllegalArgumentException if the builder is unable to build a valid instance due to incorrect settings.
         */
        public QueryLogger build() {
            if (constantThreshold) {
                return new ConstantThresholdQueryLogger(maxQueryStringLength, maxParameterValueLength, maxLoggedParameters, slowQueryLatencyThresholdMillis);
            } else {
                return new DynamicThresholdQueryLogger(maxQueryStringLength, maxParameterValueLength, maxLoggedParameters, slowQueryLatencyThresholdPercentile, perHostPercentileLatencyTracker);
            }
        }

    }

    // Getters and Setters

    /**
     * Return the maximum length of a CQL query string that can be logged verbatim
     * by the driver. Query strings longer than this value will be truncated
     * when logged.
     * The default value is {@link #DEFAULT_MAX_QUERY_STRING_LENGTH}.
     *
     * @return The maximum length of a CQL query string that can be logged verbatim
     * by the driver.
     */
    public int getMaxQueryStringLength() {
        return maxQueryStringLength;
    }

    /**
     * Set the maximum length of a CQL query string that can be logged verbatim
     * by the driver. Query strings longer than this value will be truncated
     * when logged.
     *
     * @param maxQueryStringLength The maximum length of a CQL query string
     *                             that can be logged verbatim by the driver.
     *                             It must be strictly positive or {@code -1},
     *                             in which case the query is never truncated
     *                             (use with care).
     * @throws IllegalArgumentException if {@code maxQueryStringLength <= 0 && maxQueryStringLength != -1}.
     */
    public void setMaxQueryStringLength(int maxQueryStringLength) {
        if (maxQueryStringLength <= 0 && maxQueryStringLength != -1)
            throw new IllegalArgumentException("Invalid maxQueryStringLength, should be > 0 or -1, got " + maxQueryStringLength);
        this.maxQueryStringLength = maxQueryStringLength;
    }

    /**
     * Return the maximum length of a query parameter value that can be logged verbatim
     * by the driver. Parameter values longer than this value will be truncated
     * when logged.
     * The default value is {@link #DEFAULT_MAX_PARAMETER_VALUE_LENGTH}.
     *
     * @return The maximum length of a query parameter value that can be logged verbatim
     * by the driver.
     */
    public int getMaxParameterValueLength() {
        return maxParameterValueLength;
    }

    /**
     * Set the maximum length of a query parameter value that can be logged verbatim
     * by the driver. Parameter values longer than this value will be truncated
     * when logged.
     *
     * @param maxParameterValueLength The maximum length of a query parameter value
     *                                that can be logged verbatim by the driver.
     *                                It must be strictly positive or {@code -1},
     *                                in which case the parameter value is never truncated
     *                                (use with care).
     * @throws IllegalArgumentException if {@code maxParameterValueLength <= 0 && maxParameterValueLength != -1}.
     */
    public void setMaxParameterValueLength(int maxParameterValueLength) {
        if (maxParameterValueLength <= 0 && maxParameterValueLength != -1)
            throw new IllegalArgumentException("Invalid maxParameterValueLength, should be > 0 or -1, got " + maxParameterValueLength);
        this.maxParameterValueLength = maxParameterValueLength;
    }

    /**
     * Return the maximum number of query parameters that can be logged
     * by the driver. Queries with a number of parameters higher than this value
     * will not have all their parameters logged.
     * The default value is {@link #DEFAULT_MAX_LOGGED_PARAMETERS}.
     *
     * @return The maximum number of query parameters that can be logged
     * by the driver.
     */
    public int getMaxLoggedParameters() {
        return maxLoggedParameters;
    }

    /**
     * Set the maximum number of query parameters that can be logged
     * by the driver. Queries with a number of parameters higher than this value
     * will not have all their parameters logged.
     *
     * @param maxLoggedParameters the maximum number of query parameters that can be logged
     *                            by the driver. It must be strictly positive or {@code -1},
     *                            in which case all parameters will be logged, regardless of their number
     *                            (use with care).
     * @throws IllegalArgumentException if {@code maxLoggedParameters <= 0 && maxLoggedParameters != -1}.
     */
    public void setMaxLoggedParameters(int maxLoggedParameters) {
        if (maxLoggedParameters <= 0 && maxLoggedParameters != -1)
            throw new IllegalArgumentException("Invalid maxLoggedParameters, should be > 0 or -1, got " + maxLoggedParameters);
        this.maxLoggedParameters = maxLoggedParameters;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
        if (cluster == null)
            throw new IllegalStateException("This method should only be called after the logger has been registered with a cluster");

        long latencyMs = NANOSECONDS.toMillis(newLatencyNanos);
        if (exception == null) {
            maybeLogNormalOrSlowQuery(host, statement, latencyMs);
        } else {
            maybeLogErrorQuery(host, statement, exception, latencyMs);
        }
    }

    protected abstract void maybeLogNormalOrSlowQuery(Host host, Statement statement, long latencyMs);

    protected void maybeLogNormalQuery(Host host, Statement statement, long latencyMs) {
        if (NORMAL_LOGGER.isDebugEnabled()) {
            String message = String.format(NORMAL_TEMPLATE, cluster.getClusterName(), host, latencyMs, statementAsString(statement));
            logQuery(statement, null, NORMAL_LOGGER, message);
        }
    }

    protected void maybeLogErrorQuery(Host host, Statement statement, Exception exception, long latencyMs) {
        if (ERROR_LOGGER.isDebugEnabled()) {
            String message = String.format(ERROR_TEMPLATE, cluster.getClusterName(), host, latencyMs, statementAsString(statement));
            logQuery(statement, exception, ERROR_LOGGER, message);
        }
    }

    protected void logQuery(Statement statement, Exception exception, Logger logger, String message) {
        boolean showParameterValues = logger.isTraceEnabled();
        if (showParameterValues) {
            StringBuilder params = new StringBuilder();
            if (statement instanceof BoundStatement) {
                appendParameters((BoundStatement) statement, params, maxLoggedParameters);
            } else if (statement instanceof BatchStatement) {
                BatchStatement batchStatement = (BatchStatement) statement;
                int remaining = maxLoggedParameters;
                for (Statement inner : batchStatement.getStatements()) {
                    if (inner instanceof BoundStatement) {
                        remaining = appendParameters((BoundStatement) inner, params, remaining);
                    }
                }
            }
            if (params.length() > 0)
                params.append("]");
            logger.trace(message + params, exception);
        } else {
            logger.debug(message, exception);
        }
    }

    protected String statementAsString(Statement statement) {
        StringBuilder sb = new StringBuilder();
        if (statement instanceof BatchStatement) {
            BatchStatement bs = (BatchStatement) statement;
            int statements = bs.getStatements().size();
            int boundValues = countBoundValues(bs);
            sb.append("[" + statements + " statements, " + boundValues + " bound values] ");
        } else if (statement instanceof BoundStatement) {
            int boundValues = ((BoundStatement) statement).wrapper.values.length;
            sb.append("[" + boundValues + " bound values] ");
        }

        append(statement, sb, maxQueryStringLength);
        return sb.toString();
    }

    protected int countBoundValues(BatchStatement bs) {
        int count = 0;
        for (Statement s : bs.getStatements()) {
            if (s instanceof BoundStatement)
                count += ((BoundStatement) s).wrapper.values.length;
        }
        return count;
    }

    protected int appendParameters(BoundStatement statement, StringBuilder buffer, int remaining) {
        if (remaining == 0)
            return 0;
        ColumnDefinitions metadata = statement.preparedStatement().getVariables();
        int numberOfParameters = metadata.size();
        if (numberOfParameters > 0) {
            List<ColumnDefinitions.Definition> definitions = metadata.asList();
            int numberOfLoggedParameters;
            if (remaining == -1) {
                numberOfLoggedParameters = numberOfParameters;
            } else {
                numberOfLoggedParameters = Math.min(remaining, numberOfParameters);
                remaining -= numberOfLoggedParameters;
            }
            for (int i = 0; i < numberOfLoggedParameters; i++) {
                if (buffer.length() == 0)
                    buffer.append(" [");
                else
                    buffer.append(", ");
                String value = statement.isSet(i)
                        ? parameterValueAsString(definitions.get(i), statement.wrapper.values[i])
                        : "<UNSET>";
                buffer.append(String.format("%s:%s", metadata.getName(i), value));
            }
            if (numberOfLoggedParameters < numberOfParameters) {
                buffer.append(FURTHER_PARAMS_OMITTED);
            }
        }
        return remaining;
    }

    protected String parameterValueAsString(ColumnDefinitions.Definition definition, ByteBuffer raw) {
        String valueStr;
        if (raw == null || raw.remaining() == 0) {
            valueStr = "NULL";
        } else {
            DataType type = definition.getType();
            CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
            TypeCodec<Object> codec = codecRegistry.codecFor(type);
            int maxParameterValueLength = this.maxParameterValueLength;
            if (type.equals(DataType.blob()) && maxParameterValueLength != -1) {
                // prevent large blobs from being converted to strings
                int maxBufferLength = Math.max(2, (maxParameterValueLength - 2) / 2);
                boolean bufferTooLarge = raw.remaining() > maxBufferLength;
                if (bufferTooLarge) {
                    raw = (ByteBuffer) raw.duplicate().limit(maxBufferLength);
                }
                Object value = codec.deserialize(raw, protocolVersion());
                valueStr = codec.format(value);
                if (bufferTooLarge) {
                    valueStr = valueStr + TRUNCATED_OUTPUT;
                }
            } else {
                Object value = codec.deserialize(raw, protocolVersion());
                valueStr = codec.format(value);
                if (maxParameterValueLength != -1 && valueStr.length() > maxParameterValueLength) {
                    valueStr = valueStr.substring(0, maxParameterValueLength) + TRUNCATED_OUTPUT;
                }
            }
        }
        return valueStr;
    }

    private ProtocolVersion protocolVersion() {
        // Since the QueryLogger can be registered before the Cluster was initialized, we can't retrieve
        // it at construction time. Cache it field at first use (a volatile field is good enough since we
        // don't need mutual exclusion).
        if (protocolVersion == null) {
            protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
            // At least one connection was established when QueryLogger is invoked
            assert protocolVersion != null : "protocol version should be defined";
        }
        return protocolVersion;
    }

    protected int append(Statement statement, StringBuilder buffer, int remaining) {
        if (statement instanceof StatementWrapper)
            statement = ((StatementWrapper) statement).getWrappedStatement();

        if (statement instanceof RegularStatement) {
            RegularStatement rs = (RegularStatement) statement;
            String query = rs.getQueryString();
            remaining = append(query.trim(), buffer, remaining);
        } else if (statement instanceof BoundStatement) {
            remaining = append(((BoundStatement) statement).preparedStatement().getQueryString().trim(), buffer, remaining);
        } else if (statement instanceof BatchStatement) {
            BatchStatement batchStatement = (BatchStatement) statement;
            remaining = append("BEGIN", buffer, remaining);
            switch (batchStatement.batchType) {
                case UNLOGGED:
                    append(" UNLOGGED", buffer, remaining);
                    break;
                case COUNTER:
                    append(" COUNTER", buffer, remaining);
                    break;
            }
            remaining = append(" BATCH", buffer, remaining);
            for (Statement stmt : batchStatement.getStatements()) {
                remaining = append(" ", buffer, remaining);
                remaining = append(stmt, buffer, remaining);
            }
            remaining = append(" APPLY BATCH", buffer, remaining);
        } else {
            // Unknown types of statement
            // Call toString() as a last resort
            remaining = append(statement.toString(), buffer, remaining);
        }
        if (buffer.charAt(buffer.length() - 1) != ';') {
            remaining = append(";", buffer, remaining);
        }
        return remaining;
    }

    protected int append(CharSequence str, StringBuilder buffer, int remaining) {
        if (remaining == -2) {
            // capacity exceeded
        } else if (remaining == -1) {
            // unlimited capacity
            buffer.append(str);
        } else if (str.length() > remaining) {
            buffer.append(str, 0, remaining).append(TRUNCATED_OUTPUT);
            remaining = -2;
        } else {
            buffer.append(str);
            remaining -= str.length();
        }
        return remaining;
    }

}
