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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A configurable {@link LatencyTracker} that logs all executed statements.
 * <p/>
 * Typically, client applications would instantiate one single query logger (using its {@link Builder builder}),
 * configure it and register it on the relevant {@link Cluster} instance, e.g.:
 * <p/>
 * <pre>
 * Cluster cluster = ...
 * EnhancedQueryLogger queryLogger = EnhancedQueryLogger.builder()
 *     .withConstantThreshold(...)
 *     .build();
 * cluster.register(queryLogger);
 * </pre>
 * <p/>
 * Refer to the {@link Builder} documentation for more information on
 * configuration settings for the query logger.
 * <p/>
 * Once registered, the query logger will log every statement executed by the driver;
 * note that it will never log null statements nor any special statement used internally by the driver.
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
 * The appropriate logger is chosen according to the following algorithm:
 * <ol>
 * <li>if an exception has been thrown: use {@link #ERROR_LOGGER};</li>
 * <li>otherwise, if the reported latency is greater than the configured threshold in milliseconds: use {@link #SLOW_LOGGER};</li>
 * <li>otherwise, use {@link #NORMAL_LOGGER}.</li>
 * </ol>
 * <p/>
 * All loggers are activated by setting their levels to {@code DEBUG} or {@code TRACE} (including {@link #ERROR_LOGGER}).
 * If the level is set to {@code TRACE}, then the query parameters (if any) will be logged as well (names and actual values).
 * <p/>
 * <strong>Constant thresholds vs. Dynamic thresholds</strong>
 * <p/>
 * Currently the EnhancedQueryLogger can track slow queries in two different ways:
 * using a {@link Builder#withConstantThreshold(long)} constant threshold} in milliseconds (which is the default
 * behavior), or using a {@link Builder#withDynamicThreshold(PercentileTracker, double) dynamic threshold}
 * based on latency percentiles.
 * <p/>
 * This class is thread-safe.
 *
 * @since 3.2.0
 */
public abstract class EnhancedQueryLogger implements LatencyTracker {

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

    // Loggers

    /**
     * The logger used to log normal queries, i.e., queries that completed successfully
     * within a configurable threshold in milliseconds.
     * <p/>
     * This logger is activated by setting its level to {@code DEBUG} or {@code TRACE}.
     * Additionally, if the level is set to {@code TRACE}, then the query parameters (if any)
     * will be logged.
     * <p/>
     * The name of this logger is {@code com.datastax.driver.core.QueryLogger.NORMAL}.
     */
    public static final Logger NORMAL_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.NORMAL");

    /**
     * The logger used to log slow queries, i.e., queries that completed successfully
     * but whose execution time exceeded a configurable threshold in milliseconds.
     * <p/>
     * This logger is activated by setting its level to {@code DEBUG} or {@code TRACE}.
     * Additionally, if the level is set to {@code TRACE}, then the query parameters (if any)
     * will be logged.
     * <p/>
     * The name of this logger is {@code com.datastax.driver.core.QueryLogger.SLOW}.
     */
    public static final Logger SLOW_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.SLOW");

    /**
     * The logger used to log unsuccessful queries, i.e., queries that did not complete normally and threw an exception.
     * <p/>
     * This logger is activated by setting its level to {@code DEBUG} or {@code TRACE}.
     * Additionally, if the level is set to {@code TRACE}, then the query parameters (if any)
     * will be logged.
     * <p/>
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

    protected volatile Cluster cluster;

    private final StatementFormatter formatter;

    private EnhancedQueryLogger(StatementFormatter formatter) {
        this.formatter = formatter;
    }

    /**
     * Creates a new {@link EnhancedQueryLogger.Builder} instance.
     * <p/>
     * This is a convenience method for {@code new EnhancedQueryLogger.Builder()}.
     *
     * @return the new EnhancedQueryLogger builder.
     * @throws NullPointerException if {@code cluster} is {@code null}.
     */
    public static EnhancedQueryLogger.Builder builder() {
        return new EnhancedQueryLogger.Builder();
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
     * This implementation is the default.
     */
    public static class ConstantThresholdQueryLogger extends EnhancedQueryLogger {

        private volatile long slowQueryLatencyThresholdMillis;

        private ConstantThresholdQueryLogger(long slowQueryLatencyThresholdMillis, StatementFormatter formatter) {
            super(formatter);
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
                String statementAsString = statementAsString(statement, SLOW_LOGGER);
                logQuery(SLOW_LOGGER, SLOW_TEMPLATE_MILLIS, null, cluster.getClusterName(), host, latencyMs, statementAsString);
            }
        }
    }

    /**
     * A QueryLogger that uses a dynamic threshold in milliseconds
     * to track slow queries.
     * <p/>
     * Dynamic thresholds are based on per-host latency percentiles, as computed
     * by {@link PercentileTracker}.
     */
    public static class DynamicThresholdQueryLogger extends EnhancedQueryLogger {

        private volatile double slowQueryLatencyThresholdPercentile;

        private volatile PercentileTracker percentileLatencyTracker;

        private DynamicThresholdQueryLogger(double slowQueryLatencyThresholdPercentile,
                                            PercentileTracker percentileLatencyTracker,
                                            StatementFormatter formatter) {
            super(formatter);
            this.setSlowQueryLatencyThresholdPercentile(slowQueryLatencyThresholdPercentile);
            this.setPercentileLatencyTracker(percentileLatencyTracker);
        }

        /**
         * Return the percentile tracker to use for recording per-host latency histograms.
         * Cannot be {@code null}.
         *
         * @return the percentile tracker to use.
         */
        public PercentileTracker getPercentileLatencyTracker() {
            return percentileLatencyTracker;
        }

        /**
         * Set the percentile tracker to use for recording per-host latency histograms.
         * Cannot be {@code null}.
         *
         * @param percentileLatencyTracker the percentile tracker instance to use.
         * @throws IllegalArgumentException if {@code percentileLatencyTracker == null}.
         */
        public void setPercentileLatencyTracker(PercentileTracker percentileLatencyTracker) {
            if (percentileLatencyTracker == null)
                throw new IllegalArgumentException("perHostPercentileLatencyTracker cannot be null");
            this.percentileLatencyTracker = percentileLatencyTracker;
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
            long threshold = percentileLatencyTracker.getLatencyAtPercentile(host, statement, null, slowQueryLatencyThresholdPercentile);
            if (threshold >= 0 && latencyMs > threshold) {
                maybeLogSlowQuery(host, statement, latencyMs, threshold);
            } else {
                maybeLogNormalQuery(host, statement, latencyMs);
            }
        }

        protected void maybeLogSlowQuery(Host host, Statement statement, long latencyMs, long threshold) {
            if (SLOW_LOGGER.isDebugEnabled()) {
                String statementAsString = statementAsString(statement, SLOW_LOGGER);
                logQuery(SLOW_LOGGER, SLOW_TEMPLATE_PERCENTILE, null, cluster.getClusterName(), host, latencyMs,
                        slowQueryLatencyThresholdPercentile, threshold, statementAsString);
            }
        }

        @Override
        public void onRegister(Cluster cluster) {
            super.onRegister(cluster);
            cluster.register(percentileLatencyTracker);
        }

        // Don't unregister the latency tracker in onUnregister, we can't guess if it's being used by another component
        // or not.
    }

    /**
     * Helper class to build {@link EnhancedQueryLogger} instances with a fluent API.
     */
    public static class Builder {

        private StatementFormatter formatter = StatementFormatter.DEFAULT_INSTANCE;

        private long slowQueryLatencyThresholdMillis = DEFAULT_SLOW_QUERY_THRESHOLD_MS;

        private double slowQueryLatencyThresholdPercentile = DEFAULT_SLOW_QUERY_THRESHOLD_PERCENTILE;

        private PercentileTracker percentileLatencyTracker;

        private boolean constantThreshold = true;

        /**
         * Enables slow query latency tracking based on constant thresholds.
         * <p/>
         * Note: You should either use constant thresholds
         * or {@link #withDynamicThreshold(PercentileTracker, double) dynamic thresholds},
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
         * Dynamic thresholds are based on latency percentiles, as computed by {@link PercentileTracker}.
         * <p/>
         * Note: You should either use {@link #withConstantThreshold(long) constant thresholds} or
         * dynamic thresholds, not both.
         *
         * @param percentileLatencyTracker            the {@link PercentileTracker} instance to use for recording
         *                                            latency histograms. Cannot be {@code null}.
         *                                            It will get {@link Cluster#register(LatencyTracker) registered}
         *                                            with the cluster at the same time as this logger.
         * @param slowQueryLatencyThresholdPercentile Slow queries threshold percentile.
         *                                            It must be comprised between 0 inclusive and 100 exclusive.
         *                                            The default value is {@link #DEFAULT_SLOW_QUERY_THRESHOLD_PERCENTILE}
         * @return this {@link Builder} instance (for method chaining).
         */
        public Builder withDynamicThreshold(PercentileTracker percentileLatencyTracker,
                                            double slowQueryLatencyThresholdPercentile) {
            this.percentileLatencyTracker = percentileLatencyTracker;
            this.slowQueryLatencyThresholdPercentile = slowQueryLatencyThresholdPercentile;
            constantThreshold = false;
            return this;
        }

        /**
         * Sets the {@link StatementFormatter formatter} to use
         * to format logged statements.
         * <p/>
         * If this method is not called, then the
         * {@link StatementFormatter#DEFAULT_INSTANCE default formatter}
         * will be used.
         *
         * @param formatter the {@link StatementFormatter formatter} to use.
         * @return this {@link Builder} instance (for method chaining).
         */
        public Builder withStatementFormatter(StatementFormatter formatter) {
            this.formatter = formatter;
            return this;
        }

        /**
         * Builds the {@link EnhancedQueryLogger} instance.
         *
         * @return the {@link EnhancedQueryLogger} instance.
         */
        public EnhancedQueryLogger build() {
            if (constantThreshold) {
                return new ConstantThresholdQueryLogger(slowQueryLatencyThresholdMillis, formatter);
            } else {
                return new DynamicThresholdQueryLogger(slowQueryLatencyThresholdPercentile,
                        percentileLatencyTracker, formatter);
            }
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
        if (cluster == null)
            throw new IllegalStateException("This method should only be called after the logger has been registered with a cluster");

        if (statement instanceof StatementWrapper)
            statement = ((StatementWrapper) statement).getWrappedStatement();

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
            String statementAsString = statementAsString(statement, NORMAL_LOGGER);
            logQuery(NORMAL_LOGGER, NORMAL_TEMPLATE, null, cluster.getClusterName(), host, latencyMs, statementAsString);
        }
    }

    protected void maybeLogErrorQuery(Host host, Statement statement, Exception exception, long latencyMs) {
        if (ERROR_LOGGER.isDebugEnabled()) {
            String statementAsString = statementAsString(statement, ERROR_LOGGER);
            logQuery(ERROR_LOGGER, ERROR_TEMPLATE, exception, cluster.getClusterName(), host, latencyMs, statementAsString);
        }
    }

    protected void logQuery(Logger logger, String template, Exception exception, Object... templateParameters) {
        String message = String.format(template, templateParameters);
        if (logger.isTraceEnabled()) {
            logger.trace(message, exception);
        } else {
            logger.debug(message, exception);
        }
    }

    protected String statementAsString(Statement statement, Logger logger) {
        ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
        StatementFormatter.StatementFormatVerbosity verbosity = logger.isTraceEnabled() ?
                StatementFormatter.StatementFormatVerbosity.EXTENDED :
                StatementFormatter.StatementFormatVerbosity.NORMAL;
        return formatter.format(statement, verbosity, protocolVersion, codecRegistry);
    }

}
