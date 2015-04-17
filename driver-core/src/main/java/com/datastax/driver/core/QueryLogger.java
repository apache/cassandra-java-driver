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
import java.util.List;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A configurable {@link LatencyTracker} that logs all executed statements.
 * <p>
 * Typically, client applications would instantiate one single query logger (using its {@link Builder}),
 * configure it and register it on the relevant {@link Cluster} instance, e.g.:
 *
 * <pre>
 * Cluster cluster = ...
 * QueryLogger queryLogger = QueryLogger.builder(cluster)
 *     .withSlowQueryLatencyThresholdMillis(...)
 *     .withMaxQueryStringLength(...)
 *     .build();
 * cluster.register(queryLogger);
 * </pre>
 *
 * Refer to the {@link Builder} documentation for more information on
 * configuration settings for the query logger.
 * <p>
 * Once registered, the query logger will log every {@link RegularStatement}, {@link BoundStatement} or {@link BatchStatement}
 * executed by the driver;
 * note that it will never log other types of statement, null statements nor any special statement used internally by the driver.
 * <p>
 * There is one log for each request to a Cassandra node; because the driver sometimes retries the same statement on multiple nodes,
 * a single statement execution (for example, a single call to {@link Session#execute(Statement)}) can produce multiple logs on
 * different nodes.
 * <p>
 * For more flexibility, the query logger uses 3 different {@link Logger} instances:
 *
 * <ol>
 *     <li>{@link #NORMAL_LOGGER}: used to log normal queries, i.e., queries that completed successfully
 *     within a {@link #getSlowQueryLatencyThresholdMillis() configurable threshold in milliseconds}.</li>
 *     <li>{@link #SLOW_LOGGER}: used to log slow queries, i.e., queries that completed successfully
 *     but that took longer than a {@link #getSlowQueryLatencyThresholdMillis() configurable threshold in milliseconds} to complete.</li>
 *     <li>{@link #ERROR_LOGGER}: used to log unsuccessful queries, i.e.,
 *     queries that did not completed normally and threw an exception.
 *     Note this this logger will also print the full stack trace of the reported exception.</li>
 * </ol>
 *
 * <p>
 * The appropriate logger is chosen according to the following algorithm:
 * <ol>
 *     <li>if an exception has been thrown: use {@link #ERROR_LOGGER};</li>
 *     <li>otherwise, if the reported latency is greater than {@link #getSlowQueryLatencyThresholdMillis()} the configured threshold in milliseconds}: use {@link #SLOW_LOGGER};</li>
 *     <li>otherwise, use {@link #NORMAL_LOGGER}.</li>
 * </ol>
 *
 * <p>
 * All loggers are activated by setting their levels to {@code DEBUG} or {@code TRACE} (including {@link #ERROR_LOGGER}).
 * If the level is set to {@code TRACE} and the statement being logged is a {@link BoundStatement},
 * then the query parameters (if any) will be logged as well (names and actual values).
 *
 * <p>
 * This class is thread-safe.
 *
 * @since 2.0.10
 */
public class QueryLogger implements LatencyTracker {

    /**
     * The default threshold in milliseconds beyond which queries are considered 'slow'
     * and logged as such by the driver.
     */
    public static final long DEFAULT_SLOW_QUERY_THRESHOLD_MS = 5000;

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
     * within a configurable threshold in milliseconds (see {@link #getSlowQueryLatencyThresholdMillis()}).
     * <p>
     * This logger is activated by setting its level to {@code DEBUG} or {@code TRACE}.
     * Additionally, if the level is set to {@code TRACE} and the statement being logged is a {@link BoundStatement},
     * then the query parameters (if any) will be logged as well (names and actual values).
     * <p>
     * The name of this logger is {@code com.datastax.driver.core.QueryLogger.NORMAL}.
     */
    public static final Logger NORMAL_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.NORMAL");

    /**
     * The logger used to log slow queries, i.e., queries that completed successfully
     * but whose execution time exceeded a configurable threshold in milliseconds
     * (see {@link #getSlowQueryLatencyThresholdMillis()}).
     * <p>
     * This logger is activated by setting its level to {@code DEBUG} or {@code TRACE}.
     * Additionally, if the level is set to {@code TRACE} and the statement being logged is a {@link BoundStatement},
     * then the query parameters (if any) will be logged as well (names and actual values).
     * <p>
     * The name of this logger is {@code com.datastax.driver.core.QueryLogger.SLOW}.
     */
    public static final Logger SLOW_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.SLOW");

    /**
     * The logger used to log unsuccessful queries, i.e., queries that did not completed normally and threw an exception.
     * <p>
     * This logger is activated by setting its level to {@code DEBUG} or {@code TRACE}.
     * Additionally, if the level is set to {@code TRACE} and the statement being logged is a {@link BoundStatement},
     * then the query parameters (if any) will be logged as well (names and actual values).
     * Note this this logger will also print the full stack trace of the reported exception.
     * <p>
     * The name of this logger is {@code com.datastax.driver.core.QueryLogger.ERROR}.
     */
    public static final Logger ERROR_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.ERROR");

    // Message templates

    private static final String NORMAL_TEMPLATE = "[%s] [%s] Query completed normally, took %s ms: %s";

    private static final String SLOW_TEMPLATE = "[%s] [%s] Query too slow, took %s ms: %s";

    private static final String ERROR_TEMPLATE = "[%s] [%s] Query error after %s ms: %s";

    @VisibleForTesting
    static final String TRUNCATED_OUTPUT = "... [truncated output]";

    @VisibleForTesting
    static final String FURTHER_PARAMS_OMITTED = " [further parameters omitted]";

    private final Cluster cluster;

    private volatile long slowQueryLatencyThresholdMillis = DEFAULT_SLOW_QUERY_THRESHOLD_MS;

    private volatile int maxQueryStringLength = DEFAULT_MAX_QUERY_STRING_LENGTH;

    private volatile int maxParameterValueLength = DEFAULT_MAX_PARAMETER_VALUE_LENGTH;

    private volatile int maxLoggedParameters = DEFAULT_MAX_LOGGED_PARAMETERS;

    /**
     * Private constructor. Instances of QueryLogger should be obtained via the {@link #builder(Cluster)} method.
     * @param cluster The cluster this logger is attached to
     */
    private QueryLogger(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Creates a new {@link QueryLogger.Builder} instance.
     * <p>
     * This is a convenience method for {@code new QueryLogger.Builder()}.
     *
     * @param cluster the {@link Cluster} this QueryLogger will be attached to.
     *                Note: this parameter is currently not used, but it is included in the public API in anticipation of future use.
     * @return the new QueryLogger builder.
     * @throws NullPointerException if {@code cluster} is {@code null}.
     */
    public static QueryLogger.Builder builder(Cluster cluster) {
        if(cluster == null) throw new NullPointerException("QueryLogger.Builder: cluster parameter cannot be null");
        return new QueryLogger.Builder(cluster);
    }

    /**
     * Helper class to build {@link QueryLogger} instances with a fluent API.
     */
    public static class Builder {

        private final QueryLogger instance;

        public Builder(Cluster cluster) {
            instance = new QueryLogger(cluster);
        }

        /**
         * Set the threshold in milliseconds beyond which queries are considered 'slow'
         * and logged as such by the driver.
         * The default for this setting is {@link #DEFAULT_SLOW_QUERY_THRESHOLD_MS}.
         *
         * @param slowQueryLatencyThresholdMillis Slow queries threshold in milliseconds.
         *                                        It must be strictly positive.
         * @return this {@link Builder} instance (for method chaining).
         * @throws IllegalArgumentException if {@code slowQueryLatencyThresholdMillis <= 0}.
         */
        public Builder withSlowQueryLatencyThresholdMillis(long slowQueryLatencyThresholdMillis) {
            instance.setSlowQueryLatencyThresholdMillis(slowQueryLatencyThresholdMillis);
            return this;
        }

        /**
         * Set the maximum length of a CQL query string that can be logged verbatim
         * by the driver. Query strings longer than this value will be truncated
         * when logged.
         * The default for this setting is {@link #DEFAULT_MAX_QUERY_STRING_LENGTH}.
         *
         * @param maxQueryStringLength The maximum length of a CQL query string
         *                             that can be logged verbatim by the driver.
         *                             It must be strictly positive or {@code -1},
         *                             in which case the query is never truncated
         *                             (use with care).
         * @return this {@link Builder} instance (for method chaining).
         * @throws IllegalArgumentException if {@code maxQueryStringLength <= 0 && maxQueryStringLength != -1}.
         */
        public Builder withMaxQueryStringLength(int maxQueryStringLength) {
            instance.setMaxQueryStringLength(maxQueryStringLength);
            return this;
        }

        /**
         * Set the maximum length of a query parameter value that can be logged verbatim
         * by the driver. Parameter values longer than this value will be truncated
         * when logged.
         * The default for this setting is {@link #DEFAULT_MAX_PARAMETER_VALUE_LENGTH}.
         *
         * @param maxParameterValueLength The maximum length of a query parameter value
         *                                that can be logged verbatim by the driver.
         *                                It must be strictly positive or {@code -1},
         *                                in which case the parameter value is never truncated
         *                                (use with care).
         * @return this {@link Builder} instance (for method chaining).
         * @throws IllegalArgumentException if {@code maxParameterValueLength <= 0 && maxParameterValueLength != -1}.
         */
        public Builder withMaxParameterValueLength(int maxParameterValueLength) {
            instance.setMaxParameterValueLength(maxParameterValueLength);
            return this;
        }

        /**
         * Set the maximum number of query parameters that can be logged
         * by the driver. Queries with a number of parameters higher than this value
         * will not have all their parameters logged.
         * The default for this setting is {@link #DEFAULT_MAX_LOGGED_PARAMETERS}.
         *
         * @param maxLoggedParameters The maximum number of query parameters that can be logged
         *                            by the driver. It must be strictly positive or {@code -1},
         *                            in which case all parameters will be logged, regardless of their number
         *                            (use with care).
         * @return this {@link Builder} instance (for method chaining).
         * @throws IllegalArgumentException if {@code maxLoggedParameters <= 0 && maxLoggedParameters != -1}.
         */
        public Builder withMaxLoggedParameters(int maxLoggedParameters) {
            instance.setMaxLoggedParameters(maxLoggedParameters);
            return this;
        }

        public QueryLogger build() {
            return instance;
        }
    }

    // Getters and Setters

    /**
     * Return the threshold in milliseconds beyond which queries are considered 'slow'
     * and logged as such by the driver.
     * The default for this setting is {@link #DEFAULT_SLOW_QUERY_THRESHOLD_MS}.
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

    /**
     * Return the maximum length of a CQL query string that can be logged verbatim
     * by the driver. Query strings longer than this value will be truncated
     * when logged.
     * The default for this setting is {@link #DEFAULT_MAX_QUERY_STRING_LENGTH}.
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
     * The default for this setting is {@link #DEFAULT_MAX_PARAMETER_VALUE_LENGTH}.
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
     * The default for this setting is {@link #DEFAULT_MAX_LOGGED_PARAMETERS}.
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
        Logger logger;
        String template;
        long latencyMs = NANOSECONDS.toMillis(newLatencyNanos);
        if (exception == null) {
            if (latencyMs > slowQueryLatencyThresholdMillis) {
                logger = SLOW_LOGGER;
                template = SLOW_TEMPLATE;
            } else {
                logger = NORMAL_LOGGER;
                template = NORMAL_TEMPLATE;
            }
        } else {
            logger = ERROR_LOGGER;
            template = ERROR_TEMPLATE;
        }
        logQuery(host, statement, exception, latencyMs, logger, template);
    }

    private void logQuery(Host host, Statement statement, Exception exception, long latencyMs, Logger logger, String template) {
        if (logger.isDebugEnabled()) {
            String message = String.format(template, cluster.getClusterName(), host, latencyMs, statementAsString(statement));
            boolean showParameterValues = logger.isTraceEnabled();
            if (showParameterValues) {
                StringBuilder params = new StringBuilder();
                if (statement instanceof BoundStatement) {
                    appendParameters((BoundStatement)statement, params, maxLoggedParameters);
                } else if (statement instanceof BatchStatement) {
                    BatchStatement batchStatement = (BatchStatement)statement;
                    int remaining = maxLoggedParameters;
                    for (Statement inner : batchStatement.getStatements()) {
                        if (inner instanceof BoundStatement) {
                            remaining = appendParameters((BoundStatement)inner, params, remaining);
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
    }

    private String statementAsString(Statement statement) {
        StringBuilder sb = new StringBuilder();
        if (statement instanceof BatchStatement) {
            BatchStatement bs = (BatchStatement)statement;
            int statements = bs.getStatements().size();
            int boundValues = countBoundValues(bs);
            sb.append("[" + statements + " statements, " + boundValues + " bound values] ");
        } else if (statement instanceof BoundStatement) {
            int boundValues = ((BoundStatement)statement).values.length;
            sb.append("[" + boundValues + " bound values] ");
        }

        append(statement, sb, maxQueryStringLength);
        return sb.toString();
    }

    private int countBoundValues(BatchStatement bs) {
        int count = 0;
        for (Statement s : bs.getStatements()) {
            if (s instanceof BoundStatement)
                count += ((BoundStatement)s).values.length;
        }
        return count;
    }

    private int appendParameters(BoundStatement statement, StringBuilder buffer, int remaining) {
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
                numberOfLoggedParameters = remaining > numberOfParameters ? numberOfParameters : remaining;
                remaining -= numberOfLoggedParameters;
            }
            for (int i = 0; i < numberOfLoggedParameters; i++) {
                if (buffer.length() == 0)
                    buffer.append(" [");
                else
                    buffer.append(", ");
                buffer.append(String.format("%s:%s", metadata.getName(i), parameterValueAsString(definitions.get(i), statement.values[i])));
            }
            if (numberOfLoggedParameters < numberOfParameters) {
                buffer.append(FURTHER_PARAMS_OMITTED);
            }
        }
        return remaining;
    }

    private String parameterValueAsString(ColumnDefinitions.Definition definition, ByteBuffer raw) {
        String valueStr;
        if (raw == null || raw.remaining() == 0) {
            valueStr = "NULL";
        } else {
            DataType type = definition.getType();
            int maxParameterValueLength = this.maxParameterValueLength;
            if (type.equals(DataType.blob()) && maxParameterValueLength != -1) {
                // prevent large blobs from being converted to strings
                int maxBufferLength = Math.max(2, (maxParameterValueLength - 2) / 2);
                boolean bufferTooLarge = raw.remaining() > maxBufferLength;
                if (bufferTooLarge) {
                    raw = (ByteBuffer)raw.duplicate().limit(maxBufferLength);
                }
                Object value = type.deserialize(raw);
                valueStr = type.format(value);
                if (bufferTooLarge) {
                    valueStr = valueStr + TRUNCATED_OUTPUT;
                }
            } else {
                Object value = type.deserialize(raw);
                valueStr = type.format(value);
                if (maxParameterValueLength != -1 && valueStr.length() > maxParameterValueLength) {
                    valueStr = valueStr.substring(0, maxParameterValueLength) + TRUNCATED_OUTPUT;
                }
            }
        }
        return valueStr;
    }

    private int append(Statement statement, StringBuilder buffer, int remaining) {
        if (statement instanceof RegularStatement) {
            remaining = append(((RegularStatement)statement).getQueryString().trim(), buffer, remaining);
        } else if (statement instanceof BoundStatement) {
            remaining = append(((BoundStatement)statement).preparedStatement().getQueryString().trim(), buffer, remaining);
        } else if (statement instanceof BatchStatement) {
            BatchStatement batchStatement = (BatchStatement)statement;
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

    private int append(CharSequence str, StringBuilder buffer, int remaining) {
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