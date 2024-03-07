/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.config;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A type-safe wrapper around {@link DriverOption}, that encodes the intended value type of each
 * option.
 *
 * <p>This type was introduced in conjunction with {@link DriverConfigLoader#fromMap(OptionsMap)}.
 * Unfortunately, for backward compatibility reasons, it wasn't possible to retrofit the rest of the
 * driver to use it; therefore the APIs used to read the configuration, such as {@link DriverConfig}
 * and {@link DriverExecutionProfile}, still use the untyped {@link DriverOption}.
 *
 * @since 4.6.0
 */
public class TypedDriverOption<ValueT> {

  private static volatile Iterable<TypedDriverOption<?>> builtInValues;

  /**
   * Returns the list of all built-in options known to the driver codebase; in other words, all the
   * {@link TypedDriverOption} constants defined on this class.
   *
   * <p>Note that 3rd-party driver extensions might define their own {@link TypedDriverOption}
   * constants for custom options.
   *
   * <p>This method uses reflection to introspect all the constants on this class; the result is
   * computed lazily on the first invocation, and then cached for future calls.
   */
  public static Iterable<TypedDriverOption<?>> builtInValues() {
    if (builtInValues == null) {
      builtInValues = introspectBuiltInValues();
    }
    return builtInValues;
  }

  private final DriverOption rawOption;
  private final GenericType<ValueT> expectedType;

  public TypedDriverOption(
      @NonNull DriverOption rawOption, @NonNull GenericType<ValueT> expectedType) {
    this.rawOption = Objects.requireNonNull(rawOption);
    this.expectedType = Objects.requireNonNull(expectedType);
  }

  @NonNull
  public DriverOption getRawOption() {
    return rawOption;
  }

  @NonNull
  public GenericType<ValueT> getExpectedType() {
    return expectedType;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TypedDriverOption) {
      TypedDriverOption<?> that = (TypedDriverOption<?>) other;
      return this.rawOption.equals(that.rawOption) && this.expectedType.equals(that.expectedType);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(rawOption, expectedType);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", TypedDriverOption.class.getSimpleName() + "[", "]")
        .add("rawOption=" + rawOption)
        .add("expectedType=" + expectedType)
        .toString();
  }

  /** The contact points to use for the initial connection to the cluster. */
  public static final TypedDriverOption<List<String>> CONTACT_POINTS =
      new TypedDriverOption<>(DefaultDriverOption.CONTACT_POINTS, GenericType.listOf(String.class));
  /** A name that uniquely identifies the driver instance. */
  public static final TypedDriverOption<String> SESSION_NAME =
      new TypedDriverOption<>(DefaultDriverOption.SESSION_NAME, GenericType.STRING);
  /** The name of the keyspace that the session should initially be connected to. */
  public static final TypedDriverOption<String> SESSION_KEYSPACE =
      new TypedDriverOption<>(DefaultDriverOption.SESSION_KEYSPACE, GenericType.STRING);
  /** How often the driver tries to reload the configuration. */
  public static final TypedDriverOption<Duration> CONFIG_RELOAD_INTERVAL =
      new TypedDriverOption<>(DefaultDriverOption.CONFIG_RELOAD_INTERVAL, GenericType.DURATION);
  /** How long the driver waits for a request to complete. */
  public static final TypedDriverOption<Duration> REQUEST_TIMEOUT =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_TIMEOUT, GenericType.DURATION);
  /** The consistency level. */
  public static final TypedDriverOption<String> REQUEST_CONSISTENCY =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_CONSISTENCY, GenericType.STRING);
  /** The page size. */
  public static final TypedDriverOption<Integer> REQUEST_PAGE_SIZE =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_PAGE_SIZE, GenericType.INTEGER);
  /** The serial consistency level. */
  public static final TypedDriverOption<String> REQUEST_SERIAL_CONSISTENCY =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY, GenericType.STRING);
  /** The default idempotence of a request. */
  public static final TypedDriverOption<Boolean> REQUEST_DEFAULT_IDEMPOTENCE =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, GenericType.BOOLEAN);
  /** The class of the load balancing policy. */
  public static final TypedDriverOption<String> LOAD_BALANCING_POLICY_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, GenericType.STRING);
  /** The datacenter that is considered "local". */
  public static final TypedDriverOption<String> LOAD_BALANCING_LOCAL_DATACENTER =
      new TypedDriverOption<>(
          DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, GenericType.STRING);
  /**
   * A custom filter to include/exclude nodes.
   *
   * @deprecated Use {@link #LOAD_BALANCING_DISTANCE_EVALUATOR_CLASS} instead.
   */
  @Deprecated
  public static final TypedDriverOption<String> LOAD_BALANCING_FILTER_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS, GenericType.STRING);
  /**
   * The class name of a custom {@link
   * com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator}.
   */
  public static final TypedDriverOption<String> LOAD_BALANCING_DISTANCE_EVALUATOR_CLASS =
      new TypedDriverOption<>(
          DefaultDriverOption.LOAD_BALANCING_DISTANCE_EVALUATOR_CLASS, GenericType.STRING);
  /** The timeout to use for internal queries that run as part of the initialization process. */
  public static final TypedDriverOption<Duration> CONNECTION_INIT_QUERY_TIMEOUT =
      new TypedDriverOption<>(
          DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, GenericType.DURATION);
  /** The timeout to use when the driver changes the keyspace on a connection at runtime. */
  public static final TypedDriverOption<Duration> CONNECTION_SET_KEYSPACE_TIMEOUT =
      new TypedDriverOption<>(
          DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT, GenericType.DURATION);
  /** The maximum number of requests that can be executed concurrently on a connection. */
  public static final TypedDriverOption<Integer> CONNECTION_MAX_REQUESTS =
      new TypedDriverOption<>(DefaultDriverOption.CONNECTION_MAX_REQUESTS, GenericType.INTEGER);
  /** The maximum number of "orphaned" requests before a connection gets closed automatically. */
  public static final TypedDriverOption<Integer> CONNECTION_MAX_ORPHAN_REQUESTS =
      new TypedDriverOption<>(
          DefaultDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS, GenericType.INTEGER);
  /** Whether to log non-fatal errors when the driver tries to open a new connection. */
  public static final TypedDriverOption<Boolean> CONNECTION_WARN_INIT_ERROR =
      new TypedDriverOption<>(DefaultDriverOption.CONNECTION_WARN_INIT_ERROR, GenericType.BOOLEAN);
  /** The number of connections in the LOCAL pool. */
  public static final TypedDriverOption<Integer> CONNECTION_POOL_LOCAL_SIZE =
      new TypedDriverOption<>(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, GenericType.INTEGER);
  /** The number of connections in the REMOTE pool. */
  public static final TypedDriverOption<Integer> CONNECTION_POOL_REMOTE_SIZE =
      new TypedDriverOption<>(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, GenericType.INTEGER);
  /**
   * Whether to schedule reconnection attempts if all contact points are unreachable on the first
   * initialization attempt.
   */
  public static final TypedDriverOption<Boolean> RECONNECT_ON_INIT =
      new TypedDriverOption<>(DefaultDriverOption.RECONNECT_ON_INIT, GenericType.BOOLEAN);
  /** The class of the reconnection policy. */
  public static final TypedDriverOption<String> RECONNECTION_POLICY_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.RECONNECTION_POLICY_CLASS, GenericType.STRING);
  /** Base delay for computing time between reconnection attempts. */
  public static final TypedDriverOption<Duration> RECONNECTION_BASE_DELAY =
      new TypedDriverOption<>(DefaultDriverOption.RECONNECTION_BASE_DELAY, GenericType.DURATION);
  /** Maximum delay between reconnection attempts. */
  public static final TypedDriverOption<Duration> RECONNECTION_MAX_DELAY =
      new TypedDriverOption<>(DefaultDriverOption.RECONNECTION_MAX_DELAY, GenericType.DURATION);
  /** The class of the retry policy. */
  public static final TypedDriverOption<String> RETRY_POLICY_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.RETRY_POLICY_CLASS, GenericType.STRING);
  /** The class of the speculative execution policy. */
  public static final TypedDriverOption<String> SPECULATIVE_EXECUTION_POLICY_CLASS =
      new TypedDriverOption<>(
          DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS, GenericType.STRING);
  /** The maximum number of executions. */
  public static final TypedDriverOption<Integer> SPECULATIVE_EXECUTION_MAX =
      new TypedDriverOption<>(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX, GenericType.INTEGER);
  /** The delay between each execution. */
  public static final TypedDriverOption<Duration> SPECULATIVE_EXECUTION_DELAY =
      new TypedDriverOption<>(
          DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY, GenericType.DURATION);
  /** The class of the authentication provider. */
  public static final TypedDriverOption<String> AUTH_PROVIDER_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.AUTH_PROVIDER_CLASS, GenericType.STRING);
  /** Plain text auth provider username. */
  public static final TypedDriverOption<String> AUTH_PROVIDER_USER_NAME =
      new TypedDriverOption<>(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, GenericType.STRING);
  /** Plain text auth provider password. */
  public static final TypedDriverOption<String> AUTH_PROVIDER_PASSWORD =
      new TypedDriverOption<>(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, GenericType.STRING);
  /** The class of the SSL Engine Factory. */
  public static final TypedDriverOption<String> SSL_ENGINE_FACTORY_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, GenericType.STRING);
  /** The cipher suites to enable when creating an SSLEngine for a connection. */
  public static final TypedDriverOption<List<String>> SSL_CIPHER_SUITES =
      new TypedDriverOption<>(
          DefaultDriverOption.SSL_CIPHER_SUITES, GenericType.listOf(String.class));
  /**
   * Whether or not to require validation that the hostname of the server certificate's common name
   * matches the hostname of the server being connected to.
   */
  public static final TypedDriverOption<Boolean> SSL_HOSTNAME_VALIDATION =
      new TypedDriverOption<>(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, GenericType.BOOLEAN);
  /** The location of the keystore file. */
  public static final TypedDriverOption<String> SSL_KEYSTORE_PATH =
      new TypedDriverOption<>(DefaultDriverOption.SSL_KEYSTORE_PATH, GenericType.STRING);
  /** The keystore password. */
  public static final TypedDriverOption<String> SSL_KEYSTORE_PASSWORD =
      new TypedDriverOption<>(DefaultDriverOption.SSL_KEYSTORE_PASSWORD, GenericType.STRING);

  /** The duration between attempts to reload the keystore. */
  public static final TypedDriverOption<Duration> SSL_KEYSTORE_RELOAD_INTERVAL =
      new TypedDriverOption<>(
          DefaultDriverOption.SSL_KEYSTORE_RELOAD_INTERVAL, GenericType.DURATION);

  /** The location of the truststore file. */
  public static final TypedDriverOption<String> SSL_TRUSTSTORE_PATH =
      new TypedDriverOption<>(DefaultDriverOption.SSL_TRUSTSTORE_PATH, GenericType.STRING);
  /** The truststore password. */
  public static final TypedDriverOption<String> SSL_TRUSTSTORE_PASSWORD =
      new TypedDriverOption<>(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, GenericType.STRING);
  /** The class of the generator that assigns a microsecond timestamp to each request. */
  public static final TypedDriverOption<String> TIMESTAMP_GENERATOR_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS, GenericType.STRING);
  /** Whether to force the driver to use Java's millisecond-precision system clock. */
  public static final TypedDriverOption<Boolean> TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK =
      new TypedDriverOption<>(
          DefaultDriverOption.TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK, GenericType.BOOLEAN);
  /** How far in the future timestamps are allowed to drift before the warning is logged. */
  public static final TypedDriverOption<Duration> TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD =
      new TypedDriverOption<>(
          DefaultDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD, GenericType.DURATION);
  /** How often the warning will be logged if timestamps keep drifting above the threshold. */
  public static final TypedDriverOption<Duration> TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL =
      new TypedDriverOption<>(
          DefaultDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL, GenericType.DURATION);

  /**
   * The class of a session-wide component that tracks the outcome of requests.
   *
   * @deprecated Use {@link #REQUEST_TRACKER_CLASSES} instead.
   */
  @Deprecated
  public static final TypedDriverOption<String> REQUEST_TRACKER_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_TRACKER_CLASS, GenericType.STRING);

  /** The classes of session-wide components that track the outcome of requests. */
  public static final TypedDriverOption<List<String>> REQUEST_TRACKER_CLASSES =
      new TypedDriverOption<>(
          DefaultDriverOption.REQUEST_TRACKER_CLASSES, GenericType.listOf(String.class));

  /** Whether to log successful requests. */
  public static final TypedDriverOption<Boolean> REQUEST_LOGGER_SUCCESS_ENABLED =
      new TypedDriverOption<>(
          DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED, GenericType.BOOLEAN);
  /** The threshold to classify a successful request as "slow". */
  public static final TypedDriverOption<Duration> REQUEST_LOGGER_SLOW_THRESHOLD =
      new TypedDriverOption<>(
          DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD, GenericType.DURATION);
  /** Whether to log slow requests. */
  public static final TypedDriverOption<Boolean> REQUEST_LOGGER_SLOW_ENABLED =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED, GenericType.BOOLEAN);
  /** Whether to log failed requests. */
  public static final TypedDriverOption<Boolean> REQUEST_LOGGER_ERROR_ENABLED =
      new TypedDriverOption<>(
          DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED, GenericType.BOOLEAN);
  /** The maximum length of the query string in the log message. */
  public static final TypedDriverOption<Integer> REQUEST_LOGGER_MAX_QUERY_LENGTH =
      new TypedDriverOption<>(
          DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH, GenericType.INTEGER);
  /** Whether to log bound values in addition to the query string. */
  public static final TypedDriverOption<Boolean> REQUEST_LOGGER_VALUES =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_LOGGER_VALUES, GenericType.BOOLEAN);
  /** The maximum length for bound values in the log message. */
  public static final TypedDriverOption<Integer> REQUEST_LOGGER_MAX_VALUE_LENGTH =
      new TypedDriverOption<>(
          DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH, GenericType.INTEGER);
  /** The maximum number of bound values to log. */
  public static final TypedDriverOption<Integer> REQUEST_LOGGER_MAX_VALUES =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES, GenericType.INTEGER);
  /** Whether to log stack traces for failed queries. */
  public static final TypedDriverOption<Boolean> REQUEST_LOGGER_STACK_TRACES =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES, GenericType.BOOLEAN);
  /**
   * The class of a session-wide component that controls the rate at which requests are executed.
   */
  public static final TypedDriverOption<String> REQUEST_THROTTLER_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_THROTTLER_CLASS, GenericType.STRING);
  /** The maximum number of requests that are allowed to execute in parallel. */
  public static final TypedDriverOption<Integer> REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS =
      new TypedDriverOption<>(
          DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS, GenericType.INTEGER);
  /** The maximum allowed request rate. */
  public static final TypedDriverOption<Integer> REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND =
      new TypedDriverOption<>(
          DefaultDriverOption.REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND, GenericType.INTEGER);
  /**
   * The maximum number of requests that can be enqueued when the throttling threshold is exceeded.
   */
  public static final TypedDriverOption<Integer> REQUEST_THROTTLER_MAX_QUEUE_SIZE =
      new TypedDriverOption<>(
          DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE, GenericType.INTEGER);
  /** How often the throttler attempts to dequeue requests. */
  public static final TypedDriverOption<Duration> REQUEST_THROTTLER_DRAIN_INTERVAL =
      new TypedDriverOption<>(
          DefaultDriverOption.REQUEST_THROTTLER_DRAIN_INTERVAL, GenericType.DURATION);

  /**
   * The class of a session-wide component that listens for node state changes.
   *
   * @deprecated Use {@link #METADATA_NODE_STATE_LISTENER_CLASSES} instead.
   */
  @Deprecated
  public static final TypedDriverOption<String> METADATA_NODE_STATE_LISTENER_CLASS =
      new TypedDriverOption<>(
          DefaultDriverOption.METADATA_NODE_STATE_LISTENER_CLASS, GenericType.STRING);

  /**
   * The class of a session-wide component that listens for schema changes.
   *
   * @deprecated Use {@link #METADATA_SCHEMA_CHANGE_LISTENER_CLASSES} instead.
   */
  @Deprecated
  public static final TypedDriverOption<String> METADATA_SCHEMA_CHANGE_LISTENER_CLASS =
      new TypedDriverOption<>(
          DefaultDriverOption.METADATA_SCHEMA_CHANGE_LISTENER_CLASS, GenericType.STRING);

  /** The classes of session-wide components that listen for node state changes. */
  public static final TypedDriverOption<List<String>> METADATA_NODE_STATE_LISTENER_CLASSES =
      new TypedDriverOption<>(
          DefaultDriverOption.METADATA_NODE_STATE_LISTENER_CLASSES,
          GenericType.listOf(String.class));

  /** The classes of session-wide components that listen for schema changes. */
  public static final TypedDriverOption<List<String>> METADATA_SCHEMA_CHANGE_LISTENER_CLASSES =
      new TypedDriverOption<>(
          DefaultDriverOption.METADATA_SCHEMA_CHANGE_LISTENER_CLASSES,
          GenericType.listOf(String.class));

  /**
   * The class of the address translator to use to convert the addresses sent by Cassandra nodes
   * into ones that the driver uses to connect.
   */
  public static final TypedDriverOption<String> ADDRESS_TRANSLATOR_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS, GenericType.STRING);
  /** The native protocol version to use. */
  public static final TypedDriverOption<String> PROTOCOL_VERSION =
      new TypedDriverOption<>(DefaultDriverOption.PROTOCOL_VERSION, GenericType.STRING);
  /** The name of the algorithm used to compress protocol frames. */
  public static final TypedDriverOption<String> PROTOCOL_COMPRESSION =
      new TypedDriverOption<>(DefaultDriverOption.PROTOCOL_COMPRESSION, GenericType.STRING);
  /** The maximum length, in bytes, of the frames supported by the driver. */
  public static final TypedDriverOption<Long> PROTOCOL_MAX_FRAME_LENGTH =
      new TypedDriverOption<>(DefaultDriverOption.PROTOCOL_MAX_FRAME_LENGTH, GenericType.LONG);
  /**
   * Whether a warning is logged when a request (such as a CQL `USE ...`) changes the active
   * keyspace.
   */
  public static final TypedDriverOption<Boolean> REQUEST_WARN_IF_SET_KEYSPACE =
      new TypedDriverOption<>(
          DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, GenericType.BOOLEAN);
  /** How many times the driver will attempt to fetch the query trace if it is not ready yet. */
  public static final TypedDriverOption<Integer> REQUEST_TRACE_ATTEMPTS =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_TRACE_ATTEMPTS, GenericType.INTEGER);
  /** The interval between each attempt. */
  public static final TypedDriverOption<Duration> REQUEST_TRACE_INTERVAL =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_TRACE_INTERVAL, GenericType.DURATION);
  /** The consistency level to use for trace queries. */
  public static final TypedDriverOption<String> REQUEST_TRACE_CONSISTENCY =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_TRACE_CONSISTENCY, GenericType.STRING);
  /** Whether or not to publish aggregable histogram for metrics */
  public static final TypedDriverOption<Boolean> METRICS_GENERATE_AGGREGABLE_HISTOGRAMS =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_GENERATE_AGGREGABLE_HISTOGRAMS, GenericType.BOOLEAN);
  /** List of enabled session-level metrics. */
  public static final TypedDriverOption<List<String>> METRICS_SESSION_ENABLED =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_SESSION_ENABLED, GenericType.listOf(String.class));
  /** List of enabled node-level metrics. */
  public static final TypedDriverOption<List<String>> METRICS_NODE_ENABLED =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_NODE_ENABLED, GenericType.listOf(String.class));
  /** The largest latency that we expect to record for requests. */
  public static final TypedDriverOption<Duration> METRICS_SESSION_CQL_REQUESTS_HIGHEST =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST, GenericType.DURATION);
  /** The shortest latency that we expect to record for requests. */
  public static final TypedDriverOption<Duration> METRICS_SESSION_CQL_REQUESTS_LOWEST =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_LOWEST, GenericType.DURATION);
  /** Optional service-level objectives to meet, as a list of latencies to track. */
  public static final TypedDriverOption<List<Duration>> METRICS_SESSION_CQL_REQUESTS_SLO =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_SLO,
          GenericType.listOf(GenericType.DURATION));
  /** Optional pre-defined percentile of cql requests to publish, as a list of percentiles . */
  public static final TypedDriverOption<List<Double>>
      METRICS_SESSION_CQL_REQUESTS_PUBLISH_PERCENTILES =
          new TypedDriverOption<>(
              DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_PUBLISH_PERCENTILES,
              GenericType.listOf(GenericType.DOUBLE));
  /**
   * The number of significant decimal digits to which internal structures will maintain for
   * requests.
   */
  public static final TypedDriverOption<Integer> METRICS_SESSION_CQL_REQUESTS_DIGITS =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS, GenericType.INTEGER);
  /** The interval at which percentile data is refreshed for requests. */
  public static final TypedDriverOption<Duration> METRICS_SESSION_CQL_REQUESTS_INTERVAL =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL, GenericType.DURATION);
  /** The largest latency that we expect to record for throttling. */
  public static final TypedDriverOption<Duration> METRICS_SESSION_THROTTLING_HIGHEST =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_SESSION_THROTTLING_HIGHEST, GenericType.DURATION);
  /** The shortest latency that we expect to record for throttling. */
  public static final TypedDriverOption<Duration> METRICS_SESSION_THROTTLING_LOWEST =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_SESSION_THROTTLING_LOWEST, GenericType.DURATION);
  /** Optional service-level objectives to meet, as a list of latencies to track. */
  public static final TypedDriverOption<List<Duration>> METRICS_SESSION_THROTTLING_SLO =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_SESSION_THROTTLING_SLO,
          GenericType.listOf(GenericType.DURATION));
  /** Optional pre-defined percentile of throttling delay to publish, as a list of percentiles . */
  public static final TypedDriverOption<List<Double>>
      METRICS_SESSION_THROTTLING_PUBLISH_PERCENTILES =
          new TypedDriverOption<>(
              DefaultDriverOption.METRICS_SESSION_THROTTLING_PUBLISH_PERCENTILES,
              GenericType.listOf(GenericType.DOUBLE));
  /**
   * The number of significant decimal digits to which internal structures will maintain for
   * throttling.
   */
  public static final TypedDriverOption<Integer> METRICS_SESSION_THROTTLING_DIGITS =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_SESSION_THROTTLING_DIGITS, GenericType.INTEGER);
  /** The interval at which percentile data is refreshed for throttling. */
  public static final TypedDriverOption<Duration> METRICS_SESSION_THROTTLING_INTERVAL =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_SESSION_THROTTLING_INTERVAL, GenericType.DURATION);
  /** The largest latency that we expect to record for requests. */
  public static final TypedDriverOption<Duration> METRICS_NODE_CQL_MESSAGES_HIGHEST =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST, GenericType.DURATION);
  /** The shortest latency that we expect to record for requests. */
  public static final TypedDriverOption<Duration> METRICS_NODE_CQL_MESSAGES_LOWEST =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_LOWEST, GenericType.DURATION);
  /** Optional service-level objectives to meet, as a list of latencies to track. */
  public static final TypedDriverOption<List<Duration>> METRICS_NODE_CQL_MESSAGES_SLO =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_SLO,
          GenericType.listOf(GenericType.DURATION));
  /** Optional pre-defined percentile of node cql messages to publish, as a list of percentiles . */
  public static final TypedDriverOption<List<Double>>
      METRICS_NODE_CQL_MESSAGES_PUBLISH_PERCENTILES =
          new TypedDriverOption<>(
              DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_PUBLISH_PERCENTILES,
              GenericType.listOf(GenericType.DOUBLE));
  /**
   * The number of significant decimal digits to which internal structures will maintain for
   * requests.
   */
  public static final TypedDriverOption<Integer> METRICS_NODE_CQL_MESSAGES_DIGITS =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS, GenericType.INTEGER);
  /** The interval at which percentile data is refreshed for requests. */
  public static final TypedDriverOption<Duration> METRICS_NODE_CQL_MESSAGES_INTERVAL =
      new TypedDriverOption<>(
          DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_INTERVAL, GenericType.DURATION);
  /** Whether or not to disable the Nagle algorithm. */
  public static final TypedDriverOption<Boolean> SOCKET_TCP_NODELAY =
      new TypedDriverOption<>(DefaultDriverOption.SOCKET_TCP_NODELAY, GenericType.BOOLEAN);
  /** Whether or not to enable TCP keep-alive probes. */
  public static final TypedDriverOption<Boolean> SOCKET_KEEP_ALIVE =
      new TypedDriverOption<>(DefaultDriverOption.SOCKET_KEEP_ALIVE, GenericType.BOOLEAN);
  /** Whether or not to allow address reuse. */
  public static final TypedDriverOption<Boolean> SOCKET_REUSE_ADDRESS =
      new TypedDriverOption<>(DefaultDriverOption.SOCKET_REUSE_ADDRESS, GenericType.BOOLEAN);
  /** Sets the linger interval. */
  public static final TypedDriverOption<Integer> SOCKET_LINGER_INTERVAL =
      new TypedDriverOption<>(DefaultDriverOption.SOCKET_LINGER_INTERVAL, GenericType.INTEGER);
  /** Sets a hint to the size of the underlying buffers for incoming network I/O. */
  public static final TypedDriverOption<Integer> SOCKET_RECEIVE_BUFFER_SIZE =
      new TypedDriverOption<>(DefaultDriverOption.SOCKET_RECEIVE_BUFFER_SIZE, GenericType.INTEGER);
  /** Sets a hint to the size of the underlying buffers for outgoing network I/O. */
  public static final TypedDriverOption<Integer> SOCKET_SEND_BUFFER_SIZE =
      new TypedDriverOption<>(DefaultDriverOption.SOCKET_SEND_BUFFER_SIZE, GenericType.INTEGER);
  /** The connection heartbeat interval. */
  public static final TypedDriverOption<Duration> HEARTBEAT_INTERVAL =
      new TypedDriverOption<>(DefaultDriverOption.HEARTBEAT_INTERVAL, GenericType.DURATION);
  /** How long the driver waits for the response to a heartbeat. */
  public static final TypedDriverOption<Duration> HEARTBEAT_TIMEOUT =
      new TypedDriverOption<>(DefaultDriverOption.HEARTBEAT_TIMEOUT, GenericType.DURATION);
  /** How long the driver waits to propagate a Topology event. */
  public static final TypedDriverOption<Duration> METADATA_TOPOLOGY_WINDOW =
      new TypedDriverOption<>(DefaultDriverOption.METADATA_TOPOLOGY_WINDOW, GenericType.DURATION);
  /** The maximum number of events that can accumulate. */
  public static final TypedDriverOption<Integer> METADATA_TOPOLOGY_MAX_EVENTS =
      new TypedDriverOption<>(
          DefaultDriverOption.METADATA_TOPOLOGY_MAX_EVENTS, GenericType.INTEGER);
  /** Whether schema metadata is enabled. */
  public static final TypedDriverOption<Boolean> METADATA_SCHEMA_ENABLED =
      new TypedDriverOption<>(DefaultDriverOption.METADATA_SCHEMA_ENABLED, GenericType.BOOLEAN);
  /** The timeout for the requests to the schema tables. */
  public static final TypedDriverOption<Duration> METADATA_SCHEMA_REQUEST_TIMEOUT =
      new TypedDriverOption<>(
          DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, GenericType.DURATION);
  /** The page size for the requests to the schema tables. */
  public static final TypedDriverOption<Integer> METADATA_SCHEMA_REQUEST_PAGE_SIZE =
      new TypedDriverOption<>(
          DefaultDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE, GenericType.INTEGER);
  /** The list of keyspaces for which schema and token metadata should be maintained. */
  public static final TypedDriverOption<List<String>> METADATA_SCHEMA_REFRESHED_KEYSPACES =
      new TypedDriverOption<>(
          DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES,
          GenericType.listOf(String.class));
  /** How long the driver waits to apply a refresh. */
  public static final TypedDriverOption<Duration> METADATA_SCHEMA_WINDOW =
      new TypedDriverOption<>(DefaultDriverOption.METADATA_SCHEMA_WINDOW, GenericType.DURATION);
  /** The maximum number of refreshes that can accumulate. */
  public static final TypedDriverOption<Integer> METADATA_SCHEMA_MAX_EVENTS =
      new TypedDriverOption<>(DefaultDriverOption.METADATA_SCHEMA_MAX_EVENTS, GenericType.INTEGER);
  /** Whether token metadata is enabled. */
  public static final TypedDriverOption<Boolean> METADATA_TOKEN_MAP_ENABLED =
      new TypedDriverOption<>(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, GenericType.BOOLEAN);
  /** How long the driver waits for responses to control queries. */
  public static final TypedDriverOption<Duration> CONTROL_CONNECTION_TIMEOUT =
      new TypedDriverOption<>(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, GenericType.DURATION);
  /** The interval between each schema agreement check attempt. */
  public static final TypedDriverOption<Duration> CONTROL_CONNECTION_AGREEMENT_INTERVAL =
      new TypedDriverOption<>(
          DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_INTERVAL, GenericType.DURATION);
  /** The timeout after which schema agreement fails. */
  public static final TypedDriverOption<Duration> CONTROL_CONNECTION_AGREEMENT_TIMEOUT =
      new TypedDriverOption<>(
          DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, GenericType.DURATION);
  /** Whether to log a warning if schema agreement fails. */
  public static final TypedDriverOption<Boolean> CONTROL_CONNECTION_AGREEMENT_WARN =
      new TypedDriverOption<>(
          DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_WARN, GenericType.BOOLEAN);
  /** Whether `Session.prepare` calls should be sent to all nodes in the cluster. */
  public static final TypedDriverOption<Boolean> PREPARE_ON_ALL_NODES =
      new TypedDriverOption<>(DefaultDriverOption.PREPARE_ON_ALL_NODES, GenericType.BOOLEAN);
  /** Whether the driver tries to prepare on new nodes at all. */
  public static final TypedDriverOption<Boolean> REPREPARE_ENABLED =
      new TypedDriverOption<>(DefaultDriverOption.REPREPARE_ENABLED, GenericType.BOOLEAN);
  /** Whether to check `system.prepared_statements` on the target node before repreparing. */
  public static final TypedDriverOption<Boolean> REPREPARE_CHECK_SYSTEM_TABLE =
      new TypedDriverOption<>(
          DefaultDriverOption.REPREPARE_CHECK_SYSTEM_TABLE, GenericType.BOOLEAN);
  /** The maximum number of statements that should be reprepared. */
  public static final TypedDriverOption<Integer> REPREPARE_MAX_STATEMENTS =
      new TypedDriverOption<>(DefaultDriverOption.REPREPARE_MAX_STATEMENTS, GenericType.INTEGER);
  /** The maximum number of concurrent requests when repreparing. */
  public static final TypedDriverOption<Integer> REPREPARE_MAX_PARALLELISM =
      new TypedDriverOption<>(DefaultDriverOption.REPREPARE_MAX_PARALLELISM, GenericType.INTEGER);
  /** The request timeout when repreparing. */
  public static final TypedDriverOption<Duration> REPREPARE_TIMEOUT =
      new TypedDriverOption<>(DefaultDriverOption.REPREPARE_TIMEOUT, GenericType.DURATION);
  /** Whether the prepared statements cache use weak values. */
  public static final TypedDriverOption<Boolean> PREPARED_CACHE_WEAK_VALUES =
      new TypedDriverOption<>(DefaultDriverOption.PREPARED_CACHE_WEAK_VALUES, GenericType.BOOLEAN);
  /** The number of threads in the I/O group. */
  public static final TypedDriverOption<Integer> NETTY_IO_SIZE =
      new TypedDriverOption<>(DefaultDriverOption.NETTY_IO_SIZE, GenericType.INTEGER);
  /** Quiet period for I/O group shutdown. */
  public static final TypedDriverOption<Integer> NETTY_IO_SHUTDOWN_QUIET_PERIOD =
      new TypedDriverOption<>(
          DefaultDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD, GenericType.INTEGER);
  /** Max time to wait for I/O group shutdown. */
  public static final TypedDriverOption<Integer> NETTY_IO_SHUTDOWN_TIMEOUT =
      new TypedDriverOption<>(DefaultDriverOption.NETTY_IO_SHUTDOWN_TIMEOUT, GenericType.INTEGER);
  /** Units for I/O group quiet period and timeout. */
  public static final TypedDriverOption<String> NETTY_IO_SHUTDOWN_UNIT =
      new TypedDriverOption<>(DefaultDriverOption.NETTY_IO_SHUTDOWN_UNIT, GenericType.STRING);
  /** The number of threads in the Admin group. */
  public static final TypedDriverOption<Integer> NETTY_ADMIN_SIZE =
      new TypedDriverOption<>(DefaultDriverOption.NETTY_ADMIN_SIZE, GenericType.INTEGER);
  /** Quiet period for admin group shutdown. */
  public static final TypedDriverOption<Integer> NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD =
      new TypedDriverOption<>(
          DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, GenericType.INTEGER);
  /** Max time to wait for admin group shutdown. */
  public static final TypedDriverOption<Integer> NETTY_ADMIN_SHUTDOWN_TIMEOUT =
      new TypedDriverOption<>(
          DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_TIMEOUT, GenericType.INTEGER);
  /** Units for admin group quiet period and timeout. */
  public static final TypedDriverOption<String> NETTY_ADMIN_SHUTDOWN_UNIT =
      new TypedDriverOption<>(DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_UNIT, GenericType.STRING);
  /** @deprecated This option was removed in version 4.6.1. */
  @Deprecated
  public static final TypedDriverOption<Integer> COALESCER_MAX_RUNS =
      new TypedDriverOption<>(DefaultDriverOption.COALESCER_MAX_RUNS, GenericType.INTEGER);
  /** The coalescer reschedule interval. */
  public static final TypedDriverOption<Duration> COALESCER_INTERVAL =
      new TypedDriverOption<>(DefaultDriverOption.COALESCER_INTERVAL, GenericType.DURATION);
  /** Whether to resolve the addresses passed to `basic.contact-points`. */
  public static final TypedDriverOption<Boolean> RESOLVE_CONTACT_POINTS =
      new TypedDriverOption<>(DefaultDriverOption.RESOLVE_CONTACT_POINTS, GenericType.BOOLEAN);
  /**
   * This is how frequent the timer should wake up to check for timed-out tasks or speculative
   * executions.
   */
  public static final TypedDriverOption<Duration> NETTY_TIMER_TICK_DURATION =
      new TypedDriverOption<>(DefaultDriverOption.NETTY_TIMER_TICK_DURATION, GenericType.DURATION);
  /** Number of ticks in the Timer wheel. */
  public static final TypedDriverOption<Integer> NETTY_TIMER_TICKS_PER_WHEEL =
      new TypedDriverOption<>(DefaultDriverOption.NETTY_TIMER_TICKS_PER_WHEEL, GenericType.INTEGER);
  /**
   * Whether logging of server warnings generated during query execution should be disabled by the
   * driver.
   */
  public static final TypedDriverOption<Boolean> REQUEST_LOG_WARNINGS =
      new TypedDriverOption<>(DefaultDriverOption.REQUEST_LOG_WARNINGS, GenericType.BOOLEAN);
  /** Whether the threads created by the driver should be daemon threads. */
  public static final TypedDriverOption<Boolean> NETTY_DAEMON =
      new TypedDriverOption<>(DefaultDriverOption.NETTY_DAEMON, GenericType.BOOLEAN);
  /**
   * The location of the cloud secure bundle used to connect to DataStax Apache Cassandra as a
   * service.
   */
  public static final TypedDriverOption<String> CLOUD_SECURE_CONNECT_BUNDLE =
      new TypedDriverOption<>(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, GenericType.STRING);
  /** Whether the slow replica avoidance should be enabled in the default LBP. */
  public static final TypedDriverOption<Boolean> LOAD_BALANCING_POLICY_SLOW_AVOIDANCE =
      new TypedDriverOption<>(
          DefaultDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, GenericType.BOOLEAN);
  /** The timeout to use when establishing driver connections. */
  public static final TypedDriverOption<Duration> CONNECTION_CONNECT_TIMEOUT =
      new TypedDriverOption<>(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, GenericType.DURATION);
  /** The maximum number of live sessions that are allowed to coexist in a given VM. */
  public static final TypedDriverOption<Integer> SESSION_LEAK_THRESHOLD =
      new TypedDriverOption<>(DefaultDriverOption.SESSION_LEAK_THRESHOLD, GenericType.INTEGER);

  /** The name of the application using the session. */
  public static final TypedDriverOption<String> APPLICATION_NAME =
      new TypedDriverOption<>(DseDriverOption.APPLICATION_NAME, GenericType.STRING);
  /** The version of the application using the session. */
  public static final TypedDriverOption<String> APPLICATION_VERSION =
      new TypedDriverOption<>(DseDriverOption.APPLICATION_VERSION, GenericType.STRING);
  /** Proxy authentication for GSSAPI authentication: allows to login as another user or role. */
  public static final TypedDriverOption<String> AUTH_PROVIDER_AUTHORIZATION_ID =
      new TypedDriverOption<>(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID, GenericType.STRING);
  /** Service name for GSSAPI authentication. */
  public static final TypedDriverOption<String> AUTH_PROVIDER_SERVICE =
      new TypedDriverOption<>(DseDriverOption.AUTH_PROVIDER_SERVICE, GenericType.STRING);
  /** Login configuration for GSSAPI authentication. */
  public static final TypedDriverOption<String> AUTH_PROVIDER_LOGIN_CONFIGURATION =
      new TypedDriverOption<>(
          DseDriverOption.AUTH_PROVIDER_LOGIN_CONFIGURATION, GenericType.STRING);
  /** Internal SASL properties, if any, such as QOP, for GSSAPI authentication. */
  public static final TypedDriverOption<Map<String, String>> AUTH_PROVIDER_SASL_PROPERTIES =
      new TypedDriverOption<>(
          DseDriverOption.AUTH_PROVIDER_SASL_PROPERTIES,
          GenericType.mapOf(GenericType.STRING, GenericType.STRING));
  /** The page size for continuous paging. */
  public static final TypedDriverOption<Integer> CONTINUOUS_PAGING_PAGE_SIZE =
      new TypedDriverOption<>(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, GenericType.INTEGER);
  /**
   * Whether {@link #CONTINUOUS_PAGING_PAGE_SIZE} should be interpreted in number of rows or bytes.
   */
  public static final TypedDriverOption<Boolean> CONTINUOUS_PAGING_PAGE_SIZE_BYTES =
      new TypedDriverOption<>(
          DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE_BYTES, GenericType.BOOLEAN);
  /** The maximum number of continuous pages to return. */
  public static final TypedDriverOption<Integer> CONTINUOUS_PAGING_MAX_PAGES =
      new TypedDriverOption<>(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES, GenericType.INTEGER);
  /** The maximum number of continuous pages per second. */
  public static final TypedDriverOption<Integer> CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND =
      new TypedDriverOption<>(
          DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, GenericType.INTEGER);
  /** The maximum number of continuous pages that can be stored in the local queue. */
  public static final TypedDriverOption<Integer> CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES =
      new TypedDriverOption<>(
          DseDriverOption.CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES, GenericType.INTEGER);
  /** How long to wait for the coordinator to send the first continuous page. */
  public static final TypedDriverOption<Duration> CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE =
      new TypedDriverOption<>(
          DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE, GenericType.DURATION);
  /** How long to wait for the coordinator to send subsequent continuous pages. */
  public static final TypedDriverOption<Duration> CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES =
      new TypedDriverOption<>(
          DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES, GenericType.DURATION);
  /** The largest latency that we expect to record for continuous requests. */
  public static final TypedDriverOption<Duration>
      CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_HIGHEST =
          new TypedDriverOption<>(
              DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_HIGHEST,
              GenericType.DURATION);
  /** The shortest latency that we expect to record for continuous requests. */
  public static final TypedDriverOption<Duration>
      CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_LOWEST =
          new TypedDriverOption<>(
              DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_LOWEST,
              GenericType.DURATION);
  /** Optional service-level objectives to meet, as a list of latencies to track. */
  public static final TypedDriverOption<List<Duration>>
      CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_SLO =
          new TypedDriverOption<>(
              DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_SLO,
              GenericType.listOf(GenericType.DURATION));
  /**
   * Optional pre-defined percentile of continuous paging cql requests to publish, as a list of
   * percentiles .
   */
  public static final TypedDriverOption<List<Double>>
      CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_PUBLISH_PERCENTILES =
          new TypedDriverOption<>(
              DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_PUBLISH_PERCENTILES,
              GenericType.listOf(GenericType.DOUBLE));
  /**
   * The number of significant decimal digits to which internal structures will maintain for
   * continuous requests.
   */
  public static final TypedDriverOption<Integer>
      CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_DIGITS =
          new TypedDriverOption<>(
              DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_DIGITS,
              GenericType.INTEGER);
  /** The interval at which percentile data is refreshed for continuous requests. */
  public static final TypedDriverOption<Duration>
      CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_INTERVAL =
          new TypedDriverOption<>(
              DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_INTERVAL,
              GenericType.DURATION);
  /** The read consistency level to use for graph statements. */
  public static final TypedDriverOption<String> GRAPH_READ_CONSISTENCY_LEVEL =
      new TypedDriverOption<>(DseDriverOption.GRAPH_READ_CONSISTENCY_LEVEL, GenericType.STRING);
  /** The write consistency level to use for graph statements. */
  public static final TypedDriverOption<String> GRAPH_WRITE_CONSISTENCY_LEVEL =
      new TypedDriverOption<>(DseDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL, GenericType.STRING);
  /** The traversal source to use for graph statements. */
  public static final TypedDriverOption<String> GRAPH_TRAVERSAL_SOURCE =
      new TypedDriverOption<>(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, GenericType.STRING);
  /**
   * The sub-protocol the driver will use to communicate with DSE Graph, on top of the Cassandra
   * native protocol.
   */
  public static final TypedDriverOption<String> GRAPH_SUB_PROTOCOL =
      new TypedDriverOption<>(DseDriverOption.GRAPH_SUB_PROTOCOL, GenericType.STRING);
  /** Whether a script statement represents a system query. */
  public static final TypedDriverOption<Boolean> GRAPH_IS_SYSTEM_QUERY =
      new TypedDriverOption<>(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, GenericType.BOOLEAN);
  /** The name of the graph targeted by graph statements. */
  public static final TypedDriverOption<String> GRAPH_NAME =
      new TypedDriverOption<>(DseDriverOption.GRAPH_NAME, GenericType.STRING);
  /** How long the driver waits for a graph request to complete. */
  public static final TypedDriverOption<Duration> GRAPH_TIMEOUT =
      new TypedDriverOption<>(DseDriverOption.GRAPH_TIMEOUT, GenericType.DURATION);
  /** Whether to send events for Insights monitoring. */
  public static final TypedDriverOption<Boolean> MONITOR_REPORTING_ENABLED =
      new TypedDriverOption<>(DseDriverOption.MONITOR_REPORTING_ENABLED, GenericType.BOOLEAN);
  /** Whether to enable paging for Graph queries. */
  public static final TypedDriverOption<String> GRAPH_PAGING_ENABLED =
      new TypedDriverOption<>(DseDriverOption.GRAPH_PAGING_ENABLED, GenericType.STRING);
  /** The page size for Graph continuous paging. */
  public static final TypedDriverOption<Integer> GRAPH_CONTINUOUS_PAGING_PAGE_SIZE =
      new TypedDriverOption<>(
          DseDriverOption.GRAPH_CONTINUOUS_PAGING_PAGE_SIZE, GenericType.INTEGER);
  /** The maximum number of Graph continuous pages to return. */
  public static final TypedDriverOption<Integer> GRAPH_CONTINUOUS_PAGING_MAX_PAGES =
      new TypedDriverOption<>(
          DseDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_PAGES, GenericType.INTEGER);
  /** The maximum number of Graph continuous pages per second. */
  public static final TypedDriverOption<Integer> GRAPH_CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND =
      new TypedDriverOption<>(
          DseDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, GenericType.INTEGER);
  /** The maximum number of Graph continuous pages that can be stored in the local queue. */
  public static final TypedDriverOption<Integer> GRAPH_CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES =
      new TypedDriverOption<>(
          DseDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES, GenericType.INTEGER);
  /** The largest latency that we expect to record for graph requests. */
  public static final TypedDriverOption<Duration> METRICS_SESSION_GRAPH_REQUESTS_HIGHEST =
      new TypedDriverOption<>(
          DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_HIGHEST, GenericType.DURATION);
  /** The shortest latency that we expect to record for graph requests. */
  public static final TypedDriverOption<Duration> METRICS_SESSION_GRAPH_REQUESTS_LOWEST =
      new TypedDriverOption<>(
          DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_LOWEST, GenericType.DURATION);
  /** Optional service-level objectives to meet, as a list of latencies to track. */
  public static final TypedDriverOption<List<Duration>> METRICS_SESSION_GRAPH_REQUESTS_SLO =
      new TypedDriverOption<>(
          DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_SLO,
          GenericType.listOf(GenericType.DURATION));
  /** Optional pre-defined percentile of graph requests to publish, as a list of percentiles . */
  public static final TypedDriverOption<List<Double>>
      METRICS_SESSION_GRAPH_REQUESTS_PUBLISH_PERCENTILES =
          new TypedDriverOption<>(
              DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_PUBLISH_PERCENTILES,
              GenericType.listOf(GenericType.DOUBLE));
  /**
   * The number of significant decimal digits to which internal structures will maintain for graph
   * requests.
   */
  public static final TypedDriverOption<Integer> METRICS_SESSION_GRAPH_REQUESTS_DIGITS =
      new TypedDriverOption<>(
          DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_DIGITS, GenericType.INTEGER);
  /** The interval at which percentile data is refreshed for graph requests. */
  public static final TypedDriverOption<Duration> METRICS_SESSION_GRAPH_REQUESTS_INTERVAL =
      new TypedDriverOption<>(
          DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_INTERVAL, GenericType.DURATION);
  /** The largest latency that we expect to record for graph requests. */
  public static final TypedDriverOption<Duration> METRICS_NODE_GRAPH_MESSAGES_HIGHEST =
      new TypedDriverOption<>(
          DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_HIGHEST, GenericType.DURATION);
  /** The shortest latency that we expect to record for graph requests. */
  public static final TypedDriverOption<Duration> METRICS_NODE_GRAPH_MESSAGES_LOWEST =
      new TypedDriverOption<>(
          DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_LOWEST, GenericType.DURATION);
  /** Optional service-level objectives to meet, as a list of latencies to track. */
  public static final TypedDriverOption<List<Duration>> METRICS_NODE_GRAPH_MESSAGES_SLO =
      new TypedDriverOption<>(
          DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_SLO,
          GenericType.listOf(GenericType.DURATION));
  /**
   * Optional pre-defined percentile of node graph requests to publish, as a list of percentiles .
   */
  public static final TypedDriverOption<List<Double>>
      METRICS_NODE_GRAPH_MESSAGES_PUBLISH_PERCENTILES =
          new TypedDriverOption<>(
              DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_PUBLISH_PERCENTILES,
              GenericType.listOf(GenericType.DOUBLE));
  /**
   * The number of significant decimal digits to which internal structures will maintain for graph
   * requests.
   */
  public static final TypedDriverOption<Integer> METRICS_NODE_GRAPH_MESSAGES_DIGITS =
      new TypedDriverOption<>(
          DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_DIGITS, GenericType.INTEGER);
  /** The interval at which percentile data is refreshed for graph requests. */
  public static final TypedDriverOption<Duration> METRICS_NODE_GRAPH_MESSAGES_INTERVAL =
      new TypedDriverOption<>(
          DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_INTERVAL, GenericType.DURATION);

  /** The time after which the node level metrics will be evicted. */
  public static final TypedDriverOption<Duration> METRICS_NODE_EXPIRE_AFTER =
      new TypedDriverOption<>(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER, GenericType.DURATION);

  /** The classname of the desired MetricsFactory implementation. */
  public static final TypedDriverOption<String> METRICS_FACTORY_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.METRICS_FACTORY_CLASS, GenericType.STRING);

  /** The classname of the desired {@code MetricIdGenerator} implementation. */
  public static final TypedDriverOption<String> METRICS_ID_GENERATOR_CLASS =
      new TypedDriverOption<>(DefaultDriverOption.METRICS_ID_GENERATOR_CLASS, GenericType.STRING);

  /** The value of the prefix to prepend to all metric names. */
  public static final TypedDriverOption<String> METRICS_ID_GENERATOR_PREFIX =
      new TypedDriverOption<>(DefaultDriverOption.METRICS_ID_GENERATOR_PREFIX, GenericType.STRING);

  /** The maximum number of nodes from remote DCs to include in query plans. */
  public static final TypedDriverOption<Integer>
      LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC =
          new TypedDriverOption<>(
              DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC,
              GenericType.INTEGER);
  /** Whether to consider nodes from remote DCs if the request's consistency level is local. */
  public static final TypedDriverOption<Boolean>
      LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS =
          new TypedDriverOption<>(
              DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS,
              GenericType.BOOLEAN);

  private static Iterable<TypedDriverOption<?>> introspectBuiltInValues() {
    try {
      ImmutableList.Builder<TypedDriverOption<?>> result = ImmutableList.builder();
      for (Field field : TypedDriverOption.class.getFields()) {
        if ((field.getModifiers() & PUBLIC_STATIC_FINAL) == PUBLIC_STATIC_FINAL
            && field.getType() == TypedDriverOption.class) {
          TypedDriverOption<?> typedOption = (TypedDriverOption<?>) field.get(null);
          result.add(typedOption);
        }
      }
      return result.build();
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Unexpected error while introspecting built-in values", e);
    }
  }

  private static final int PUBLIC_STATIC_FINAL = Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL;
}
