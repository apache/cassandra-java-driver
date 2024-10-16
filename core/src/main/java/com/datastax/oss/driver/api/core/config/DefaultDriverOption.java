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

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Built-in driver options for the core driver.
 *
 * <p>Refer to {@code reference.conf} in the driver codebase for a full description of each option.
 */
public enum DefaultDriverOption implements DriverOption {
  /**
   * The contact points to use for the initial connection to the cluster.
   *
   * <p>Value type: {@link java.util.List List}&#60;{@link String}&#62;
   */
  CONTACT_POINTS("basic.contact-points"),
  /**
   * A name that uniquely identifies the driver instance.
   *
   * <p>Value-type: {@link String}
   */
  SESSION_NAME("basic.session-name"),
  /**
   * The name of the keyspace that the session should initially be connected to.
   *
   * <p>Value-type: {@link String}
   */
  SESSION_KEYSPACE("basic.session-keyspace"),
  /**
   * How often the driver tries to reload the configuration.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONFIG_RELOAD_INTERVAL("basic.config-reload-interval"),

  /**
   * How long the driver waits for a request to complete.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  REQUEST_TIMEOUT("basic.request.timeout"),
  /**
   * The consistency level.
   *
   * <p>Value-Type: {@link String}
   */
  REQUEST_CONSISTENCY("basic.request.consistency"),
  /**
   * The page size.
   *
   * <p>Value-Type: int
   */
  REQUEST_PAGE_SIZE("basic.request.page-size"),
  /**
   * The serial consistency level.
   *
   * <p>Value-type: {@link String}
   */
  REQUEST_SERIAL_CONSISTENCY("basic.request.serial-consistency"),
  /**
   * The default idempotence of a request.
   *
   * <p>Value-type: boolean
   */
  REQUEST_DEFAULT_IDEMPOTENCE("basic.request.default-idempotence"),

  // LOAD_BALANCING_POLICY is a collection of sub-properties
  LOAD_BALANCING_POLICY("basic.load-balancing-policy"),
  /**
   * The class of the load balancing policy.
   *
   * <p>Value-type: {@link String}
   */
  LOAD_BALANCING_POLICY_CLASS("basic.load-balancing-policy.class"),
  /**
   * The datacenter that is considered "local".
   *
   * <p>Value-type: {@link String}
   */
  LOAD_BALANCING_LOCAL_DATACENTER("basic.load-balancing-policy.local-datacenter"),
  /**
   * A custom filter to include/exclude nodes.
   *
   * <p>Value-Type: {@link String}
   *
   * @deprecated use {@link #LOAD_BALANCING_DISTANCE_EVALUATOR_CLASS} instead.
   */
  @Deprecated
  LOAD_BALANCING_FILTER_CLASS("basic.load-balancing-policy.filter.class"),

  /**
   * The timeout to use for internal queries that run as part of the initialization process
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONNECTION_INIT_QUERY_TIMEOUT("advanced.connection.init-query-timeout"),
  /**
   * The timeout to use when the driver changes the keyspace on a connection at runtime.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONNECTION_SET_KEYSPACE_TIMEOUT("advanced.connection.set-keyspace-timeout"),
  /**
   * The maximum number of requests that can be executed concurrently on a connection
   *
   * <p>Value-type: int
   */
  CONNECTION_MAX_REQUESTS("advanced.connection.max-requests-per-connection"),
  /**
   * The maximum number of "orphaned" requests before a connection gets closed automatically.
   *
   * <p>Value-type: int
   */
  CONNECTION_MAX_ORPHAN_REQUESTS("advanced.connection.max-orphan-requests"),
  /**
   * Whether to log non-fatal errors when the driver tries to open a new connection.
   *
   * <p>Value-type: boolean
   */
  CONNECTION_WARN_INIT_ERROR("advanced.connection.warn-on-init-error"),
  /**
   * The number of connections in the LOCAL pool.
   *
   * <p>Value-type: int
   */
  CONNECTION_POOL_LOCAL_SIZE("advanced.connection.pool.local.size"),
  /**
   * The number of connections in the REMOTE pool.
   *
   * <p>Value-type: int
   */
  CONNECTION_POOL_REMOTE_SIZE("advanced.connection.pool.remote.size"),

  /**
   * Whether to schedule reconnection attempts if all contact points are unreachable on the first
   * initialization attempt.
   *
   * <p>Value-type: boolean
   */
  RECONNECT_ON_INIT("advanced.reconnect-on-init"),

  /**
   * The class of the reconnection policy.
   *
   * <p>Value-type: {@link String}
   */
  RECONNECTION_POLICY_CLASS("advanced.reconnection-policy.class"),
  /**
   * Base delay for computing time between reconnection attempts.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  RECONNECTION_BASE_DELAY("advanced.reconnection-policy.base-delay"),
  /**
   * Maximum delay between reconnection attempts.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  RECONNECTION_MAX_DELAY("advanced.reconnection-policy.max-delay"),

  // RETRY_POLICY is a collection of sub-properties
  RETRY_POLICY("advanced.retry-policy"),
  /**
   * The class of the retry policy.
   *
   * <p>Value-type: {@link String}
   */
  RETRY_POLICY_CLASS("advanced.retry-policy.class"),

  // SPECULATIVE_EXECUTION_POLICY is a collection of sub-properties
  SPECULATIVE_EXECUTION_POLICY("advanced.speculative-execution-policy"),
  /**
   * The class of the speculative execution policy.
   *
   * <p>Value-type: {@link String}
   */
  SPECULATIVE_EXECUTION_POLICY_CLASS("advanced.speculative-execution-policy.class"),
  /**
   * The maximum number of executions.
   *
   * <p>Value-type: int
   */
  SPECULATIVE_EXECUTION_MAX("advanced.speculative-execution-policy.max-executions"),
  /**
   * The delay between each execution.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  SPECULATIVE_EXECUTION_DELAY("advanced.speculative-execution-policy.delay"),

  /**
   * The class of the authentication provider.
   *
   * <p>Value-type: {@link String}
   */
  AUTH_PROVIDER_CLASS("advanced.auth-provider.class"),
  /**
   * Plain text auth provider username.
   *
   * <p>Value-type: {@link String}
   */
  AUTH_PROVIDER_USER_NAME("advanced.auth-provider.username"),
  /**
   * Plain text auth provider password.
   *
   * <p>Value-type: {@link String}
   */
  AUTH_PROVIDER_PASSWORD("advanced.auth-provider.password"),

  /**
   * The class of the SSL Engine Factory.
   *
   * <p>Value-type: {@link String}
   */
  SSL_ENGINE_FACTORY_CLASS("advanced.ssl-engine-factory.class"),
  /**
   * The cipher suites to enable when creating an SSLEngine for a connection.
   *
   * <p>Value type: {@link java.util.List List}&#60;{@link String}&#62;
   */
  SSL_CIPHER_SUITES("advanced.ssl-engine-factory.cipher-suites"),
  /**
   * Whether or not to require validation that the hostname of the server certificate's common name
   * matches the hostname of the server being connected to.
   *
   * <p>Value-type: boolean
   */
  SSL_HOSTNAME_VALIDATION("advanced.ssl-engine-factory.hostname-validation"),
  /**
   * The location of the keystore file.
   *
   * <p>Value-type: {@link String}
   */
  SSL_KEYSTORE_PATH("advanced.ssl-engine-factory.keystore-path"),
  /**
   * The keystore password.
   *
   * <p>Value-type: {@link String}
   */
  SSL_KEYSTORE_PASSWORD("advanced.ssl-engine-factory.keystore-password"),
  /**
   * The location of the truststore file.
   *
   * <p>Value-type: {@link String}
   */
  SSL_TRUSTSTORE_PATH("advanced.ssl-engine-factory.truststore-path"),
  /**
   * The truststore password.
   *
   * <p>Value-type: {@link String}
   */
  SSL_TRUSTSTORE_PASSWORD("advanced.ssl-engine-factory.truststore-password"),

  /**
   * The class of the generator that assigns a microsecond timestamp to each request.
   *
   * <p>Value-type: {@link String}
   */
  TIMESTAMP_GENERATOR_CLASS("advanced.timestamp-generator.class"),
  /**
   * Whether to force the driver to use Java's millisecond-precision system clock.
   *
   * <p>Value-type: boolean
   */
  TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK("advanced.timestamp-generator.force-java-clock"),
  /**
   * How far in the future timestamps are allowed to drift before the warning is logged.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD(
      "advanced.timestamp-generator.drift-warning.threshold"),
  /**
   * How often the warning will be logged if timestamps keep drifting above the threshold.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL("advanced.timestamp-generator.drift-warning.interval"),

  /**
   * The class of a session-wide component that tracks the outcome of requests.
   *
   * <p>Value-type: {@link String}
   *
   * @deprecated Use {@link #REQUEST_TRACKER_CLASSES} instead.
   */
  @Deprecated
  REQUEST_TRACKER_CLASS("advanced.request-tracker.class"),
  /**
   * Whether to log successful requests.
   *
   * <p>Value-type: boolean
   */
  REQUEST_LOGGER_SUCCESS_ENABLED("advanced.request-tracker.logs.success.enabled"),
  /**
   * The threshold to classify a successful request as "slow".
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  REQUEST_LOGGER_SLOW_THRESHOLD("advanced.request-tracker.logs.slow.threshold"),
  /**
   * Whether to log slow requests.
   *
   * <p>Value-type: boolean
   */
  REQUEST_LOGGER_SLOW_ENABLED("advanced.request-tracker.logs.slow.enabled"),
  /**
   * Whether to log failed requests.
   *
   * <p>Value-type: boolean
   */
  REQUEST_LOGGER_ERROR_ENABLED("advanced.request-tracker.logs.error.enabled"),
  /**
   * The maximum length of the query string in the log message.
   *
   * <p>Value-type: int
   */
  REQUEST_LOGGER_MAX_QUERY_LENGTH("advanced.request-tracker.logs.max-query-length"),
  /**
   * Whether to log bound values in addition to the query string.
   *
   * <p>Value-type: boolean
   */
  REQUEST_LOGGER_VALUES("advanced.request-tracker.logs.show-values"),
  /**
   * The maximum length for bound values in the log message.
   *
   * <p>Value-type: int
   */
  REQUEST_LOGGER_MAX_VALUE_LENGTH("advanced.request-tracker.logs.max-value-length"),
  /**
   * The maximum number of bound values to log.
   *
   * <p>Value-type: int
   */
  REQUEST_LOGGER_MAX_VALUES("advanced.request-tracker.logs.max-values"),
  /**
   * Whether to log stack traces for failed queries.
   *
   * <p>Value-type: boolean
   */
  REQUEST_LOGGER_STACK_TRACES("advanced.request-tracker.logs.show-stack-traces"),

  /**
   * The class of a session-wide component that controls the rate at which requests are executed.
   *
   * <p>Value-type: {@link String}
   */
  REQUEST_THROTTLER_CLASS("advanced.throttler.class"),
  /**
   * The maximum number of requests that are allowed to execute in parallel.
   *
   * <p>Value-type: int
   */
  REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS("advanced.throttler.max-concurrent-requests"),
  /**
   * The maximum allowed request rate.
   *
   * <p>Value-type: int
   */
  REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND("advanced.throttler.max-requests-per-second"),
  /**
   * The maximum number of requests that can be enqueued when the throttling threshold is exceeded.
   *
   * <p>Value-type: int
   */
  REQUEST_THROTTLER_MAX_QUEUE_SIZE("advanced.throttler.max-queue-size"),
  /**
   * How often the throttler attempts to dequeue requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  REQUEST_THROTTLER_DRAIN_INTERVAL("advanced.throttler.drain-interval"),

  /**
   * The class of a session-wide component that listens for node state changes.
   *
   * <p>Value-type: {@link String}
   *
   * @deprecated Use {@link #METADATA_NODE_STATE_LISTENER_CLASSES} instead.
   */
  @Deprecated
  METADATA_NODE_STATE_LISTENER_CLASS("advanced.node-state-listener.class"),

  /**
   * The class of a session-wide component that listens for schema changes.
   *
   * <p>Value-type: {@link String}
   *
   * @deprecated Use {@link #METADATA_SCHEMA_CHANGE_LISTENER_CLASSES} instead.
   */
  @Deprecated
  METADATA_SCHEMA_CHANGE_LISTENER_CLASS("advanced.schema-change-listener.class"),

  /**
   * The class of the address translator to use to convert the addresses sent by Cassandra nodes
   * into ones that the driver uses to connect.
   *
   * <p>Value-type: {@link String}
   */
  ADDRESS_TRANSLATOR_CLASS("advanced.address-translator.class"),

  /**
   * The native protocol version to use.
   *
   * <p>Value-type: {@link String}
   */
  PROTOCOL_VERSION("advanced.protocol.version"),
  /**
   * The name of the algorithm used to compress protocol frames.
   *
   * <p>Value-type: {@link String}
   */
  PROTOCOL_COMPRESSION("advanced.protocol.compression"),
  /**
   * The maximum length, in bytes, of the frames supported by the driver.
   *
   * <p>Value-type: long
   */
  PROTOCOL_MAX_FRAME_LENGTH("advanced.protocol.max-frame-length"),

  /**
   * Whether a warning is logged when a request (such as a CQL `USE ...`) changes the active
   * keyspace.
   *
   * <p>Value-type: boolean
   */
  REQUEST_WARN_IF_SET_KEYSPACE("advanced.request.warn-if-set-keyspace"),
  /**
   * How many times the driver will attempt to fetch the query trace if it is not ready yet.
   *
   * <p>Value-type: int
   */
  REQUEST_TRACE_ATTEMPTS("advanced.request.trace.attempts"),
  /**
   * The interval between each attempt.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  REQUEST_TRACE_INTERVAL("advanced.request.trace.interval"),
  /**
   * The consistency level to use for trace queries.
   *
   * <p>Value-type: {@link String}
   */
  REQUEST_TRACE_CONSISTENCY("advanced.request.trace.consistency"),

  /**
   * List of enabled session-level metrics.
   *
   * <p>Value type: {@link java.util.List List}&#60;{@link String}&#62;
   */
  METRICS_SESSION_ENABLED("advanced.metrics.session.enabled"),
  /**
   * List of enabled node-level metrics.
   *
   * <p>Value type: {@link java.util.List List}&#60;{@link String}&#62;
   */
  METRICS_NODE_ENABLED("advanced.metrics.node.enabled"),
  /**
   * The largest latency that we expect to record for requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_SESSION_CQL_REQUESTS_HIGHEST("advanced.metrics.session.cql-requests.highest-latency"),
  /**
   * The number of significant decimal digits to which internal structures will maintain for
   * requests.
   *
   * <p>Value-type: int
   */
  METRICS_SESSION_CQL_REQUESTS_DIGITS("advanced.metrics.session.cql-requests.significant-digits"),
  /**
   * The interval at which percentile data is refreshed for requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_SESSION_CQL_REQUESTS_INTERVAL("advanced.metrics.session.cql-requests.refresh-interval"),
  /**
   * The largest latency that we expect to record for throttling.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_SESSION_THROTTLING_HIGHEST("advanced.metrics.session.throttling.delay.highest-latency"),
  /**
   * The number of significant decimal digits to which internal structures will maintain for
   * throttling.
   *
   * <p>Value-type: int
   */
  METRICS_SESSION_THROTTLING_DIGITS("advanced.metrics.session.throttling.delay.significant-digits"),
  /**
   * The interval at which percentile data is refreshed for throttling.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_SESSION_THROTTLING_INTERVAL("advanced.metrics.session.throttling.delay.refresh-interval"),
  /**
   * The largest latency that we expect to record for requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_NODE_CQL_MESSAGES_HIGHEST("advanced.metrics.node.cql-messages.highest-latency"),
  /**
   * The number of significant decimal digits to which internal structures will maintain for
   * requests.
   *
   * <p>Value-type: int
   */
  METRICS_NODE_CQL_MESSAGES_DIGITS("advanced.metrics.node.cql-messages.significant-digits"),
  /**
   * The interval at which percentile data is refreshed for requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_NODE_CQL_MESSAGES_INTERVAL("advanced.metrics.node.cql-messages.refresh-interval"),

  /**
   * Whether or not to disable the Nagle algorithm.
   *
   * <p>Value-type: boolean
   */
  SOCKET_TCP_NODELAY("advanced.socket.tcp-no-delay"),
  /**
   * Whether or not to enable TCP keep-alive probes.
   *
   * <p>Value-type: boolean
   */
  SOCKET_KEEP_ALIVE("advanced.socket.keep-alive"),
  /**
   * Whether or not to allow address reuse.
   *
   * <p>Value-type: boolean
   */
  SOCKET_REUSE_ADDRESS("advanced.socket.reuse-address"),
  /**
   * Sets the linger interval.
   *
   * <p>Value-type: int
   */
  SOCKET_LINGER_INTERVAL("advanced.socket.linger-interval"),
  /**
   * Sets a hint to the size of the underlying buffers for incoming network I/O.
   *
   * <p>Value-type: int
   */
  SOCKET_RECEIVE_BUFFER_SIZE("advanced.socket.receive-buffer-size"),
  /**
   * Sets a hint to the size of the underlying buffers for outgoing network I/O.
   *
   * <p>Value-type: int
   */
  SOCKET_SEND_BUFFER_SIZE("advanced.socket.send-buffer-size"),

  /**
   * The connection heartbeat interval.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  HEARTBEAT_INTERVAL("advanced.heartbeat.interval"),
  /**
   * How long the driver waits for the response to a heartbeat.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  HEARTBEAT_TIMEOUT("advanced.heartbeat.timeout"),

  /**
   * How long the driver waits to propagate a Topology event.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METADATA_TOPOLOGY_WINDOW("advanced.metadata.topology-event-debouncer.window"),
  /**
   * The maximum number of events that can accumulate.
   *
   * <p>Value-type: int
   */
  METADATA_TOPOLOGY_MAX_EVENTS("advanced.metadata.topology-event-debouncer.max-events"),
  /**
   * Whether schema metadata is enabled.
   *
   * <p>Value-type: boolean
   */
  METADATA_SCHEMA_ENABLED("advanced.metadata.schema.enabled"),
  /**
   * The timeout for the requests to the schema tables.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METADATA_SCHEMA_REQUEST_TIMEOUT("advanced.metadata.schema.request-timeout"),
  /**
   * The page size for the requests to the schema tables.
   *
   * <p>Value-type: int
   */
  METADATA_SCHEMA_REQUEST_PAGE_SIZE("advanced.metadata.schema.request-page-size"),
  /**
   * The list of keyspaces for which schema and token metadata should be maintained.
   *
   * <p>Value type: {@link java.util.List List}&#60;{@link String}&#62;
   */
  METADATA_SCHEMA_REFRESHED_KEYSPACES("advanced.metadata.schema.refreshed-keyspaces"),
  /**
   * How long the driver waits to apply a refresh.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METADATA_SCHEMA_WINDOW("advanced.metadata.schema.debouncer.window"),
  /**
   * The maximum number of refreshes that can accumulate.
   *
   * <p>Value-type: int
   */
  METADATA_SCHEMA_MAX_EVENTS("advanced.metadata.schema.debouncer.max-events"),
  /**
   * Whether token metadata is enabled.
   *
   * <p>Value-type: boolean
   */
  METADATA_TOKEN_MAP_ENABLED("advanced.metadata.token-map.enabled"),

  /**
   * How long the driver waits for responses to control queries.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTROL_CONNECTION_TIMEOUT("advanced.control-connection.timeout"),
  /**
   * The interval between each schema agreement check attempt.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTROL_CONNECTION_AGREEMENT_INTERVAL("advanced.control-connection.schema-agreement.interval"),
  /**
   * The timeout after which schema agreement fails.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTROL_CONNECTION_AGREEMENT_TIMEOUT("advanced.control-connection.schema-agreement.timeout"),
  /**
   * Whether to log a warning if schema agreement fails.
   *
   * <p>Value-type: boolean
   */
  CONTROL_CONNECTION_AGREEMENT_WARN("advanced.control-connection.schema-agreement.warn-on-failure"),

  /**
   * Whether `Session.prepare` calls should be sent to all nodes in the cluster.
   *
   * <p>Value-type: boolean
   */
  PREPARE_ON_ALL_NODES("advanced.prepared-statements.prepare-on-all-nodes"),
  /**
   * Whether the driver tries to prepare on new nodes at all.
   *
   * <p>Value-type: boolean
   */
  REPREPARE_ENABLED("advanced.prepared-statements.reprepare-on-up.enabled"),
  /**
   * Whether to check `system.prepared_statements` on the target node before repreparing.
   *
   * <p>Value-type: boolean
   */
  REPREPARE_CHECK_SYSTEM_TABLE("advanced.prepared-statements.reprepare-on-up.check-system-table"),
  /**
   * The maximum number of statements that should be reprepared.
   *
   * <p>Value-type: int
   */
  REPREPARE_MAX_STATEMENTS("advanced.prepared-statements.reprepare-on-up.max-statements"),
  /**
   * The maximum number of concurrent requests when repreparing.
   *
   * <p>Value-type: int
   */
  REPREPARE_MAX_PARALLELISM("advanced.prepared-statements.reprepare-on-up.max-parallelism"),
  /**
   * The request timeout when repreparing.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  REPREPARE_TIMEOUT("advanced.prepared-statements.reprepare-on-up.timeout"),

  /**
   * The number of threads in the I/O group.
   *
   * <p>Value-type: int
   */
  NETTY_IO_SIZE("advanced.netty.io-group.size"),
  /**
   * Quiet period for I/O group shutdown.
   *
   * <p>Value-type: int
   */
  NETTY_IO_SHUTDOWN_QUIET_PERIOD("advanced.netty.io-group.shutdown.quiet-period"),
  /**
   * Max time to wait for I/O group shutdown.
   *
   * <p>Value-type: int
   */
  NETTY_IO_SHUTDOWN_TIMEOUT("advanced.netty.io-group.shutdown.timeout"),
  /**
   * Units for I/O group quiet period and timeout.
   *
   * <p>Value-type: {@link String}
   */
  NETTY_IO_SHUTDOWN_UNIT("advanced.netty.io-group.shutdown.unit"),
  /**
   * The number of threads in the Admin group.
   *
   * <p>Value-type: int
   */
  NETTY_ADMIN_SIZE("advanced.netty.admin-group.size"),
  /**
   * Quiet period for admin group shutdown.
   *
   * <p>Value-type: int
   */
  NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD("advanced.netty.admin-group.shutdown.quiet-period"),
  /**
   * Max time to wait for admin group shutdown.
   *
   * <p>Value-type: {@link String}
   */
  NETTY_ADMIN_SHUTDOWN_TIMEOUT("advanced.netty.admin-group.shutdown.timeout"),
  /**
   * Units for admin group quite period and timeout.
   *
   * <p>Value-type: {@link String}
   */
  NETTY_ADMIN_SHUTDOWN_UNIT("advanced.netty.admin-group.shutdown.unit"),

  /** @deprecated This option was removed in version 4.6.1. */
  @Deprecated
  COALESCER_MAX_RUNS("advanced.coalescer.max-runs-with-no-work"),
  /**
   * The coalescer reschedule interval.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  COALESCER_INTERVAL("advanced.coalescer.reschedule-interval"),

  /**
   * Whether to resolve the addresses passed to `basic.contact-points`.
   *
   * <p>Value-type: boolean
   */
  RESOLVE_CONTACT_POINTS("advanced.resolve-contact-points"),

  /**
   * This is how frequent the timer should wake up to check for timed-out tasks or speculative
   * executions.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  NETTY_TIMER_TICK_DURATION("advanced.netty.timer.tick-duration"),
  /**
   * Number of ticks in the Timer wheel.
   *
   * <p>Value-type: int
   */
  NETTY_TIMER_TICKS_PER_WHEEL("advanced.netty.timer.ticks-per-wheel"),

  /**
   * Whether logging of server warnings generated during query execution should be disabled by the
   * driver.
   *
   * <p>Value-type: boolean
   */
  REQUEST_LOG_WARNINGS("advanced.request.log-warnings"),

  /**
   * Whether the threads created by the driver should be daemon threads.
   *
   * <p>Value-type: boolean
   */
  NETTY_DAEMON("advanced.netty.daemon"),

  /**
   * The location of the cloud secure bundle used to connect to DataStax Apache Cassandra as a
   * service.
   *
   * <p>Value-type: {@link String}
   */
  CLOUD_SECURE_CONNECT_BUNDLE("basic.cloud.secure-connect-bundle"),

  /**
   * Whether the slow replica avoidance should be enabled in the default LBP.
   *
   * <p>Value-type: boolean
   */
  LOAD_BALANCING_POLICY_SLOW_AVOIDANCE("basic.load-balancing-policy.slow-replica-avoidance"),

  /**
   * The timeout to use when establishing driver connections.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONNECTION_CONNECT_TIMEOUT("advanced.connection.connect-timeout"),

  /**
   * The maximum number of live sessions that are allowed to coexist in a given VM.
   *
   * <p>Value-type: int
   */
  SESSION_LEAK_THRESHOLD("advanced.session-leak.threshold"),
  /**
   * The period of inactivity after which the node level metrics will be evicted. The eviction will
   * happen only if none of the enabled node-level metrics is updated for a given node within this
   * time window.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_NODE_EXPIRE_AFTER("advanced.metrics.node.expire-after"),

  /**
   * The classname of the desired MetricsFactory implementation.
   *
   * <p>Value-type: {@link String}
   */
  METRICS_FACTORY_CLASS("advanced.metrics.factory.class"),

  /**
   * The maximum number of nodes from remote DCs to include in query plans.
   *
   * <p>Value-Type: int
   */
  LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC(
      "advanced.load-balancing-policy.dc-failover.max-nodes-per-remote-dc"),
  /**
   * Whether to consider nodes from remote DCs if the request's consistency level is local.
   *
   * <p>Value-Type: boolean
   */
  LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS(
      "advanced.load-balancing-policy.dc-failover.allow-for-local-consistency-levels"),

  /**
   * The classname of the desired {@code MetricIdGenerator} implementation.
   *
   * <p>Value-type: {@link String}
   */
  METRICS_ID_GENERATOR_CLASS("advanced.metrics.id-generator.class"),

  /**
   * The value of the prefix to prepend to all metric names.
   *
   * <p>Value-type: {@link String}
   */
  METRICS_ID_GENERATOR_PREFIX("advanced.metrics.id-generator.prefix"),

  /**
   * The class name of a custom {@link
   * com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator}.
   *
   * <p>Value-Type: {@link String}
   */
  LOAD_BALANCING_DISTANCE_EVALUATOR_CLASS("basic.load-balancing-policy.evaluator.class"),

  /**
   * The shortest latency that we expect to record for requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_SESSION_CQL_REQUESTS_LOWEST("advanced.metrics.session.cql-requests.lowest-latency"),
  /**
   * Optional service-level objectives to meet, as a list of latencies to track.
   *
   * <p>Value-type: List of {@link java.time.Duration Duration}
   */
  METRICS_SESSION_CQL_REQUESTS_SLO("advanced.metrics.session.cql-requests.slo"),

  /**
   * The shortest latency that we expect to record for throttling.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_SESSION_THROTTLING_LOWEST("advanced.metrics.session.throttling.delay.lowest-latency"),
  /**
   * Optional service-level objectives to meet, as a list of latencies to track.
   *
   * <p>Value-type: List of {@link java.time.Duration Duration}
   */
  METRICS_SESSION_THROTTLING_SLO("advanced.metrics.session.throttling.delay.slo"),

  /**
   * The shortest latency that we expect to record for requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_NODE_CQL_MESSAGES_LOWEST("advanced.metrics.node.cql-messages.lowest-latency"),
  /**
   * Optional service-level objectives to meet, as a list of latencies to track.
   *
   * <p>Value-type: List of {@link java.time.Duration Duration}
   */
  METRICS_NODE_CQL_MESSAGES_SLO("advanced.metrics.node.cql-messages.slo"),

  /**
   * Whether the prepared statements cache use weak values.
   *
   * <p>Value-type: boolean
   */
  PREPARED_CACHE_WEAK_VALUES("advanced.prepared-statements.prepared-cache.weak-values"),

  /**
   * The classes of session-wide components that track the outcome of requests.
   *
   * <p>Value-type: List of {@link String}
   */
  REQUEST_TRACKER_CLASSES("advanced.request-tracker.classes"),

  /**
   * The classes of session-wide components that listen for node state changes.
   *
   * <p>Value-type: List of {@link String}
   */
  METADATA_NODE_STATE_LISTENER_CLASSES("advanced.node-state-listener.classes"),

  /**
   * The classes of session-wide components that listen for schema changes.
   *
   * <p>Value-type: List of {@link String}
   */
  METADATA_SCHEMA_CHANGE_LISTENER_CLASSES("advanced.schema-change-listener.classes"),
  /**
   * Optional list of percentiles to publish for histogram metrics. Produces an additional time
   * series for each requested percentile. This percentile is computed locally, and so can't be
   * aggregated with percentiles computed across other dimensions (e.g. in a different instance).
   *
   * <p>Value type: {@link java.util.List List}&#60;{@link Double}&#62;
   */
  METRICS_HISTOGRAM_PUBLISH_LOCAL_PERCENTILES(
      "advanced.metrics.histograms.publish-local-percentiles"),
  /**
   * Adds histogram buckets used to generate aggregable percentile approximations in monitoring
   * systems that have query facilities to do so (e.g. Prometheus histogram_quantile, Atlas
   * percentiles).
   *
   * <p>Value-type: boolean
   */
  METRICS_GENERATE_AGGREGABLE_HISTOGRAMS("advanced.metrics.histograms.generate-aggregable"),
  /**
   * The duration between attempts to reload the keystore.
   *
   * <p>Value-type: {@link java.time.Duration}
   */
  SSL_KEYSTORE_RELOAD_INTERVAL("advanced.ssl-engine-factory.keystore-reload-interval"),
  /**
   * Ordered preference list of remote dcs optionally supplied for automatic failover.
   *
   * <p>Value type: {@link java.util.List List}&#60;{@link String}&#62;
   */
  LOAD_BALANCING_DC_FAILOVER_PREFERRED_REMOTE_DCS(
      "advanced.load-balancing-policy.dc-failover.preferred-remote-dcs");

  private final String path;

  DefaultDriverOption(String path) {
    this.path = path;
  }

  @NonNull
  @Override
  public String getPath() {
    return path;
  }
}
