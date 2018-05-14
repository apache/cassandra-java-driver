/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.config;

/**
 * Built-in driver options for the core driver.
 *
 * <p>Refer to {@code reference.conf} in the driver codebase for a full description of each option.
 */
public enum DefaultDriverOption implements DriverOption {
  CONTACT_POINTS("contact-points", false),

  PROTOCOL_VERSION("protocol.version", false),
  PROTOCOL_MAX_FRAME_LENGTH("protocol.max-frame-length", true),
  PROTOCOL_COMPRESSOR_CLASS("protocol.compressor.class", false),

  SESSION_NAME("session-name", false),
  SESSION_KEYSPACE("session-keyspace", false),
  CONFIG_RELOAD_INTERVAL("config-reload-interval", false),

  CONNECTION_INIT_QUERY_TIMEOUT("connection.init-query-timeout", true),
  CONNECTION_SET_KEYSPACE_TIMEOUT("connection.set-keyspace-timeout", true),
  CONNECTION_MAX_REQUESTS("connection.max-requests-per-connection", true),
  CONNECTION_HEARTBEAT_INTERVAL("connection.heartbeat.interval", true),
  CONNECTION_HEARTBEAT_TIMEOUT("connection.heartbeat.timeout", true),
  CONNECTION_MAX_ORPHAN_REQUESTS("connection.max-orphan-requests", true),
  CONNECTION_WARN_INIT_ERROR("connection.warn-on-init-error", true),
  CONNECTION_POOL_LOCAL_SIZE("connection.pool.local.size", true),
  CONNECTION_POOL_REMOTE_SIZE("connection.pool.remote.size", true),

  CONNECTION_SOCKET_TCP_NODELAY("connection.socket.tcpNoDelay", true),
  CONNECTION_SOCKET_KEEP_ALIVE("connection.socket.keepAlive", false),
  CONNECTION_SOCKET_REUSE_ADDRESS("connection.socket.reuseAddress", false),
  CONNECTION_SOCKET_LINGER_INTERVAL("connection.socket.lingerInterval", false),
  CONNECTION_SOCKET_RECEIVE_BUFFER_SIZE("connection.socket.receiveBufferSize", false),
  CONNECTION_SOCKET_SEND_BUFFER_SIZE("connection.socket.sendBufferSize", false),

  REQUEST_TIMEOUT("request.timeout", true),
  REQUEST_CONSISTENCY("request.consistency", true),
  REQUEST_PAGE_SIZE("request.page-size", true),
  REQUEST_SERIAL_CONSISTENCY("request.serial-consistency", true),
  REQUEST_WARN_IF_SET_KEYSPACE("request.warn-if-set-keyspace", true),
  REQUEST_DEFAULT_IDEMPOTENCE("request.default-idempotence", true),
  RETRY_POLICY("request.retry-policy", true),
  SPECULATIVE_EXECUTION_POLICY("request.speculative-execution-policy", false),
  SPECULATIVE_EXECUTION_MAX("request.speculative-execution-policy.max-executions", false),
  SPECULATIVE_EXECUTION_DELAY("request.speculative-execution-policy.delay", false),
  REQUEST_TRACE_ATTEMPTS("request.trace.attempts", true),
  REQUEST_TRACE_INTERVAL("request.trace.interval", true),
  REQUEST_TRACE_CONSISTENCY("request.trace.consistency", true),
  REQUEST_THROTTLER_CLASS("request.throttler.class", true),
  REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS("request.throttler.max-concurrent-requests", false),
  REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND("request.throttler.max-requests-per-second", false),
  REQUEST_THROTTLER_MAX_QUEUE_SIZE("request.throttler.max-queue-size", false),
  REQUEST_THROTTLER_DRAIN_INTERVAL("request.throttler.drain-interval", false),
  REQUEST_TRACKER_CLASS("request.tracker.class", false),

  REQUEST_LOGGER_SUCCESS_ENABLED("request.tracker.logs.success.enabled", false),
  REQUEST_LOGGER_SLOW_THRESHOLD("request.tracker.logs.slow.threshold", false),
  REQUEST_LOGGER_SLOW_ENABLED("request.tracker.logs.slow.enabled", false),
  REQUEST_LOGGER_ERROR_ENABLED("request.tracker.logs.error.enabled", false),
  REQUEST_LOGGER_MAX_QUERY_LENGTH("request.tracker.logs.max-query-length", false),
  REQUEST_LOGGER_VALUES("request.tracker.logs.show-values", false),
  REQUEST_LOGGER_MAX_VALUE_LENGTH("request.tracker.logs.max-value-length", false),
  REQUEST_LOGGER_MAX_VALUES("request.tracker.logs.max-values", false),
  REQUEST_LOGGER_STACK_TRACES("request.tracker.logs.show-stack-traces", false),

  CONTROL_CONNECTION_TIMEOUT("connection.control-connection.timeout", true),
  CONTROL_CONNECTION_AGREEMENT_INTERVAL(
      "connection.control-connection.schema-agreement.interval", true),
  CONTROL_CONNECTION_AGREEMENT_TIMEOUT(
      "connection.control-connection.schema-agreement.timeout", true),
  CONTROL_CONNECTION_AGREEMENT_WARN(
      "connection.control-connection.schema-agreement.warn-on-failure", true),

  COALESCER_MAX_RUNS("connection.coalescer.max-runs-with-no-work", false),
  COALESCER_INTERVAL("connection.coalescer.reschedule-interval", false),

  LOAD_BALANCING_POLICY("load-balancing-policy", true),
  LOAD_BALANCING_LOCAL_DATACENTER("load-balancing-policy.local-datacenter", false),
  LOAD_BALANCING_FILTER_CLASS("load-balancing-policy.filter.class", false),

  RECONNECTION_POLICY_CLASS("connection.reconnection-policy.class", true),
  RECONNECTION_BASE_DELAY("connection.reconnection-policy.base-delay", false),
  RECONNECTION_MAX_DELAY("connection.reconnection-policy.max-delay", false),

  PREPARE_ON_ALL_NODES("prepared-statements.prepare-on-all-nodes", true),
  REPREPARE_ENABLED("prepared-statements.reprepare-on-up.enabled", true),
  REPREPARE_CHECK_SYSTEM_TABLE("prepared-statements.reprepare-on-up.check-system-table", false),
  REPREPARE_MAX_STATEMENTS("prepared-statements.reprepare-on-up.max-statements", false),
  REPREPARE_MAX_PARALLELISM("prepared-statements.reprepare-on-up.max-parallelism", false),
  REPREPARE_TIMEOUT("prepared-statements.reprepare-on-up.timeout", false),

  ADDRESS_TRANSLATOR_CLASS("address-translator.class", true),

  AUTH_PROVIDER_CLASS("protocol.auth-provider.class", false),
  AUTH_PROVIDER_USER_NAME("protocol.auth-provider.username", false),
  AUTH_PROVIDER_PASSWORD("protocol.auth-provider.password", false),

  SSL_ENGINE_FACTORY_CLASS("ssl-engine-factory.class", false),
  SSL_CIPHER_SUITES("ssl-engine-factory.cipher-suites", false),

  METADATA_TOPOLOGY_WINDOW("metadata.topology-event-debouncer.window", true),
  METADATA_TOPOLOGY_MAX_EVENTS("metadata.topology-event-debouncer.max-events", true),
  METADATA_SCHEMA_ENABLED("metadata.schema.enabled", true),
  METADATA_SCHEMA_REQUEST_TIMEOUT("metadata.schema.request-timeout", true),
  METADATA_SCHEMA_REQUEST_PAGE_SIZE("metadata.schema.request-page-size", true),
  METADATA_SCHEMA_REFRESHED_KEYSPACES("metadata.schema.refreshed-keyspaces", false),
  METADATA_SCHEMA_WINDOW("metadata.schema.debouncer.window", true),
  METADATA_SCHEMA_MAX_EVENTS("metadata.schema.debouncer.max-events", true),
  METADATA_TOKEN_MAP_ENABLED("metadata.token-map.enabled", true),
  METADATA_NODE_STATE_LISTENER_CLASS("metadata.node-state-listener.class", false),
  METADATA_SCHEMA_CHANGE_LISTENER_CLASS("metadata.schema-change-listener.class", false),

  TIMESTAMP_GENERATOR_CLASS("request.timestamp-generator.class", true),
  TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK("request.timestamp-generator.force-java-clock", false),
  TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD(
      "request.timestamp-generator.drift-warning.threshold", false),
  TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL(
      "request.timestamp-generator.drift-warning.interval", false),

  METRICS_SESSION_ENABLED("metrics.session.enabled", false),
  METRICS_NODE_ENABLED("metrics.node.enabled", false),
  METRICS_SESSION_CQL_REQUESTS_HIGHEST("metrics.session.cql-requests.highest-latency", false),
  METRICS_SESSION_CQL_REQUESTS_DIGITS("metrics.session.cql-requests.significant-digits", false),
  METRICS_SESSION_CQL_REQUESTS_INTERVAL("metrics.session.cql-requests.refresh-interval", false),
  METRICS_SESSION_THROTTLING_HIGHEST("metrics.session.throttling.delay.highest-latency", false),
  METRICS_SESSION_THROTTLING_DIGITS("metrics.session.throttling.delay.significant-digits", false),
  METRICS_SESSION_THROTTLING_INTERVAL("metrics.session.throttling.delay.refresh-interval", false),
  METRICS_NODE_CQL_MESSAGES_HIGHEST("metrics.node.cql-messages.highest-latency", false),
  METRICS_NODE_CQL_MESSAGES_DIGITS("metrics.node.cql-messages.significant-digits", false),
  METRICS_NODE_CQL_MESSAGES_INTERVAL("metrics.node.cql-messages.refresh-interval", false),

  NETTY_IO_SIZE("netty.io-group.size", false),
  NETTY_IO_SHUTDOWN_QUIET_PERIOD("netty.io-group.shutdown.quiet-period", false),
  NETTY_IO_SHUTDOWN_TIMEOUT("netty.io-group.shutdown.timeout", false),
  NETTY_IO_SHUTDOWN_UNIT("netty.io-group.shutdown.unit", false),
  NETTY_ADMIN_SIZE("netty.admin-group.size", false),
  NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD("netty.admin-group.shutdown.quiet-period", false),
  NETTY_ADMIN_SHUTDOWN_TIMEOUT("netty.admin-group.shutdown.timeout", false),
  NETTY_ADMIN_SHUTDOWN_UNIT("netty.admin-group.shutdown.unit", false),
  ;

  private final String path;
  private final boolean required;

  DefaultDriverOption(String path, boolean required) {
    this.path = path;
    this.required = required;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public boolean required() {
    return required;
  }
}
