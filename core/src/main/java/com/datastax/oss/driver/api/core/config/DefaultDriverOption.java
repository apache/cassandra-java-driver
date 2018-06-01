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
  CONTACT_POINTS("basic.contact-points", false),
  SESSION_NAME("basic.session-name", false),
  SESSION_KEYSPACE("basic.session-keyspace", false),
  CONFIG_RELOAD_INTERVAL("basic.config-reload-interval", false),

  REQUEST_TIMEOUT("basic.request.timeout", true),
  REQUEST_CONSISTENCY("basic.request.consistency", true),
  REQUEST_PAGE_SIZE("basic.request.page-size", true),
  REQUEST_SERIAL_CONSISTENCY("basic.request.serial-consistency", true),
  REQUEST_DEFAULT_IDEMPOTENCE("basic.request.default-idempotence", true),

  LOAD_BALANCING_POLICY("basic.load-balancing-policy", true),
  LOAD_BALANCING_LOCAL_DATACENTER("basic.load-balancing-policy.local-datacenter", false),
  LOAD_BALANCING_FILTER_CLASS("basic.load-balancing-policy.filter.class", false),

  CONNECTION_INIT_QUERY_TIMEOUT("advanced.connection.init-query-timeout", true),
  CONNECTION_SET_KEYSPACE_TIMEOUT("advanced.connection.set-keyspace-timeout", true),
  CONNECTION_MAX_REQUESTS("advanced.connection.max-requests-per-connection", true),
  CONNECTION_MAX_ORPHAN_REQUESTS("advanced.connection.max-orphan-requests", true),
  CONNECTION_WARN_INIT_ERROR("advanced.connection.warn-on-init-error", true),
  CONNECTION_POOL_LOCAL_SIZE("advanced.connection.pool.local.size", true),
  CONNECTION_POOL_REMOTE_SIZE("advanced.connection.pool.remote.size", true),

  RECONNECTION_POLICY_CLASS("advanced.reconnection-policy.class", true),
  RECONNECTION_BASE_DELAY("advanced.reconnection-policy.base-delay", false),
  RECONNECTION_MAX_DELAY("advanced.reconnection-policy.max-delay", false),

  RETRY_POLICY("advanced.retry-policy", true),

  SPECULATIVE_EXECUTION_POLICY("advanced.speculative-execution-policy", false),
  SPECULATIVE_EXECUTION_MAX("advanced.speculative-execution-policy.max-executions", false),
  SPECULATIVE_EXECUTION_DELAY("advanced.speculative-execution-policy.delay", false),

  AUTH_PROVIDER_CLASS("advanced.auth-provider.class", false),
  AUTH_PROVIDER_USER_NAME("advanced.auth-provider.username", false),
  AUTH_PROVIDER_PASSWORD("advanced.auth-provider.password", false),

  SSL_ENGINE_FACTORY_CLASS("advanced.ssl-engine-factory.class", false),
  SSL_CIPHER_SUITES("advanced.ssl-engine-factory.cipher-suites", false),

  TIMESTAMP_GENERATOR_CLASS("advanced.timestamp-generator.class", true),
  TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK("advanced.timestamp-generator.force-java-clock", false),
  TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD(
      "advanced.timestamp-generator.drift-warning.threshold", false),
  TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL(
      "advanced.timestamp-generator.drift-warning.interval", false),

  REQUEST_TRACKER_CLASS("advanced.request-tracker.class", false),
  REQUEST_LOGGER_SUCCESS_ENABLED("advanced.request-tracker.logs.success.enabled", false),
  REQUEST_LOGGER_SLOW_THRESHOLD("advanced.request-tracker.logs.slow.threshold", false),
  REQUEST_LOGGER_SLOW_ENABLED("advanced.request-tracker.logs.slow.enabled", false),
  REQUEST_LOGGER_ERROR_ENABLED("advanced.request-tracker.logs.error.enabled", false),
  REQUEST_LOGGER_MAX_QUERY_LENGTH("advanced.request-tracker.logs.max-query-length", false),
  REQUEST_LOGGER_VALUES("advanced.request-tracker.logs.show-values", false),
  REQUEST_LOGGER_MAX_VALUE_LENGTH("advanced.request-tracker.logs.max-value-length", false),
  REQUEST_LOGGER_MAX_VALUES("advanced.request-tracker.logs.max-values", false),
  REQUEST_LOGGER_STACK_TRACES("advanced.request-tracker.logs.show-stack-traces", false),

  REQUEST_THROTTLER_CLASS("advanced.throttler.class", true),
  REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS("advanced.throttler.max-concurrent-requests", false),
  REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND("advanced.throttler.max-requests-per-second", false),
  REQUEST_THROTTLER_MAX_QUEUE_SIZE("advanced.throttler.max-queue-size", false),
  REQUEST_THROTTLER_DRAIN_INTERVAL("advanced.throttler.drain-interval", false),

  METADATA_NODE_STATE_LISTENER_CLASS("advanced.node-state-listener.class", false),

  METADATA_SCHEMA_CHANGE_LISTENER_CLASS("advanced.schema-change-listener.class", false),

  ADDRESS_TRANSLATOR_CLASS("advanced.address-translator.class", true),

  PROTOCOL_VERSION("advanced.protocol.version", false),
  PROTOCOL_COMPRESSION("advanced.protocol.compression", false),
  PROTOCOL_MAX_FRAME_LENGTH("advanced.protocol.max-frame-length", true),

  REQUEST_WARN_IF_SET_KEYSPACE("advanced.request.warn-if-set-keyspace", true),
  REQUEST_TRACE_ATTEMPTS("advanced.request.trace.attempts", true),
  REQUEST_TRACE_INTERVAL("advanced.request.trace.interval", true),
  REQUEST_TRACE_CONSISTENCY("advanced.request.trace.consistency", true),

  METRICS_SESSION_ENABLED("advanced.metrics.session.enabled", false),
  METRICS_NODE_ENABLED("advanced.metrics.node.enabled", false),
  METRICS_SESSION_CQL_REQUESTS_HIGHEST(
      "advanced.metrics.session.cql-requests.highest-latency", false),
  METRICS_SESSION_CQL_REQUESTS_DIGITS(
      "advanced.metrics.session.cql-requests.significant-digits", false),
  METRICS_SESSION_CQL_REQUESTS_INTERVAL(
      "advanced.metrics.session.cql-requests.refresh-interval", false),
  METRICS_SESSION_THROTTLING_HIGHEST(
      "advanced.metrics.session.throttling.delay.highest-latency", false),
  METRICS_SESSION_THROTTLING_DIGITS(
      "advanced.metrics.session.throttling.delay.significant-digits", false),
  METRICS_SESSION_THROTTLING_INTERVAL(
      "advanced.metrics.session.throttling.delay.refresh-interval", false),
  METRICS_NODE_CQL_MESSAGES_HIGHEST("advanced.metrics.node.cql-messages.highest-latency", false),
  METRICS_NODE_CQL_MESSAGES_DIGITS("advanced.metrics.node.cql-messages.significant-digits", false),
  METRICS_NODE_CQL_MESSAGES_INTERVAL("advanced.metrics.node.cql-messages.refresh-interval", false),

  SOCKET_TCP_NODELAY("advanced.socket.tcp-no-delay", true),
  SOCKET_KEEP_ALIVE("advanced.socket.keep-alive", false),
  SOCKET_REUSE_ADDRESS("advanced.socket.reuse-address", false),
  SOCKET_LINGER_INTERVAL("advanced.socket.linger-interval", false),
  SOCKET_RECEIVE_BUFFER_SIZE("advanced.socket.receive-buffer-size", false),
  SOCKET_SEND_BUFFER_SIZE("advanced.socket.send-buffer-size", false),

  HEARTBEAT_INTERVAL("advanced.heartbeat.interval", true),
  HEARTBEAT_TIMEOUT("advanced.heartbeat.timeout", true),

  METADATA_TOPOLOGY_WINDOW("advanced.metadata.topology-event-debouncer.window", true),
  METADATA_TOPOLOGY_MAX_EVENTS("advanced.metadata.topology-event-debouncer.max-events", true),
  METADATA_SCHEMA_ENABLED("advanced.metadata.schema.enabled", true),
  METADATA_SCHEMA_REQUEST_TIMEOUT("advanced.metadata.schema.request-timeout", true),
  METADATA_SCHEMA_REQUEST_PAGE_SIZE("advanced.metadata.schema.request-page-size", true),
  METADATA_SCHEMA_REFRESHED_KEYSPACES("advanced.metadata.schema.refreshed-keyspaces", false),
  METADATA_SCHEMA_WINDOW("advanced.metadata.schema.debouncer.window", true),
  METADATA_SCHEMA_MAX_EVENTS("advanced.metadata.schema.debouncer.max-events", true),
  METADATA_TOKEN_MAP_ENABLED("advanced.metadata.token-map.enabled", true),

  CONTROL_CONNECTION_TIMEOUT("advanced.control-connection.timeout", true),
  CONTROL_CONNECTION_AGREEMENT_INTERVAL(
      "advanced.control-connection.schema-agreement.interval", true),
  CONTROL_CONNECTION_AGREEMENT_TIMEOUT(
      "advanced.control-connection.schema-agreement.timeout", true),
  CONTROL_CONNECTION_AGREEMENT_WARN(
      "advanced.control-connection.schema-agreement.warn-on-failure", true),

  PREPARE_ON_ALL_NODES("advanced.prepared-statements.prepare-on-all-nodes", true),
  REPREPARE_ENABLED("advanced.prepared-statements.reprepare-on-up.enabled", true),
  REPREPARE_CHECK_SYSTEM_TABLE(
      "advanced.prepared-statements.reprepare-on-up.check-system-table", false),
  REPREPARE_MAX_STATEMENTS("advanced.prepared-statements.reprepare-on-up.max-statements", false),
  REPREPARE_MAX_PARALLELISM("advanced.prepared-statements.reprepare-on-up.max-parallelism", false),
  REPREPARE_TIMEOUT("advanced.prepared-statements.reprepare-on-up.timeout", false),

  NETTY_IO_SIZE("advanced.netty.io-group.size", false),
  NETTY_IO_SHUTDOWN_QUIET_PERIOD("advanced.netty.io-group.shutdown.quiet-period", false),
  NETTY_IO_SHUTDOWN_TIMEOUT("advanced.netty.io-group.shutdown.timeout", false),
  NETTY_IO_SHUTDOWN_UNIT("advanced.netty.io-group.shutdown.unit", false),
  NETTY_ADMIN_SIZE("advanced.netty.admin-group.size", false),
  NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD("advanced.netty.admin-group.shutdown.quiet-period", false),
  NETTY_ADMIN_SHUTDOWN_TIMEOUT("advanced.netty.admin-group.shutdown.timeout", false),
  NETTY_ADMIN_SHUTDOWN_UNIT("advanced.netty.admin-group.shutdown.unit", false),

  COALESCER_MAX_RUNS("advanced.coalescer.max-runs-with-no-work", false),
  COALESCER_INTERVAL("advanced.coalescer.reschedule-interval", false),
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
