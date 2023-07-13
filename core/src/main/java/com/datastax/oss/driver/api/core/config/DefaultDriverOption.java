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
  CONTACT_POINTS("basic.contact-points"),
  SESSION_NAME("basic.session-name"),
  SESSION_KEYSPACE("basic.session-keyspace"),
  CONFIG_RELOAD_INTERVAL("basic.config-reload-interval"),

  REQUEST_TIMEOUT("basic.request.timeout"),
  REQUEST_CONSISTENCY("basic.request.consistency"),
  REQUEST_PAGE_SIZE("basic.request.page-size"),
  REQUEST_SERIAL_CONSISTENCY("basic.request.serial-consistency"),
  REQUEST_DEFAULT_IDEMPOTENCE("basic.request.default-idempotence"),

  LOAD_BALANCING_POLICY("basic.load-balancing-policy"),
  LOAD_BALANCING_POLICY_CLASS("basic.load-balancing-policy.class"),
  LOAD_BALANCING_LOCAL_DATACENTER("basic.load-balancing-policy.local-datacenter"),
  LOAD_BALANCING_FILTER_CLASS("basic.load-balancing-policy.filter.class"),

  CONNECTION_INIT_QUERY_TIMEOUT("advanced.connection.init-query-timeout"),
  CONNECTION_SET_KEYSPACE_TIMEOUT("advanced.connection.set-keyspace-timeout"),
  CONNECTION_MAX_REQUESTS("advanced.connection.max-requests-per-connection"),
  CONNECTION_MAX_ORPHAN_REQUESTS("advanced.connection.max-orphan-requests"),
  CONNECTION_WARN_INIT_ERROR("advanced.connection.warn-on-init-error"),
  CONNECTION_POOL_LOCAL_SIZE("advanced.connection.pool.local.size"),
  CONNECTION_POOL_REMOTE_SIZE("advanced.connection.pool.remote.size"),

  RECONNECT_ON_INIT("advanced.reconnect-on-init"),

  RECONNECTION_POLICY_CLASS("advanced.reconnection-policy.class"),
  RECONNECTION_BASE_DELAY("advanced.reconnection-policy.base-delay"),
  RECONNECTION_MAX_DELAY("advanced.reconnection-policy.max-delay"),

  RETRY_POLICY("advanced.retry-policy"),
  RETRY_POLICY_CLASS("advanced.retry-policy.class"),

  SPECULATIVE_EXECUTION_POLICY("advanced.speculative-execution-policy"),
  SPECULATIVE_EXECUTION_POLICY_CLASS("advanced.speculative-execution-policy.class"),
  SPECULATIVE_EXECUTION_MAX("advanced.speculative-execution-policy.max-executions"),
  SPECULATIVE_EXECUTION_DELAY("advanced.speculative-execution-policy.delay"),

  AUTH_PROVIDER_CLASS("advanced.auth-provider.class"),
  AUTH_PROVIDER_USER_NAME("advanced.auth-provider.username"),
  AUTH_PROVIDER_PASSWORD("advanced.auth-provider.password"),

  SSL_ENGINE_FACTORY_CLASS("advanced.ssl-engine-factory.class"),
  SSL_CIPHER_SUITES("advanced.ssl-engine-factory.cipher-suites"),
  SSL_HOSTNAME_VALIDATION("advanced.ssl-engine-factory.hostname-validation"),
  SSL_KEYSTORE_PATH("advanced.ssl-engine-factory.keystore-path"),
  SSL_KEYSTORE_PASSWORD("advanced.ssl-engine-factory.keystore-password"),
  SSL_TRUSTSTORE_PATH("advanced.ssl-engine-factory.truststore-path"),
  SSL_TRUSTSTORE_PASSWORD("advanced.ssl-engine-factory.truststore-password"),

  TIMESTAMP_GENERATOR_CLASS("advanced.timestamp-generator.class"),
  TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK("advanced.timestamp-generator.force-java-clock"),
  TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD(
      "advanced.timestamp-generator.drift-warning.threshold"),
  TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL("advanced.timestamp-generator.drift-warning.interval"),

  REQUEST_TRACKER_CLASS("advanced.request-tracker.class"),
  REQUEST_LOGGER_SUCCESS_ENABLED("advanced.request-tracker.logs.success.enabled"),
  REQUEST_LOGGER_SLOW_THRESHOLD("advanced.request-tracker.logs.slow.threshold"),
  REQUEST_LOGGER_SLOW_ENABLED("advanced.request-tracker.logs.slow.enabled"),
  REQUEST_LOGGER_ERROR_ENABLED("advanced.request-tracker.logs.error.enabled"),
  REQUEST_LOGGER_MAX_QUERY_LENGTH("advanced.request-tracker.logs.max-query-length"),
  REQUEST_LOGGER_VALUES("advanced.request-tracker.logs.show-values"),
  REQUEST_LOGGER_MAX_VALUE_LENGTH("advanced.request-tracker.logs.max-value-length"),
  REQUEST_LOGGER_MAX_VALUES("advanced.request-tracker.logs.max-values"),
  REQUEST_LOGGER_STACK_TRACES("advanced.request-tracker.logs.show-stack-traces"),

  REQUEST_THROTTLER_CLASS("advanced.throttler.class"),
  REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS("advanced.throttler.max-concurrent-requests"),
  REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND("advanced.throttler.max-requests-per-second"),
  REQUEST_THROTTLER_MAX_QUEUE_SIZE("advanced.throttler.max-queue-size"),
  REQUEST_THROTTLER_DRAIN_INTERVAL("advanced.throttler.drain-interval"),

  METADATA_NODE_STATE_LISTENER_CLASS("advanced.node-state-listener.class"),

  METADATA_SCHEMA_CHANGE_LISTENER_CLASS("advanced.schema-change-listener.class"),

  ADDRESS_TRANSLATOR_CLASS("advanced.address-translator.class"),

  PROTOCOL_VERSION("advanced.protocol.version"),
  PROTOCOL_COMPRESSION("advanced.protocol.compression"),
  PROTOCOL_MAX_FRAME_LENGTH("advanced.protocol.max-frame-length"),

  REQUEST_WARN_IF_SET_KEYSPACE("advanced.request.warn-if-set-keyspace"),
  REQUEST_TRACE_ATTEMPTS("advanced.request.trace.attempts"),
  REQUEST_TRACE_INTERVAL("advanced.request.trace.interval"),
  REQUEST_TRACE_CONSISTENCY("advanced.request.trace.consistency"),

  METRICS_SESSION_ENABLED("advanced.metrics.session.enabled"),
  METRICS_NODE_ENABLED("advanced.metrics.node.enabled"),
  METRICS_SESSION_CQL_REQUESTS_HIGHEST("advanced.metrics.session.cql-requests.highest-latency"),
  METRICS_SESSION_CQL_REQUESTS_DIGITS("advanced.metrics.session.cql-requests.significant-digits"),
  METRICS_SESSION_CQL_REQUESTS_INTERVAL("advanced.metrics.session.cql-requests.refresh-interval"),
  METRICS_SESSION_THROTTLING_HIGHEST("advanced.metrics.session.throttling.delay.highest-latency"),
  METRICS_SESSION_THROTTLING_DIGITS("advanced.metrics.session.throttling.delay.significant-digits"),
  METRICS_SESSION_THROTTLING_INTERVAL("advanced.metrics.session.throttling.delay.refresh-interval"),
  METRICS_NODE_CQL_MESSAGES_HIGHEST("advanced.metrics.node.cql-messages.highest-latency"),
  METRICS_NODE_CQL_MESSAGES_DIGITS("advanced.metrics.node.cql-messages.significant-digits"),
  METRICS_NODE_CQL_MESSAGES_INTERVAL("advanced.metrics.node.cql-messages.refresh-interval"),

  SOCKET_TCP_NODELAY("advanced.socket.tcp-no-delay"),
  SOCKET_KEEP_ALIVE("advanced.socket.keep-alive"),
  SOCKET_REUSE_ADDRESS("advanced.socket.reuse-address"),
  SOCKET_LINGER_INTERVAL("advanced.socket.linger-interval"),
  SOCKET_RECEIVE_BUFFER_SIZE("advanced.socket.receive-buffer-size"),
  SOCKET_SEND_BUFFER_SIZE("advanced.socket.send-buffer-size"),

  HEARTBEAT_INTERVAL("advanced.heartbeat.interval"),
  HEARTBEAT_TIMEOUT("advanced.heartbeat.timeout"),

  METADATA_TOPOLOGY_WINDOW("advanced.metadata.topology-event-debouncer.window"),
  METADATA_TOPOLOGY_MAX_EVENTS("advanced.metadata.topology-event-debouncer.max-events"),
  METADATA_SCHEMA_ENABLED("advanced.metadata.schema.enabled"),
  METADATA_SCHEMA_REQUEST_TIMEOUT("advanced.metadata.schema.request-timeout"),
  METADATA_SCHEMA_REQUEST_PAGE_SIZE("advanced.metadata.schema.request-page-size"),
  METADATA_SCHEMA_REFRESHED_KEYSPACES("advanced.metadata.schema.refreshed-keyspaces"),
  METADATA_SCHEMA_WINDOW("advanced.metadata.schema.debouncer.window"),
  METADATA_SCHEMA_MAX_EVENTS("advanced.metadata.schema.debouncer.max-events"),
  METADATA_TOKEN_MAP_ENABLED("advanced.metadata.token-map.enabled"),

  CONTROL_CONNECTION_TIMEOUT("advanced.control-connection.timeout"),
  CONTROL_CONNECTION_AGREEMENT_INTERVAL("advanced.control-connection.schema-agreement.interval"),
  CONTROL_CONNECTION_AGREEMENT_TIMEOUT("advanced.control-connection.schema-agreement.timeout"),
  CONTROL_CONNECTION_AGREEMENT_WARN("advanced.control-connection.schema-agreement.warn-on-failure"),

  PREPARE_ON_ALL_NODES("advanced.prepared-statements.prepare-on-all-nodes"),
  REPREPARE_ENABLED("advanced.prepared-statements.reprepare-on-up.enabled"),
  REPREPARE_CHECK_SYSTEM_TABLE("advanced.prepared-statements.reprepare-on-up.check-system-table"),
  REPREPARE_MAX_STATEMENTS("advanced.prepared-statements.reprepare-on-up.max-statements"),
  REPREPARE_MAX_PARALLELISM("advanced.prepared-statements.reprepare-on-up.max-parallelism"),
  REPREPARE_TIMEOUT("advanced.prepared-statements.reprepare-on-up.timeout"),

  NETTY_IO_SIZE("advanced.netty.io-group.size"),
  NETTY_IO_SHUTDOWN_QUIET_PERIOD("advanced.netty.io-group.shutdown.quiet-period"),
  NETTY_IO_SHUTDOWN_TIMEOUT("advanced.netty.io-group.shutdown.timeout"),
  NETTY_IO_SHUTDOWN_UNIT("advanced.netty.io-group.shutdown.unit"),
  NETTY_ADMIN_SIZE("advanced.netty.admin-group.size"),
  NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD("advanced.netty.admin-group.shutdown.quiet-period"),
  NETTY_ADMIN_SHUTDOWN_TIMEOUT("advanced.netty.admin-group.shutdown.timeout"),
  NETTY_ADMIN_SHUTDOWN_UNIT("advanced.netty.admin-group.shutdown.unit"),

  COALESCER_MAX_RUNS("advanced.coalescer.max-runs-with-no-work"),
  COALESCER_INTERVAL("advanced.coalescer.reschedule-interval"),

  RESOLVE_CONTACT_POINTS("advanced.resolve-contact-points"),

  NETTY_TIMER_TICK_DURATION("advanced.netty.timer.tick-duration"),
  NETTY_TIMER_TICKS_PER_WHEEL("advanced.netty.timer.ticks-per-wheel"),

  REQUEST_LOG_WARNINGS("advanced.request.log-warnings"),
  ;

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
