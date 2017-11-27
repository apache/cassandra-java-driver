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
public enum CoreDriverOption implements DriverOption {
  CONTACT_POINTS("contact-points", false),

  PROTOCOL_VERSION("protocol.version", false),
  PROTOCOL_MAX_FRAME_LENGTH("protocol.max-frame-length", true),
  PROTOCOL_COMPRESSOR_CLASS("protocol.compressor.class", false),

  CLUSTER_NAME("cluster-name", false),
  CONFIG_RELOAD_INTERVAL("config-reload-interval", false),

  CONNECTION_INIT_QUERY_TIMEOUT("connection.init-query-timeout", true),
  CONNECTION_SET_KEYSPACE_TIMEOUT("connection.set-keyspace-timeout", true),
  CONNECTION_MAX_REQUESTS("connection.max-requests-per-connection", true),
  CONNECTION_HEARTBEAT_INTERVAL("connection.heartbeat.interval", true),
  CONNECTION_HEARTBEAT_TIMEOUT("connection.heartbeat.timeout", true),
  CONNECTION_MAX_ORPHAN_REQUESTS("connection.max-orphan-requests", true),
  CONNECTION_POOL_LOCAL_SIZE("connection.pool.local.size", true),
  CONNECTION_POOL_REMOTE_SIZE("connection.pool.remote.size", true),

  REQUEST_TIMEOUT("request.timeout", true),
  REQUEST_CONSISTENCY("request.consistency", true),
  REQUEST_PAGE_SIZE("request.page-size", true),
  REQUEST_SERIAL_CONSISTENCY("request.serial-consistency", true),
  REQUEST_WARN_IF_SET_KEYSPACE("request.warn-if-set-keyspace", true),
  REQUEST_DEFAULT_IDEMPOTENCE("request.default-idempotence", true),
  RETRY_POLICY_CLASS("request.retry-policy.class", true),
  SPECULATIVE_EXECUTION_POLICY_CLASS("request.speculative-execution-policy.class", true),
  SPECULATIVE_EXECUTION_MAX("request.speculative-execution-policy.max-executions", false),
  SPECULATIVE_EXECUTION_DELAY("request.speculative-execution-policy.delay", false),
  REQUEST_TRACE_ATTEMPTS("request.trace.attempts", true),
  REQUEST_TRACE_INTERVAL("request.trace.interval", true),
  REQUEST_TRACE_CONSISTENCY("request.trace.consistency", true),

  CONTROL_CONNECTION_TIMEOUT("connection.control-connection.timeout", true),
  CONTROL_CONNECTION_AGREEMENT_INTERVAL(
      "connection.control-connection.schema-agreement.interval", true),
  CONTROL_CONNECTION_AGREEMENT_TIMEOUT(
      "connection.control-connection.schema-agreement.timeout", true),
  CONTROL_CONNECTION_AGREEMENT_WARN(
      "connection.control-connection.schema-agreement.warn-on-failure", true),

  COALESCER_CLASS("connection.coalescer.class", true),
  COALESCER_MAX_RUNS("connection.coalescer.max-runs-with-no-work", false),
  COALESCER_INTERVAL("connection.coalescer.reschedule-interval", false),

  LOAD_BALANCING_POLICY_CLASS("load-balancing-policy.class", true),
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
  AUTH_PROVIDER_WARN_IF_NO_SERVER_AUTH("protocol.auth-provider.warn-if-no-server-auth", false),

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

  TIMESTAMP_GENERATOR_CLASS("request.timestamp-generator.class", true),
  TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK("request.timestamp-generator.force-java-clock", false),
  TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD(
      "request.timestamp-generator.drift-warning.threshold", false),
  TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL(
      "request.timestamp-generator.drift-warning.interval", false),

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

  CoreDriverOption(String path, boolean required) {
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
