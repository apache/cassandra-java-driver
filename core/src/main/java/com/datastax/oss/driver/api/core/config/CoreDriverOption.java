/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
  RETRY_POLICY_ROOT("request.retry-policy", true),
  SPECULATIVE_EXECUTION_POLICY_ROOT("request.speculative-execution-policy", true),

  CONTROL_CONNECTION_TIMEOUT("connection.control-connection.timeout", true),
  CONTROL_CONNECTION_PAGE_SIZE("connection.control-connection.page-size", true),

  COALESCER_ROOT("connection.coalescer", true),
  RELATIVE_COALESCER_MAX_RUNS("max-runs-with-no-work", false),
  RELATIVE_COALESCER_INTERVAL("reschedule-interval", false),

  // "Sub-option" for all the policies, etc.
  RELATIVE_POLICY_CLASS("class", false),

  LOAD_BALANCING_POLICY_ROOT("load-balancing-policy", true),

  RECONNECTION_POLICY_ROOT("connection.reconnection-policy", true),
  RELATIVE_EXPONENTIAL_RECONNECTION_BASE_DELAY("base-delay", false),
  RELATIVE_EXPONENTIAL_RECONNECTION_MAX_DELAY("max-delay", false),

  PREPARE_ON_ALL_NODES("prepared-statements.prepare-on-all-nodes", true),
  REPREPARE_ENABLED("prepared-statements.reprepare-on-up.enabled", true),
  REPREPARE_CHECK_SYSTEM_TABLE("prepared-statements.reprepare-on-up.check-system-table", false),
  REPREPARE_MAX_STATEMENTS("prepared-statements.reprepare-on-up.max-statements", false),
  REPREPARE_MAX_PARALLELISM("prepared-statements.reprepare-on-up.max-parallelism", false),
  REPREPARE_TIMEOUT("prepared-statements.reprepare-on-up.timeout", false),

  ADDRESS_TRANSLATOR_ROOT("address-translator", true),

  AUTH_PROVIDER_ROOT("protocol.auth-provider", false),
  RELATIVE_PLAIN_TEXT_AUTH_USERNAME("username", false),
  RELATIVE_PLAIN_TEXT_AUTH_PASSWORD("password", false),

  SSL_ENGINE_FACTORY_ROOT("ssl-engine-factory", false),
  RELATIVE_DEFAULT_SSL_CIPHER_SUITES("cipher-suites", false),

  METADATA_TOPOLOGY_WINDOW("metadata.topology-event-debouncer.window", true),
  METADATA_TOPOLOGY_MAX_EVENTS("metadata.topology-event-debouncer.max-events", true),

  TIMESTAMP_GENERATOR_ROOT("request.timestamp-generator", true),
  RELATIVE_TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK("force-java-clock", false),
  RELATIVE_TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD("drift-warning.threshold", false),
  RELATIVE_TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL("drift-warning.interval", false),

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
