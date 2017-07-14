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
  CLUSTER_NAME("cluster-name", false),
  CONFIG_RELOAD_INTERVAL("config-reload-interval", false),

  CONNECTION_INIT_QUERY_TIMEOUT("connection.init-query-timeout", true),
  CONNECTION_SET_KEYSPACE_TIMEOUT("connection.set-keyspace-timeout", true),
  CONNECTION_MAX_FRAME_LENGTH("connection.max-frame-length", true),
  CONNECTION_MAX_REQUESTS("connection.max-requests-per-connection", true),
  CONNECTION_HEARTBEAT_INTERVAL("connection.heartbeat.interval", true),
  CONNECTION_HEARTBEAT_TIMEOUT("connection.heartbeat.timeout", true),
  CONNECTION_MAX_ORPHAN_REQUESTS("connection.max-orphan-requests", true),

  REQUEST_TIMEOUT("request.timeout", true),
  REQUEST_CONSISTENCY("request.consistency", true),
  REQUEST_PAGE_SIZE("request.page-size", true),
  REQUEST_SERIAL_CONSISTENCY("request.serial-consistency", true),
  REQUEST_WARN_IF_SET_KEYSPACE("request.warn-if-set-keyspace", true),
  REQUEST_DEFAULT_IDEMPOTENCE("request.default-idempotence", true),

  CONTROL_CONNECTION_TIMEOUT("connection.control-connection.timeout", true),
  CONTROL_CONNECTION_PAGE_SIZE("connection.control-connection.page-size", true),

  // "Sub-option" for all the policies, etc.
  RELATIVE_POLICY_CLASS("class", false),

  RETRY_POLICY_ROOT("retry-policy", true),

  LOAD_BALANCING_POLICY_ROOT("load-balancing-policy", true),

  SPECULATIVE_EXECUTION_POLICY_ROOT("speculative-execution-policy", true),

  RECONNECTION_POLICY_ROOT("connection.reconnection-policy", true),
  RELATIVE_EXPONENTIAL_RECONNECTION_BASE_DELAY("base-delay", false),
  RELATIVE_EXPONENTIAL_RECONNECTION_MAX_DELAY("max-delay", false),

  PREPARE_ON_ALL_NODES("prepared-statements.prepare-on-all-nodes", true),
  REPREPARE_ENABLED("prepared-statements.reprepare-on-up.enabled", true),
  REPREPARE_CHECK_SYSTEM_TABLE("prepared-statements.reprepare-on-up.check-system-table", false),
  REPREPARE_MAX_STATEMENTS("prepared-statements.reprepare-on-up.max-statements", false),
  REPREPARE_MAX_PARALLELISM("prepared-statements.reprepare-on-up.max-parallelism", false),
  REPREPARE_TIMEOUT("prepared-statements.reprepare-on-up.timeout", false),

  POOLING_LOCAL_CONNECTIONS("pooling.local.connections", true),
  POOLING_REMOTE_CONNECTIONS("pooling.remote.connections", true),

  ADDRESS_TRANSLATOR_ROOT("address-translator", true),

  AUTH_PROVIDER_ROOT("auth-provider", false),
  RELATIVE_PLAIN_TEXT_AUTH_USERNAME("username", false),
  RELATIVE_PLAIN_TEXT_AUTH_PASSWORD("password", false),

  SSL_ENGINE_FACTORY_ROOT("ssl-engine-factory", false),
  RELATIVE_DEFAULT_SSL_CIPHER_SUITES("cipher-suites", false),

  METADATA_TOPOLOGY_WINDOW("metadata.topology-event-debouncer.window", true),
  METADATA_TOPOLOGY_MAX_EVENTS("metadata.topology-event-debouncer.max-events", true),

  TIMESTAMP_GENERATOR_ROOT("timestamp-generator", true),
  RELATIVE_TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK("force-java-clock", false),
  RELATIVE_TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD("drift-warning.threshold", false),
  RELATIVE_TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL("drift-warning.interval", false),
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
