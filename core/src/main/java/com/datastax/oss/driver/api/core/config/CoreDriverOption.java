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

public enum CoreDriverOption implements DriverOption {
  PROTOCOL_VERSION("protocol.version", false),

  CONNECTION_INIT_QUERY_TIMEOUT("connection.init-query-timeout", true),
  CONNECTION_SET_KEYSPACE_TIMEOUT("connection.set-keyspace-timeout", true),
  CONNECTION_MAX_FRAME_LENGTH("connection.max-frame-length", true),
  CONNECTION_MAX_REQUESTS("connection.max-requests-per-connection", true),
  CONNECTION_HEARTBEAT_INTERVAL("connection.heartbeat.interval", true),
  CONNECTION_HEARTBEAT_TIMEOUT("connection.heartbeat.interval", true),

  AUTHENTICATION_PROVIDER_CLASS("authentication.provider-class", false),
  AUTHENTICATION_CONFIG_USERNAME("authentication.config.username", false),
  AUTHENTICATION_CONFIG_PASSWORD("authentication.config.password", false),

  SSL_FACTORY_CLASS("ssl.factory-class", false),
  SSL_CONFIG_CIPHER_SUITES("ssl.config.cipher-suites", false),
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
