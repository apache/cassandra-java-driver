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
package com.datastax.oss.driver.api.core.context;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import java.util.Optional;

/** Holds common components that are shared throughout a driver instance. */
public interface DriverContext {

  /** The driver's configuration. */
  DriverConfig config();

  /** The configured reconnection policy. */
  ReconnectionPolicy reconnectionPolicy();

  /** The authentication provider, if authentication was configured. */
  Optional<AuthProvider> authProvider();

  /** The SSL engine factory, if SSL was configured. */
  Optional<SslEngineFactory> sslEngineFactory();
}
