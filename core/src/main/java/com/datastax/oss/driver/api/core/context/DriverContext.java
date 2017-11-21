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
package com.datastax.oss.driver.api.core.context;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import java.util.Optional;

/** Holds common components that are shared throughout a driver instance. */
public interface DriverContext extends AttachmentPoint {

  /**
   * This is the same as {@link Cluster#getName()}, it's exposed here for components that only have
   * a reference to the context.
   */
  String clusterName();

  DriverConfig config();

  LoadBalancingPolicy loadBalancingPolicy();

  ReconnectionPolicy reconnectionPolicy();

  RetryPolicy retryPolicy();

  SpeculativeExecutionPolicy speculativeExecutionPolicy();

  AddressTranslator addressTranslator();

  /** The authentication provider, if authentication was configured. */
  Optional<AuthProvider> authProvider();

  /** The SSL engine factory, if SSL was configured. */
  Optional<SslEngineFactory> sslEngineFactory();

  TimestampGenerator timestampGenerator();
}
