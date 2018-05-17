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

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import java.util.Map;
import java.util.Optional;

/** Holds common components that are shared throughout a driver instance. */
public interface DriverContext extends AttachmentPoint {

  /**
   * This is the same as {@link Session#getName()}, it's exposed here for components that only have
   * a reference to the context.
   */
  String sessionName();

  DriverConfig config();

  Map<String, LoadBalancingPolicy> loadBalancingPolicies();

  default LoadBalancingPolicy loadBalancingPolicy(String profileName) {
    LoadBalancingPolicy policy = loadBalancingPolicies().get(profileName);
    // Protect against a non-existent name
    return (policy != null)
        ? policy
        : loadBalancingPolicies().get(DriverConfigProfile.DEFAULT_NAME);
  }

  Map<String, RetryPolicy> retryPolicies();

  default RetryPolicy retryPolicy(String profileName) {
    RetryPolicy policy = retryPolicies().get(profileName);
    return (policy != null) ? policy : retryPolicies().get(DriverConfigProfile.DEFAULT_NAME);
  }

  Map<String, SpeculativeExecutionPolicy> speculativeExecutionPolicies();

  default SpeculativeExecutionPolicy speculativeExecutionPolicy(String profileName) {
    SpeculativeExecutionPolicy policy = speculativeExecutionPolicies().get(profileName);
    return (policy != null)
        ? policy
        : speculativeExecutionPolicies().get(DriverConfigProfile.DEFAULT_NAME);
  }

  TimestampGenerator timestampGenerator();

  ReconnectionPolicy reconnectionPolicy();

  AddressTranslator addressTranslator();

  /** The authentication provider, if authentication was configured. */
  Optional<AuthProvider> authProvider();

  /** The SSL engine factory, if SSL was configured. */
  Optional<SslEngineFactory> sslEngineFactory();
}
