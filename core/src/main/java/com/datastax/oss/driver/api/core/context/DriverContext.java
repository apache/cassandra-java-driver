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
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;

/** Holds common components that are shared throughout a driver instance. */
public interface DriverContext extends AttachmentPoint {

  /**
   * This is the same as {@link Session#getName()}, it's exposed here for components that only have
   * a reference to the context.
   */
  @NonNull
  String getSessionName();

  /** @return The driver's configuration; never {@code null}. */
  @NonNull
  DriverConfig getConfig();

  /** @return The driver's configuration loader; never {@code null}. */
  @NonNull
  DriverConfigLoader getConfigLoader();

  /**
   * @return The driver's load balancing policies, keyed by profile name; the returned map is
   *     guaranteed to never be {@code null} and to always contain an entry for the {@value
   *     DriverExecutionProfile#DEFAULT_NAME} profile.
   */
  @NonNull
  Map<String, LoadBalancingPolicy> getLoadBalancingPolicies();

  /**
   * @param profileName the profile name; never {@code null}.
   * @return The driver's load balancing policy for the given profile; never {@code null}.
   */
  @NonNull
  default LoadBalancingPolicy getLoadBalancingPolicy(@NonNull String profileName) {
    LoadBalancingPolicy policy = getLoadBalancingPolicies().get(profileName);
    // Protect against a non-existent name
    return (policy != null)
        ? policy
        : getLoadBalancingPolicies().get(DriverExecutionProfile.DEFAULT_NAME);
  }

  /**
   * @return The driver's retry policies, keyed by profile name; the returned map is guaranteed to
   *     never be {@code null} and to always contain an entry for the {@value
   *     DriverExecutionProfile#DEFAULT_NAME} profile.
   */
  @NonNull
  Map<String, RetryPolicy> getRetryPolicies();

  /**
   * @param profileName the profile name; never {@code null}.
   * @return The driver's retry policy for the given profile; never {@code null}.
   */
  @NonNull
  default RetryPolicy getRetryPolicy(@NonNull String profileName) {
    RetryPolicy policy = getRetryPolicies().get(profileName);
    return (policy != null) ? policy : getRetryPolicies().get(DriverExecutionProfile.DEFAULT_NAME);
  }

  /**
   * @return The driver's speculative execution policies, keyed by profile name; the returned map is
   *     guaranteed to never be {@code null} and to always contain an entry for the {@value
   *     DriverExecutionProfile#DEFAULT_NAME} profile.
   */
  @NonNull
  Map<String, SpeculativeExecutionPolicy> getSpeculativeExecutionPolicies();

  /**
   * @param profileName the profile name; never {@code null}.
   * @return The driver's speculative execution policy for the given profile; never {@code null}.
   */
  @NonNull
  default SpeculativeExecutionPolicy getSpeculativeExecutionPolicy(@NonNull String profileName) {
    SpeculativeExecutionPolicy policy = getSpeculativeExecutionPolicies().get(profileName);
    return (policy != null)
        ? policy
        : getSpeculativeExecutionPolicies().get(DriverExecutionProfile.DEFAULT_NAME);
  }

  /** @return The driver's timestamp generator; never {@code null}. */
  @NonNull
  TimestampGenerator getTimestampGenerator();

  /** @return The driver's reconnection policy; never {@code null}. */
  @NonNull
  ReconnectionPolicy getReconnectionPolicy();

  /** @return The driver's address translator; never {@code null}. */
  @NonNull
  AddressTranslator getAddressTranslator();

  /** @return The authentication provider, if authentication was configured. */
  @NonNull
  Optional<AuthProvider> getAuthProvider();

  /** @return The SSL engine factory, if SSL was configured. */
  @NonNull
  Optional<SslEngineFactory> getSslEngineFactory();

  /** @return The driver's request tracker; never {@code null}. */
  @NonNull
  RequestTracker getRequestTracker();

  /** @return The driver's request throttler; never {@code null}. */
  @NonNull
  RequestThrottler getRequestThrottler();

  /** @return The driver's node state listener; never {@code null}. */
  @NonNull
  NodeStateListener getNodeStateListener();

  /** @return The driver's schema change listener; never {@code null}. */
  @NonNull
  SchemaChangeListener getSchemaChangeListener();
}
