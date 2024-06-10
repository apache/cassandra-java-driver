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
package com.datastax.dse.driver.internal.core.insights;

import static com.datastax.dse.driver.api.core.config.DseDriverOption.GRAPH_TRAVERSAL_SOURCE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.HEARTBEAT_INTERVAL;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_DISTANCE_EVALUATOR_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.PROTOCOL_COMPRESSION;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.RECONNECTION_BASE_DELAY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_CONSISTENCY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SPECULATIVE_EXECUTION_MAX;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import java.time.Duration;

class ExecutionProfileMockUtil {
  static final String DEFAULT_LOCAL_DC = "local-dc";
  static final int SPECEX_MAX_DEFAULT = 100;
  static final int SPECEX_DELAY_DEFAULT = 20;

  static DriverExecutionProfile mockDefaultExecutionProfile() {
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);

    when(profile.getDuration(REQUEST_TIMEOUT)).thenReturn(Duration.ofMillis(100));
    when(profile.getString(LOAD_BALANCING_POLICY_CLASS)).thenReturn("LoadBalancingPolicyImpl");
    when(profile.isDefined(LOAD_BALANCING_DISTANCE_EVALUATOR_CLASS)).thenReturn(true);
    when(profile.isDefined(LOAD_BALANCING_LOCAL_DATACENTER)).thenReturn(true);
    when(profile.getString(LOAD_BALANCING_LOCAL_DATACENTER)).thenReturn(DEFAULT_LOCAL_DC);
    when(profile.isDefined(SPECULATIVE_EXECUTION_MAX)).thenReturn(true);
    when(profile.getInt(SPECULATIVE_EXECUTION_MAX)).thenReturn(SPECEX_MAX_DEFAULT);
    when(profile.isDefined(SPECULATIVE_EXECUTION_DELAY)).thenReturn(true);
    when(profile.getInt(SPECULATIVE_EXECUTION_DELAY)).thenReturn(SPECEX_DELAY_DEFAULT);
    when(profile.getString(SPECULATIVE_EXECUTION_POLICY_CLASS))
        .thenReturn("SpeculativeExecutionImpl");
    when(profile.getString(REQUEST_CONSISTENCY)).thenReturn("LOCAL_ONE");
    when(profile.getString(REQUEST_SERIAL_CONSISTENCY)).thenReturn("SERIAL");
    when(profile.getInt(CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);
    when(profile.getInt(CONNECTION_POOL_REMOTE_SIZE)).thenReturn(1);
    when(profile.getString(eq(PROTOCOL_COMPRESSION), any())).thenReturn("none");
    when(profile.getDuration(HEARTBEAT_INTERVAL)).thenReturn(Duration.ofMillis(100));
    when(profile.getDuration(RECONNECTION_BASE_DELAY)).thenReturn(Duration.ofMillis(100));
    when(profile.isDefined(SSL_ENGINE_FACTORY_CLASS)).thenReturn(true);
    when(profile.getString(eq(AUTH_PROVIDER_CLASS), any())).thenReturn("AuthProviderImpl");
    when(profile.getString(GRAPH_TRAVERSAL_SOURCE, null)).thenReturn("src-graph");
    return profile;
  }

  static DriverExecutionProfile mockNonDefaultRequestTimeoutExecutionProfile() {
    DriverExecutionProfile profile = mockDefaultExecutionProfile();
    when(profile.getDuration(REQUEST_TIMEOUT)).thenReturn(Duration.ofMillis(50));
    return profile;
  }

  static DriverExecutionProfile mockNonDefaultLoadBalancingExecutionProfile() {
    DriverExecutionProfile profile = mockDefaultExecutionProfile();
    when(profile.getString(LOAD_BALANCING_POLICY_CLASS)).thenReturn("NonDefaultLoadBalancing");
    return profile;
  }

  static DriverExecutionProfile mockUndefinedLocalDcExecutionProfile() {
    DriverExecutionProfile profile = mockNonDefaultLoadBalancingExecutionProfile();
    when(profile.isDefined(LOAD_BALANCING_LOCAL_DATACENTER)).thenReturn(false);
    return profile;
  }

  static DriverExecutionProfile mockNonDefaultSpeculativeExecutionInfo() {
    DriverExecutionProfile profile = mockDefaultExecutionProfile();
    when(profile.getString(SPECULATIVE_EXECUTION_POLICY_CLASS))
        .thenReturn("NonDefaultSpecexPolicy");
    return profile;
  }

  static DriverExecutionProfile mockNonDefaultConsistency() {
    DriverExecutionProfile profile = mockDefaultExecutionProfile();
    when(profile.getString(REQUEST_CONSISTENCY)).thenReturn("ALL");
    return profile;
  }

  static DriverExecutionProfile mockNonDefaultSerialConsistency() {
    DriverExecutionProfile profile = mockDefaultExecutionProfile();
    when(profile.getString(REQUEST_SERIAL_CONSISTENCY)).thenReturn("ONE");
    return profile;
  }

  static DriverExecutionProfile mockNonDefaultGraphOptions() {
    DriverExecutionProfile profile = mockDefaultExecutionProfile();
    when(profile.getString(GRAPH_TRAVERSAL_SOURCE, null)).thenReturn("non-default-graph");
    return profile;
  }
}
