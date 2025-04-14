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
package com.datastax.oss.driver.internal.core.context;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.ConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class MockedDriverContextFactory {

  public static DefaultDriverContext defaultDriverContext() {
    return defaultDriverContext(MockedDriverContextFactory.defaultProfile("datacenter1"));
  }

  public static DefaultDriverContext defaultDriverContext(
      DriverExecutionProfile defaultProfile, DriverExecutionProfile... profiles) {

    /* Setup machinery to connect the input DriverExecutionProfile to the config loader */
    final DriverConfig driverConfig = mock(DriverConfig.class);
    final DriverConfigLoader configLoader = mock(DriverConfigLoader.class);
    when(configLoader.getInitialConfig()).thenReturn(driverConfig);
    when(driverConfig.getDefaultProfile()).thenReturn(defaultProfile);
    when(driverConfig.getProfile(defaultProfile.getName())).thenReturn(defaultProfile);

    for (DriverExecutionProfile profile : profiles) {
      when(driverConfig.getProfile(profile.getName())).thenReturn(profile);
    }

    ProgrammaticArguments args =
        ProgrammaticArguments.builder()
            .withNodeStateListener(mock(NodeStateListener.class))
            .withSchemaChangeListener(mock(SchemaChangeListener.class))
            .withRequestTracker(mock(RequestTracker.class))
            .withLocalDatacenters(Maps.newHashMap())
            .withNodeDistanceEvaluators(Maps.newHashMap())
            .build();

    return new DefaultDriverContext(configLoader, args) {
      @NonNull
      @Override
      public Map<String, LoadBalancingPolicy> getLoadBalancingPolicies() {
        ImmutableMap.Builder<String, LoadBalancingPolicy> map = ImmutableMap.builder();
        map.put(
            defaultProfile.getName(),
            mockLoadBalancingPolicy(
                this,
                defaultProfile.getName(),
                defaultProfile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER)));
        for (DriverExecutionProfile profile : profiles) {
          map.put(
              profile.getName(),
              mockLoadBalancingPolicy(
                  this,
                  profile.getName(),
                  profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER)));
        }
        return map.build();
      }

      @NonNull
      @Override
      public ConsistencyLevelRegistry getConsistencyLevelRegistry() {
        return mock(ConsistencyLevelRegistry.class);
      }
    };
  }

  public static DriverExecutionProfile defaultProfile(String localDc) {
    return createProfile(DriverExecutionProfile.DEFAULT_NAME, localDc);
  }

  public static DriverExecutionProfile createProfile(String name, String localDc) {
    DriverExecutionProfile defaultProfile = mock(DriverExecutionProfile.class);
    when(defaultProfile.getName()).thenReturn(name);
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn("none");
    when(defaultProfile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(Duration.ofMinutes(5));
    when(defaultProfile.isDefined(DefaultDriverOption.METRICS_FACTORY_CLASS)).thenReturn(true);
    when(defaultProfile.getString(DefaultDriverOption.METRICS_FACTORY_CLASS))
        .thenReturn("DefaultMetricsFactory");
    when(defaultProfile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(localDc);
    return defaultProfile;
  }

  public static void allowRemoteDcConnectivity(
      DriverExecutionProfile profile,
      int maxNodesPerRemoteDc,
      boolean allowRemoteSatisfyLocalDc,
      List<String> preferredRemoteDcs) {
    when(profile.getInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC))
        .thenReturn(maxNodesPerRemoteDc);
    when(profile.getBoolean(
            DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS))
        .thenReturn(allowRemoteSatisfyLocalDc);
    when(profile.getStringList(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_PREFERRED_REMOTE_DCS))
        .thenReturn(preferredRemoteDcs);
  }

  private static LoadBalancingPolicy mockLoadBalancingPolicy(
      DefaultDriverContext driverContext, String profile, String localDc) {
    LoadBalancingPolicy loadBalancingPolicy =
        new DefaultLoadBalancingPolicy(driverContext, profile) {
          @NonNull
          @Override
          protected Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
            return Optional.ofNullable(localDc);
          }

          @NonNull
          @Override
          protected NodeDistanceEvaluator createNodeDistanceEvaluator(
              @Nullable String localDc, @NonNull Map<UUID, Node> nodes) {
            return mock(NodeDistanceEvaluator.class);
          }
        };
    loadBalancingPolicy.init(
        Collections.emptyMap(), mock(LoadBalancingPolicy.DistanceReporter.class));
    return loadBalancingPolicy;
  }
}
