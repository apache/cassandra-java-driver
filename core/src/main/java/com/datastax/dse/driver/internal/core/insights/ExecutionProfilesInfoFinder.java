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

import com.datastax.dse.driver.internal.core.insights.PackageUtil.ClassSettingDetails;
import com.datastax.dse.driver.internal.core.insights.schema.LoadBalancingInfo;
import com.datastax.dse.driver.internal.core.insights.schema.SpecificExecutionProfile;
import com.datastax.dse.driver.internal.core.insights.schema.SpeculativeExecutionInfo;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class ExecutionProfilesInfoFinder {
  Map<String, SpecificExecutionProfile> getExecutionProfilesInfo(
      InternalDriverContext driverContext) {

    SpecificExecutionProfile defaultProfile =
        mapToSpecificProfile(driverContext.getConfig().getDefaultProfile());

    return driverContext.getConfig().getProfiles().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> {
                  if (isNotDefaultProfile(e)) {
                    SpecificExecutionProfile specificExecutionProfile =
                        mapToSpecificProfile(e.getValue());
                    return retainOnlyDifferentFieldsFromSpecificProfile(
                        defaultProfile, specificExecutionProfile);
                  } else {
                    return defaultProfile;
                  }
                }));
  }

  private boolean isNotDefaultProfile(Map.Entry<String, ? extends DriverExecutionProfile> e) {
    return !e.getKey().equals("default");
  }

  private SpecificExecutionProfile retainOnlyDifferentFieldsFromSpecificProfile(
      SpecificExecutionProfile defaultProfile, SpecificExecutionProfile specificExecutionProfile) {
    Integer readTimeout =
        getIfDifferentOrReturnNull(
            defaultProfile, specificExecutionProfile, SpecificExecutionProfile::getReadTimeout);
    LoadBalancingInfo loadBalancingInfo =
        getIfDifferentOrReturnNull(
            defaultProfile, specificExecutionProfile, SpecificExecutionProfile::getLoadBalancing);

    SpeculativeExecutionInfo speculativeExecutionInfo =
        getIfDifferentOrReturnNull(
            defaultProfile,
            specificExecutionProfile,
            SpecificExecutionProfile::getSpeculativeExecution);

    String consistency =
        getIfDifferentOrReturnNull(
            defaultProfile, specificExecutionProfile, SpecificExecutionProfile::getConsistency);

    String serialConsistency =
        getIfDifferentOrReturnNull(
            defaultProfile,
            specificExecutionProfile,
            SpecificExecutionProfile::getSerialConsistency);

    Map<String, Object> graphOptions =
        getIfDifferentOrReturnNull(
            defaultProfile, specificExecutionProfile, SpecificExecutionProfile::getGraphOptions);

    return new SpecificExecutionProfile(
        readTimeout,
        loadBalancingInfo,
        speculativeExecutionInfo,
        consistency,
        serialConsistency,
        graphOptions);
  }

  private <T> T getIfDifferentOrReturnNull(
      SpecificExecutionProfile defaultProfile,
      SpecificExecutionProfile profile,
      Function<SpecificExecutionProfile, T> valueExtractor) {
    T defaultProfileValue = valueExtractor.apply(defaultProfile);
    T specificProfileValue = valueExtractor.apply(profile);
    if (defaultProfileValue.equals(specificProfileValue)) {
      return null;
    } else {
      return specificProfileValue;
    }
  }

  private SpecificExecutionProfile mapToSpecificProfile(
      DriverExecutionProfile driverExecutionProfile) {
    return new SpecificExecutionProfile(
        (int) driverExecutionProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT).toMillis(),
        getLoadBalancingInfo(driverExecutionProfile),
        getSpeculativeExecutionInfo(driverExecutionProfile),
        driverExecutionProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY),
        driverExecutionProfile.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY),
        getGraphOptions(driverExecutionProfile));
  }

  private SpeculativeExecutionInfo getSpeculativeExecutionInfo(
      DriverExecutionProfile driverExecutionProfile) {
    Map<String, Object> options = new LinkedHashMap<>();

    putIfExists(
        options,
        "maxSpeculativeExecutions",
        DefaultDriverOption.SPECULATIVE_EXECUTION_MAX,
        driverExecutionProfile);
    putIfExists(
        options, "delay", DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY, driverExecutionProfile);

    ClassSettingDetails speculativeExecutionDetails =
        PackageUtil.getSpeculativeExecutionDetails(
            driverExecutionProfile.getString(
                DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS));
    return new SpeculativeExecutionInfo(
        speculativeExecutionDetails.getClassName(),
        options,
        speculativeExecutionDetails.getFullPackage());
  }

  private void putIfExists(
      Map<String, Object> options,
      String key,
      DefaultDriverOption option,
      DriverExecutionProfile executionProfile) {
    if (executionProfile.isDefined(option)) {
      options.put(key, executionProfile.getInt(option));
    }
  }

  private LoadBalancingInfo getLoadBalancingInfo(DriverExecutionProfile driverExecutionProfile) {
    Map<String, Object> options = new LinkedHashMap<>();
    if (driverExecutionProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER)) {
      options.put(
          "localDataCenter",
          driverExecutionProfile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER));
    }
    @SuppressWarnings("deprecation")
    boolean hasNodeFiltering =
        driverExecutionProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS)
            || driverExecutionProfile.isDefined(
                DefaultDriverOption.LOAD_BALANCING_DISTANCE_EVALUATOR_CLASS);
    options.put("filterFunction", hasNodeFiltering);
    ClassSettingDetails loadBalancingDetails =
        PackageUtil.getLoadBalancingDetails(
            driverExecutionProfile.getString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS));
    return new LoadBalancingInfo(
        loadBalancingDetails.getClassName(), options, loadBalancingDetails.getFullPackage());
  }

  private Map<String, Object> getGraphOptions(DriverExecutionProfile driverExecutionProfile) {
    Map<String, Object> graphOptionsMap = new HashMap<>();
    String graphTraversalSource = driverExecutionProfile.getString(GRAPH_TRAVERSAL_SOURCE, null);
    if (graphTraversalSource != null) {
      graphOptionsMap.put("source", graphTraversalSource);
    }
    return graphOptionsMap;
  }
}
