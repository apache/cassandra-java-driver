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
package com.datastax.dse.driver.internal.core.insights;

import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.DEFAULT_LOCAL_DC;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.SPECEX_DELAY_DEFAULT;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.SPECEX_MAX_DEFAULT;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.mockDefaultExecutionProfile;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.mockNonDefaultConsistency;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.mockNonDefaultGraphOptions;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.mockNonDefaultLoadBalancingExecutionProfile;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.mockNonDefaultRequestTimeoutExecutionProfile;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.mockNonDefaultSerialConsistency;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.mockNonDefaultSpeculativeExecutionInfo;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.mockUndefinedLocalDcExecutionProfile;
import static com.datastax.dse.driver.internal.core.insights.PackageUtil.DEFAULT_LOAD_BALANCING_PACKAGE;
import static com.datastax.dse.driver.internal.core.insights.PackageUtil.DEFAULT_SPECULATIVE_EXECUTION_PACKAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.internal.core.insights.schema.LoadBalancingInfo;
import com.datastax.dse.driver.internal.core.insights.schema.SpecificExecutionProfile;
import com.datastax.dse.driver.internal.core.insights.schema.SpeculativeExecutionInfo;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(DataProviderRunner.class)
public class ExecutionProfilesInfoFinderTest {

  @Test
  public void should_include_info_about_default_profile() {
    // given
    DriverExecutionProfile defaultExecutionProfile = mockDefaultExecutionProfile();
    Map<String, DriverExecutionProfile> profiles =
        ImmutableMap.of("default", defaultExecutionProfile);

    InternalDriverContext context =
        mockDriverContextWithProfiles(defaultExecutionProfile, profiles);

    // when
    Map<String, SpecificExecutionProfile> executionProfilesInfo =
        new ExecutionProfilesInfoFinder().getExecutionProfilesInfo(context);

    // then
    assertThat(executionProfilesInfo)
        .isEqualTo(
            ImmutableMap.of(
                "default",
                new SpecificExecutionProfile(
                    100,
                    new LoadBalancingInfo(
                        "LoadBalancingPolicyImpl",
                        ImmutableMap.of("localDataCenter", "local-dc", "filterFunction", true),
                        DEFAULT_LOAD_BALANCING_PACKAGE),
                    new SpeculativeExecutionInfo(
                        "SpeculativeExecutionImpl",
                        ImmutableMap.of("maxSpeculativeExecutions", 100, "delay", 20),
                        DEFAULT_SPECULATIVE_EXECUTION_PACKAGE),
                    "LOCAL_ONE",
                    "SERIAL",
                    ImmutableMap.of("source", "src-graph"))));
  }

  @Test
  @UseDataProvider("executionProfileProvider")
  public void should_include_info_about_default_profile_and_only_difference_for_specific_profile(
      DriverExecutionProfile nonDefaultExecutionProfile, SpecificExecutionProfile expected) {
    // given

    DriverExecutionProfile defaultExecutionProfile = mockDefaultExecutionProfile();
    Map<String, DriverExecutionProfile> profiles =
        ImmutableMap.of(
            "default", defaultExecutionProfile, "non-default", nonDefaultExecutionProfile);
    InternalDriverContext context =
        mockDriverContextWithProfiles(defaultExecutionProfile, profiles);
    // when
    Map<String, SpecificExecutionProfile> executionProfilesInfo =
        new ExecutionProfilesInfoFinder().getExecutionProfilesInfo(context);

    // then
    assertThat(executionProfilesInfo)
        .isEqualTo(
            ImmutableMap.of(
                "default",
                new SpecificExecutionProfile(
                    100,
                    new LoadBalancingInfo(
                        "LoadBalancingPolicyImpl",
                        ImmutableMap.of("localDataCenter", "local-dc", "filterFunction", true),
                        DEFAULT_LOAD_BALANCING_PACKAGE),
                    new SpeculativeExecutionInfo(
                        "SpeculativeExecutionImpl",
                        ImmutableMap.of("maxSpeculativeExecutions", 100, "delay", 20),
                        DEFAULT_SPECULATIVE_EXECUTION_PACKAGE),
                    "LOCAL_ONE",
                    "SERIAL",
                    ImmutableMap.of("source", "src-graph")),
                "non-default",
                expected));
  }

  @DataProvider
  public static Object[][] executionProfileProvider() {
    return new Object[][] {
      {
        mockNonDefaultRequestTimeoutExecutionProfile(),
        new SpecificExecutionProfile(50, null, null, null, null, null)
      },
      {
        mockNonDefaultLoadBalancingExecutionProfile(),
        new SpecificExecutionProfile(
            null,
            new LoadBalancingInfo(
                "NonDefaultLoadBalancing",
                ImmutableMap.of("localDataCenter", DEFAULT_LOCAL_DC, "filterFunction", true),
                DEFAULT_LOAD_BALANCING_PACKAGE),
            null,
            null,
            null,
            null)
      },
      {
        mockUndefinedLocalDcExecutionProfile(),
        new SpecificExecutionProfile(
            null,
            new LoadBalancingInfo(
                "NonDefaultLoadBalancing",
                ImmutableMap.of("filterFunction", true),
                DEFAULT_LOAD_BALANCING_PACKAGE),
            null,
            null,
            null,
            null)
      },
      {
        mockNonDefaultSpeculativeExecutionInfo(),
        new SpecificExecutionProfile(
            null,
            null,
            new SpeculativeExecutionInfo(
                "NonDefaultSpecexPolicy",
                ImmutableMap.of(
                    "maxSpeculativeExecutions", SPECEX_MAX_DEFAULT, "delay", SPECEX_DELAY_DEFAULT),
                DEFAULT_SPECULATIVE_EXECUTION_PACKAGE),
            null,
            null,
            null)
      },
      {
        mockNonDefaultConsistency(),
        new SpecificExecutionProfile(null, null, null, "ALL", null, null)
      },
      {
        mockNonDefaultSerialConsistency(),
        new SpecificExecutionProfile(null, null, null, null, "ONE", null)
      },
      {
        mockNonDefaultGraphOptions(),
        new SpecificExecutionProfile(
            null, null, null, null, null, ImmutableMap.of("source", "non-default-graph"))
      },
      {
        mockDefaultExecutionProfile(),
        new SpecificExecutionProfile(null, null, null, null, null, null)
      }
    };
  }

  @Test
  public void should_not_include_null_fields_in_json() throws JsonProcessingException {
    // given
    SpecificExecutionProfile specificExecutionProfile =
        new SpecificExecutionProfile(50, null, null, "ONE", null, ImmutableMap.of("a", "b"));

    // when
    String result = new ObjectMapper().writeValueAsString(specificExecutionProfile);

    // then
    assertThat(result)
        .isEqualTo("{\"readTimeout\":50,\"consistency\":\"ONE\",\"graphOptions\":{\"a\":\"b\"}}");
  }

  @Test
  public void should_include_empty_execution_profile_if_has_all_nulls()
      throws JsonProcessingException {
    // given
    Map<String, SpecificExecutionProfile> executionProfiles =
        ImmutableMap.of("p", new SpecificExecutionProfile(null, null, null, null, null, null));

    // when
    String result = new ObjectMapper().writeValueAsString(executionProfiles);

    // then
    assertThat(result).isEqualTo("{\"p\":{}}");
  }

  private InternalDriverContext mockDriverContextWithProfiles(
      DriverExecutionProfile defaultExecutionProfile,
      Map<String, DriverExecutionProfile> profiles) {
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverConfig driverConfig = mock(DriverConfig.class);
    Mockito.<Map<String, ? extends DriverExecutionProfile>>when(driverConfig.getProfiles())
        .thenReturn(profiles);
    when(driverConfig.getDefaultProfile()).thenReturn(defaultExecutionProfile);
    when(context.getConfig()).thenReturn(driverConfig);
    return context;
  }
}
