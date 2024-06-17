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
package com.datastax.oss.driver.internal.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(DataProviderRunner.class)
public class DefaultMetricIdGeneratorTest {

  @Mock private InternalDriverContext context;

  @Mock private DriverConfig config;

  @Mock private DriverExecutionProfile profile;

  @Mock private Node node;

  @Mock private EndPoint endpoint;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    given(context.getConfig()).willReturn(config);
    given(context.getSessionName()).willReturn("s0");
    given(config.getDefaultProfile()).willReturn(profile);
    given(node.getEndPoint()).willReturn(endpoint);
    given(endpoint.asMetricPrefix()).willReturn("10_1_2_3:9042");
  }

  @Test
  @UseDataProvider("sessionMetrics")
  public void should_generate_session_metric(String prefix, String expectedName) {
    // given
    given(profile.getString(DefaultDriverOption.METRICS_ID_GENERATOR_PREFIX, ""))
        .willReturn(prefix);
    DefaultMetricIdGenerator generator = new DefaultMetricIdGenerator(context);
    // when
    MetricId id = generator.sessionMetricId(DefaultSessionMetric.CONNECTED_NODES);
    // then
    assertThat(id.getName()).isEqualTo(expectedName);
    assertThat(id.getTags()).isEmpty();
  }

  @Test
  @UseDataProvider("nodeMetrics")
  public void should_generate_node_metric(String prefix, String expectedName) {
    // given
    given(profile.getString(DefaultDriverOption.METRICS_ID_GENERATOR_PREFIX, ""))
        .willReturn(prefix);
    DefaultMetricIdGenerator generator = new DefaultMetricIdGenerator(context);
    // when
    MetricId id = generator.nodeMetricId(node, DefaultNodeMetric.CQL_MESSAGES);
    // then
    assertThat(id.getName()).isEqualTo(expectedName);
    assertThat(id.getTags()).isEmpty();
  }

  @DataProvider
  public static Object[][] sessionMetrics() {
    String suffix = DefaultSessionMetric.CONNECTED_NODES.getPath();
    return new Object[][] {
      new Object[] {"", "s0." + suffix},
      new Object[] {"cassandra", "cassandra.s0." + suffix},
      new Object[] {"app.cassandra", "app.cassandra.s0." + suffix}
    };
  }

  @DataProvider
  public static Object[][] nodeMetrics() {
    String suffix = DefaultNodeMetric.CQL_MESSAGES.getPath();
    return new Object[][] {
      new Object[] {"", "s0.nodes.10_1_2_3:9042." + suffix},
      new Object[] {"cassandra", "cassandra.s0.nodes.10_1_2_3:9042." + suffix},
      new Object[] {"app.cassandra", "app.cassandra.s0.nodes.10_1_2_3:9042." + suffix}
    };
  }
}
