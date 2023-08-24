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
package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class ReplicationStrategyTest {

  @Test(groups = "unit")
  public void createSimpleReplicationStrategyTest() throws Exception {
    ReplicationStrategy strategy =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                .put("class", "SimpleStrategy")
                .put("replication_factor", "3")
                .build());

    assertNotNull(strategy);
    assertTrue(strategy instanceof ReplicationStrategy.SimpleStrategy);
  }

  @Test(groups = "unit")
  public void createNetworkTopologyStrategyTest() throws Exception {
    ReplicationStrategy strategy =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                .put("class", "NetworkTopologyStrategy")
                .put("dc1", "2")
                .put("dc2", "2")
                .build());

    assertNotNull(strategy);
    assertTrue(strategy instanceof ReplicationStrategy.NetworkTopologyStrategy);
  }

  @Test(groups = "unit")
  public void createSimpleReplicationStrategyWithoutFactorTest() throws Exception {
    ReplicationStrategy strategy =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                .put("class", "SimpleStrategy")
                // no replication_factor
                .build());

    assertNull(strategy);
  }

  @Test(groups = "unit")
  public void createUnknownStrategyTest() throws Exception {
    ReplicationStrategy strategy =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                // no such strategy
                .put("class", "FooStrategy")
                .put("foo_factor", "3")
                .build());

    assertNull(strategy);
  }

  @Test(groups = "unit")
  public void createUnspecifiedStrategyTest() throws Exception {
    ReplicationStrategy strategy =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                // nothing useful is set
                .put("foo", "bar")
                .build());

    assertNull(strategy);
  }

  @Test(groups = "unit")
  public void simpleStrategyEqualsTest() {
    ReplicationStrategy rf3_1 =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                .put("class", "SimpleStrategy")
                .put("replication_factor", "3")
                .build());

    ReplicationStrategy rf3_2 =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                .put("class", "SimpleStrategy")
                .put("replication_factor", "3")
                .build());

    ReplicationStrategy rf2_1 =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                .put("class", "SimpleStrategy")
                .put("replication_factor", "2")
                .build());

    //noinspection EqualsWithItself
    assertThat(rf3_1).isEqualTo(rf3_1);
    assertThat(rf3_1).isEqualTo(rf3_2);
    assertThat(rf3_1).isNotEqualTo(rf2_1);
  }

  public void networkTopologyStrategyEqualsTest() {
    ReplicationStrategy network_dc1x2_dc2x2_1 =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                .put("class", "NetworkTopologyStrategy")
                .put("dc1", "2")
                .put("dc2", "2")
                .build());
    ReplicationStrategy network_dc1x2_dc2x2_2 =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                .put("class", "NetworkTopologyStrategy")
                .put("dc1", "2")
                .put("dc2", "2")
                .build());
    ReplicationStrategy network_dc1x1_dc2x2_1 =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                .put("class", "NetworkTopologyStrategy")
                .put("dc1", "1")
                .put("dc2", "2")
                .build());
    ReplicationStrategy network_dc1x2_dc3x2_1 =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                .put("class", "NetworkTopologyStrategy")
                .put("dc1", "2")
                .put("dc3", "2")
                .build());
    ReplicationStrategy network_dc1x2_1 =
        ReplicationStrategy.create(
            ImmutableMap.<String, String>builder()
                .put("class", "NetworkTopologyStrategy")
                .put("dc1", "2")
                .build());

    //noinspection EqualsWithItself
    assertThat(network_dc1x2_dc2x2_1).isEqualTo(network_dc1x2_dc2x2_1);
    assertThat(network_dc1x2_dc2x2_1).isEqualTo(network_dc1x2_dc2x2_2);
    assertThat(network_dc1x2_dc2x2_1).isNotEqualTo(network_dc1x1_dc2x2_1);
    assertThat(network_dc1x2_dc2x2_1).isNotEqualTo(network_dc1x2_dc3x2_1);
    assertThat(network_dc1x2_dc2x2_1).isNotEqualTo(network_dc1x2_1);
  }
}
