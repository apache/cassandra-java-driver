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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Collections;
import org.junit.Test;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;

/**
 * Covers {@link CassandraProtocolVersionRegistry#highestCommon(Collection)} separately, because it
 * relies explicitly on {@link CoreProtocolVersion} as the version implementation.
 */
public class CassandraProtocolVersionRegistryHighestCommonTest {

  private CassandraProtocolVersionRegistry registry = new CassandraProtocolVersionRegistry("test");

  @Test
  public void should_pick_v3_when_at_least_one_node_is_2_1() {
    assertThat(
            registry.highestCommon(
                ImmutableList.of(mockNode("2.2.1"), mockNode("2.1.0"), mockNode("3.1.9"))))
        .isEqualTo(CoreProtocolVersion.V3);
  }

  @Test
  public void should_pick_v4_when_all_nodes_are_2_2_or_more() {
    assertThat(
            registry.highestCommon(
                ImmutableList.of(mockNode("2.2.0"), mockNode("2.2.1"), mockNode("3.1.9"))))
        .isEqualTo(CoreProtocolVersion.V4);
  }

  @Test
  public void should_treat_rcs_as_next_stable_versions() {
    assertThat(
            registry.highestCommon(
                ImmutableList.of(mockNode("2.2.1"), mockNode("2.1.0-rc1"), mockNode("3.1.9"))))
        .isEqualTo(CoreProtocolVersion.V3);
    assertThat(
            registry.highestCommon(
                ImmutableList.of(mockNode("2.2.0-rc2"), mockNode("2.2.1"), mockNode("3.1.9"))))
        .isEqualTo(CoreProtocolVersion.V4);
  }

  @Test
  public void should_skip_nodes_that_report_null_version() {
    assertThat(
            registry.highestCommon(
                ImmutableList.of(mockNode(null), mockNode("2.1.0"), mockNode("3.1.9"))))
        .isEqualTo(CoreProtocolVersion.V3);

    // Edge case: if all do, go with the latest version
    assertThat(
            registry.highestCommon(
                ImmutableList.of(mockNode(null), mockNode(null), mockNode(null))))
        .isEqualTo(CoreProtocolVersion.V4);
  }

  @Test
  public void should_use_v4_for_future_cassandra_versions() {
    // That might change in the future when some C* versions drop v4 support
    assertThat(
            registry.highestCommon(
                ImmutableList.of(mockNode("3.0.0"), mockNode("12.1.5"), mockNode("98.7.22"))))
        .isEqualTo(CoreProtocolVersion.V4);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_if_no_nodes() {
    registry.highestCommon(Collections.emptyList());
  }

  private Node mockNode(String cassandraVersion) {
    Node node = Mockito.mock(Node.class);
    if (cassandraVersion != null) {
      Mockito.when(node.getCassandraVersion()).thenReturn(CassandraVersion.parse(cassandraVersion));
    }
    return node;
  }

  @Test(expected = UnsupportedProtocolVersionException.class)
  public void should_fail_if_pre_2_1_node() {
    registry.highestCommon(ImmutableList.of(mockNode("3.0.0"), mockNode("2.0.9")));
  }
}
