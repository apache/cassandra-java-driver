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
package com.datastax.oss.driver.internal.core;

import static com.datastax.dse.driver.api.core.DseProtocolVersion.DSE_V1;
import static com.datastax.dse.driver.api.core.DseProtocolVersion.DSE_V2;
import static com.datastax.oss.driver.api.core.ProtocolVersion.V3;
import static com.datastax.oss.driver.api.core.ProtocolVersion.V4;
import static com.datastax.oss.driver.api.core.ProtocolVersion.V5;
import static com.datastax.oss.driver.api.core.ProtocolVersion.V6;
import static com.datastax.oss.driver.internal.core.DefaultProtocolFeature.DATE_TYPE;
import static com.datastax.oss.driver.internal.core.DefaultProtocolFeature.SMALLINT_AND_TINYINT_TYPES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Optional;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Note: some tests in this class depend on the set of supported protocol versions, they will need
 * to be updated as new versions are added or become non-beta.
 */
public class DefaultProtocolVersionRegistryTest {

  private DefaultProtocolVersionRegistry registry = new DefaultProtocolVersionRegistry("test");

  @Test
  public void should_find_version_by_name() {
    assertThat(registry.fromName("V4")).isEqualTo(ProtocolVersion.V4);
    assertThat(registry.fromName("DSE_V1")).isEqualTo(DseProtocolVersion.DSE_V1);
  }

  @Test
  public void should_fail_to_find_version_by_name_different_case() {
    assertThatThrownBy(() -> registry.fromName("v4")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> registry.fromName("dse_v1"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> registry.fromName("dDSE_v1"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> registry.fromName("dse_v1"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void should_downgrade_if_lower_version_available() {
    Optional<ProtocolVersion> downgraded = registry.downgrade(V4);
    downgraded.map(version -> assertThat(version).isEqualTo(V3)).orElseThrow(AssertionError::new);
  }

  @Test
  public void should_not_downgrade_if_no_lower_version() {
    Optional<ProtocolVersion> downgraded = registry.downgrade(V3);
    assertThat(downgraded.isPresent()).isFalse();
  }

  @Test
  public void should_downgrade_from_dse_to_oss() {
    assertThat(registry.downgrade(DseProtocolVersion.DSE_V1).get()).isEqualTo(ProtocolVersion.V5);
  }

  @Test
  public void should_pick_dse_v2_as_highest_common_when_all_nodes_are_dse_7() {
    assertThat(registry.highestCommon(ImmutableList.of(mockDseNode("7.0"), mockDseNode("7.1"))))
        .isEqualTo(DseProtocolVersion.DSE_V2);
  }

  @Test
  public void should_pick_dse_v2_as_highest_common_when_all_nodes_are_dse_6() {
    assertThat(registry.highestCommon(ImmutableList.of(mockDseNode("6.0"), mockDseNode("6.1"))))
        .isEqualTo(DseProtocolVersion.DSE_V2);
  }

  @Test
  public void should_pick_dse_v1_as_highest_common_when_all_nodes_are_dse_5_1_or_more() {
    assertThat(registry.highestCommon(ImmutableList.of(mockDseNode("5.1"), mockDseNode("6.1"))))
        .isEqualTo(DseProtocolVersion.DSE_V1);
  }

  @Test
  public void should_pick_oss_v4_as_highest_common_when_all_nodes_are_dse_5_or_more() {
    assertThat(
            registry.highestCommon(
                ImmutableList.of(mockDseNode("5.0"), mockDseNode("5.1"), mockDseNode("6.1"))))
        .isEqualTo(ProtocolVersion.V4);
  }

  @Test
  public void should_pick_oss_v3_as_highest_common_when_all_nodes_are_dse_4_7_or_more() {
    assertThat(
            registry.highestCommon(
                ImmutableList.of(mockDseNode("4.7"), mockDseNode("5.1"), mockDseNode("6.1"))))
        .isEqualTo(ProtocolVersion.V3);
  }

  @Test(expected = UnsupportedProtocolVersionException.class)
  public void should_fail_to_pick_highest_common_when_one_node_is_dse_4_6() {
    registry.highestCommon(
        ImmutableList.of(mockDseNode("4.6"), mockDseNode("5.1"), mockDseNode("6.1")));
  }

  @Test(expected = UnsupportedProtocolVersionException.class)
  public void should_fail_to_pick_highest_common_when_one_node_is_2_0() {
    registry.highestCommon(
        ImmutableList.of(mockCassandraNode("3.0.0"), mockCassandraNode("2.0.9")));
  }

  @Test
  public void should_pick_oss_v3_as_highest_common_when_one_node_is_cassandra_2_1() {
    assertThat(
            registry.highestCommon(
                ImmutableList.of(
                    mockDseNode("5.1"), // oss v4
                    mockDseNode("6.1"), // oss v4
                    mockCassandraNode("2.1") // oss v3
                    )))
        .isEqualTo(ProtocolVersion.V3);
  }

  @Test
  public void should_support_date_type_on_oss_v4_and_later() {
    assertThat(registry.supports(V3, DATE_TYPE)).isFalse();
    assertThat(registry.supports(V4, DATE_TYPE)).isTrue();
    assertThat(registry.supports(V5, DATE_TYPE)).isTrue();
    assertThat(registry.supports(V6, DATE_TYPE)).isTrue();
    assertThat(registry.supports(DSE_V1, DATE_TYPE)).isTrue();
    assertThat(registry.supports(DSE_V2, DATE_TYPE)).isTrue();
  }

  @Test
  public void should_support_smallint_and_tinyint_types_on_oss_v4_and_later() {
    assertThat(registry.supports(V3, SMALLINT_AND_TINYINT_TYPES)).isFalse();
    assertThat(registry.supports(V4, SMALLINT_AND_TINYINT_TYPES)).isTrue();
    assertThat(registry.supports(V5, SMALLINT_AND_TINYINT_TYPES)).isTrue();
    assertThat(registry.supports(V6, SMALLINT_AND_TINYINT_TYPES)).isTrue();
    assertThat(registry.supports(DSE_V1, SMALLINT_AND_TINYINT_TYPES)).isTrue();
    assertThat(registry.supports(DSE_V2, SMALLINT_AND_TINYINT_TYPES)).isTrue();
  }

  private Node mockCassandraNode(String rawVersion) {
    Node node = Mockito.mock(Node.class);
    if (rawVersion != null) {
      Mockito.when(node.getCassandraVersion()).thenReturn(Version.parse(rawVersion));
    }
    return node;
  }

  private Node mockDseNode(String rawDseVersion) {
    Node node = Mockito.mock(Node.class);
    Version dseVersion = Version.parse(rawDseVersion);
    Mockito.when(node.getExtras())
        .thenReturn(ImmutableMap.of(DseNodeProperties.DSE_VERSION, dseVersion));

    Version cassandraVersion;
    if (dseVersion.compareTo(DefaultProtocolVersionRegistry.DSE_7_0_0) >= 0) {
      cassandraVersion = Version.parse("5.0");
    } else if (dseVersion.compareTo(DefaultProtocolVersionRegistry.DSE_6_0_0) >= 0) {
      cassandraVersion = Version.parse("4.0");
    } else if (dseVersion.compareTo(DefaultProtocolVersionRegistry.DSE_5_1_0) >= 0) {
      cassandraVersion = Version.parse("3.11");
    } else if (dseVersion.compareTo(DefaultProtocolVersionRegistry.DSE_5_0_0) >= 0) {
      cassandraVersion = Version.parse("3.0");
    } else if (dseVersion.compareTo(DefaultProtocolVersionRegistry.DSE_4_7_0) >= 0) {
      cassandraVersion = Version.parse("2.1");
    } else {
      cassandraVersion = Version.parse("2.0");
    }
    Mockito.when(node.getCassandraVersion()).thenReturn(cassandraVersion);

    return node;
  }
}
