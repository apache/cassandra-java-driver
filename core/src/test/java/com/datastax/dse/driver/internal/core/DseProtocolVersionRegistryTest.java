/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Note: some tests in this class depend on the set of supported protocol versions, they will need
 * to be updated as new versions are added or become non-beta.
 */
public class DseProtocolVersionRegistryTest {

  private DseProtocolVersionRegistry registry = new DseProtocolVersionRegistry("test");

  @Test
  public void should_find_version_by_code() {
    assertThat(registry.fromCode(4)).isEqualTo(DefaultProtocolVersion.V4);
    assertThat(registry.fromCode(65)).isEqualTo(DseProtocolVersion.DSE_V1);
  }

  @Test
  public void should_find_version_by_name() {
    assertThat(registry.fromName("V4")).isEqualTo(DefaultProtocolVersion.V4);
    assertThat(registry.fromName("DSE_V1")).isEqualTo(DseProtocolVersion.DSE_V1);
  }

  @Test
  public void should_downgrade_from_dse_to_oss() {
    assertThat(registry.downgrade(DseProtocolVersion.DSE_V1).get())
        .isEqualTo(DefaultProtocolVersion.V4);
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
        .isEqualTo(DefaultProtocolVersion.V4);
  }

  @Test
  public void should_pick_oss_v3_as_highest_common_when_all_nodes_are_dse_4_7_or_more() {
    assertThat(
            registry.highestCommon(
                ImmutableList.of(mockDseNode("4.7"), mockDseNode("5.1"), mockDseNode("6.1"))))
        .isEqualTo(DefaultProtocolVersion.V3);
  }

  @Test(expected = UnsupportedProtocolVersionException.class)
  public void should_fail_to_pick_highest_common_when_one_node_is_dse_4_6() {
    registry.highestCommon(
        ImmutableList.of(mockDseNode("4.6"), mockDseNode("5.1"), mockDseNode("6.1")));
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
        .isEqualTo(DefaultProtocolVersion.V3);
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
    if (dseVersion.compareTo(DseProtocolVersionRegistry.DSE_6_0_0) >= 0) {
      cassandraVersion = Version.parse("4.0");
    } else if (dseVersion.compareTo(DseProtocolVersionRegistry.DSE_5_1_0) >= 0) {
      cassandraVersion = Version.parse("3.11");
    } else if (dseVersion.compareTo(Version.parse("5.0")) >= 0) {
      cassandraVersion = Version.parse("3.0");
    } else if (dseVersion.compareTo(DseProtocolVersionRegistry.DSE_4_7_0) >= 0) {
      cassandraVersion = Version.parse("2.1");
    } else {
      cassandraVersion = Version.parse("2.0");
    }
    Mockito.when(node.getCassandraVersion()).thenReturn(cassandraVersion);

    return node;
  }
}
