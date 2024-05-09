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
package com.datastax.dse.driver.internal.core.graph;

import static com.datastax.dse.driver.DseTestFixtures.mockNodesInMetadataWithVersions;
import static com.datastax.dse.driver.api.core.graph.PagingEnabledOptions.AUTO;
import static com.datastax.dse.driver.api.core.graph.PagingEnabledOptions.DISABLED;
import static com.datastax.dse.driver.api.core.graph.PagingEnabledOptions.ENABLED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.DseTestDataProviders;
import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.PagingEnabledOptions;
import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.dse.driver.internal.core.DseProtocolFeature;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class GraphSupportCheckerTest {

  @UseDataProvider("graphPagingEnabledAndDseVersions")
  @Test
  public void should_check_if_paging_is_supported(
      boolean protocolWithPagingSupport,
      PagingEnabledOptions statementGraphPagingEnabled,
      PagingEnabledOptions contextGraphPagingEnabled,
      List<Version> nodeDseVersions,
      boolean expected) {
    // given
    GraphStatement graphStatement = mock(GraphStatement.class);
    InternalDriverContext context = protocolWithPagingSupport(protocolWithPagingSupport);
    statementGraphPagingEnabled(graphStatement, statementGraphPagingEnabled);
    contextGraphPagingEnabled(context, contextGraphPagingEnabled);
    addNodeWithDseVersion(context, nodeDseVersions);

    // when
    boolean pagingEnabled = new GraphSupportChecker().isPagingEnabled(graphStatement, context);

    // then
    assertThat(pagingEnabled).isEqualTo(expected);
  }

  @Test
  public void should_not_support_paging_when_statement_profile_not_present() {
    // given
    GraphStatement graphStatement = mock(GraphStatement.class);
    InternalDriverContext context = protocolWithPagingSupport(true);
    contextGraphPagingEnabled(context, DISABLED);
    addNodeWithDseVersion(context, Collections.singletonList(Version.parse("6.8.0")));

    // when
    boolean pagingEnabled = new GraphSupportChecker().isPagingEnabled(graphStatement, context);

    // then
    assertThat(pagingEnabled).isEqualTo(false);
  }

  @Test
  public void
      should_support_paging_when_statement_profile_not_present_but_context_profile_has_paging_enabled() {
    // given
    GraphStatement graphStatement = mock(GraphStatement.class);
    InternalDriverContext context = protocolWithPagingSupport(true);
    contextGraphPagingEnabled(context, ENABLED);
    addNodeWithDseVersion(context, Collections.singletonList(Version.parse("6.8.0")));

    // when
    boolean pagingEnabled = new GraphSupportChecker().isPagingEnabled(graphStatement, context);

    // then
    assertThat(pagingEnabled).isEqualTo(true);
  }

  @DataProvider()
  public static Object[][] graphPagingEnabledAndDseVersions() {
    List<Version> listWithGraphPagingNode = Collections.singletonList(Version.parse("6.8.0"));
    List<Version> listWithoutGraphPagingNode = Collections.singletonList(Version.parse("6.7.0"));
    List<Version> listWithNull = Collections.singletonList(null);
    List<Version> listWithTwoNodesOneNotSupporting =
        Arrays.asList(Version.parse("6.7.0"), Version.parse("6.8.0"));

    return new Object[][] {
      {false, ENABLED, ENABLED, listWithGraphPagingNode, true},
      {true, ENABLED, ENABLED, listWithoutGraphPagingNode, true},
      {true, ENABLED, DISABLED, listWithGraphPagingNode, true},
      {true, ENABLED, ENABLED, listWithGraphPagingNode, true},
      {true, ENABLED, ENABLED, listWithNull, true},
      {true, ENABLED, ENABLED, listWithTwoNodesOneNotSupporting, true},
      {true, DISABLED, ENABLED, listWithGraphPagingNode, false},
      {true, DISABLED, AUTO, listWithGraphPagingNode, false},
      {true, DISABLED, DISABLED, listWithGraphPagingNode, false},
      {true, AUTO, AUTO, listWithGraphPagingNode, true},
      {true, AUTO, DISABLED, listWithGraphPagingNode, true},
      {false, AUTO, AUTO, listWithGraphPagingNode, false},
      {true, AUTO, AUTO, listWithTwoNodesOneNotSupporting, false},
      {true, AUTO, AUTO, listWithNull, false},
    };
  }

  private void addNodeWithDseVersion(InternalDriverContext context, List<Version> dseVersions) {
    MetadataManager manager = mock(MetadataManager.class);
    when(context.getMetadataManager()).thenReturn(manager);
    Metadata metadata = mock(Metadata.class);
    when(manager.getMetadata()).thenReturn(metadata);
    Map<UUID, Node> nodes = new HashMap<>();
    for (Version v : dseVersions) {
      Node node = mock(Node.class);
      Map<String, Object> extras = new HashMap<>();
      extras.put(DseNodeProperties.DSE_VERSION, v);
      when(node.getExtras()).thenReturn(extras);
      nodes.put(UUID.randomUUID(), node);
    }
    when(metadata.getNodes()).thenReturn(nodes);
  }

  private void contextGraphPagingEnabled(
      InternalDriverContext context, PagingEnabledOptions option) {
    DriverExecutionProfile driverExecutionProfile = mock(DriverExecutionProfile.class);
    when(driverExecutionProfile.getString(DseDriverOption.GRAPH_PAGING_ENABLED))
        .thenReturn(option.name());
    DriverConfig config = mock(DriverConfig.class);
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(driverExecutionProfile);
  }

  private InternalDriverContext protocolWithPagingSupport(boolean pagingSupport) {
    InternalDriverContext context = mock(InternalDriverContext.class);
    when(context.getProtocolVersion()).thenReturn(DseProtocolVersion.DSE_V2);
    ProtocolVersionRegistry protocolVersionRegistry = mock(ProtocolVersionRegistry.class);
    when(protocolVersionRegistry.supports(
            DseProtocolVersion.DSE_V2, DseProtocolFeature.CONTINUOUS_PAGING))
        .thenReturn(pagingSupport);
    when(context.getProtocolVersionRegistry()).thenReturn(protocolVersionRegistry);
    return context;
  }

  private void statementGraphPagingEnabled(
      GraphStatement graphStatement, PagingEnabledOptions option) {
    DriverExecutionProfile driverExecutionProfile = mock(DriverExecutionProfile.class);
    when(driverExecutionProfile.getString(DseDriverOption.GRAPH_PAGING_ENABLED))
        .thenReturn(option.name());
    when(graphStatement.getExecutionProfile()).thenReturn(driverExecutionProfile);
  }

  @Test
  @UseDataProvider("dseVersionsAndGraphProtocols")
  public void should_determine_default_graph_protocol_from_dse_version(
      Version[] dseVersions, GraphProtocol expectedProtocol) {
    // mock up the metadata for the context
    // using 'true' here will treat null test Versions as no DSE_VERSION info in the metadata
    DefaultDriverContext context =
        mockNodesInMetadataWithVersions(mock(DefaultDriverContext.class), true, dseVersions);
    GraphProtocol graphProtocol = new GraphSupportChecker().getDefaultGraphProtocol(context);
    assertThat(graphProtocol).isEqualTo(expectedProtocol);
  }

  @Test
  @UseDataProvider("dseVersionsAndGraphProtocols")
  public void should_determine_default_graph_protocol_from_dse_version_with_null_versions(
      Version[] dseVersions, GraphProtocol expectedProtocol) {
    // mock up the metadata for the context
    // using 'false' here will treat null test Versions as explicit NULL info for DSE_VERSION
    DefaultDriverContext context =
        mockNodesInMetadataWithVersions(mock(DefaultDriverContext.class), false, dseVersions);
    GraphProtocol graphProtocol = new GraphSupportChecker().getDefaultGraphProtocol(context);
    assertThat(graphProtocol).isEqualTo(expectedProtocol);
  }

  @DataProvider
  public static Object[][] dseVersionsAndGraphProtocols() {
    return new Object[][] {
      {new Version[] {Version.parse("5.0.3")}, GraphProtocol.GRAPHSON_2_0},
      {new Version[] {Version.parse("6.0.1")}, GraphProtocol.GRAPHSON_2_0},
      {new Version[] {Version.parse("6.7.4")}, GraphProtocol.GRAPHSON_2_0},
      {new Version[] {Version.parse("6.8.0")}, GraphProtocol.GRAPH_BINARY_1_0},
      {new Version[] {Version.parse("7.0.0")}, GraphProtocol.GRAPH_BINARY_1_0},
      {new Version[] {Version.parse("5.0.3"), Version.parse("6.8.0")}, GraphProtocol.GRAPHSON_2_0},
      {new Version[] {Version.parse("6.7.4"), Version.parse("6.8.0")}, GraphProtocol.GRAPHSON_2_0},
      {new Version[] {Version.parse("6.8.0"), Version.parse("6.7.4")}, GraphProtocol.GRAPHSON_2_0},
      {
        new Version[] {Version.parse("6.8.0"), Version.parse("7.0.0")},
        GraphProtocol.GRAPH_BINARY_1_0
      },
      {new Version[] {Version.parse("6.7.4"), Version.parse("6.7.4")}, GraphProtocol.GRAPHSON_2_0},
      {
        new Version[] {Version.parse("6.8.0"), Version.parse("6.8.0")},
        GraphProtocol.GRAPH_BINARY_1_0
      },
      {null, GraphProtocol.GRAPHSON_2_0},
      {new Version[] {null}, GraphProtocol.GRAPHSON_2_0},
      {new Version[] {null, Version.parse("6.8.0")}, GraphProtocol.GRAPHSON_2_0},
      {
        new Version[] {Version.parse("6.8.0"), Version.parse("7.0.0"), null},
        GraphProtocol.GRAPHSON_2_0
      },
    };
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "supportedGraphProtocols")
  public void should_pickup_graph_protocol_from_statement(GraphProtocol graphProtocol) {
    GraphStatement graphStatement = mock(GraphStatement.class);
    DriverExecutionProfile executionProfile = mock(DriverExecutionProfile.class);
    when(graphStatement.getSubProtocol()).thenReturn(graphProtocol.toInternalCode());

    GraphProtocol inferredProtocol =
        new GraphSupportChecker()
            .inferGraphProtocol(
                graphStatement, executionProfile, mock(InternalDriverContext.class));

    assertThat(inferredProtocol).isEqualTo(graphProtocol);
    verifyZeroInteractions(executionProfile);
  }

  @Test
  @UseDataProvider("graphProtocolStringsAndDseVersions")
  public void should_pickup_graph_protocol_and_parse_from_string_config(
      String stringConfig, Version dseVersion) {
    GraphStatement graphStatement = mock(GraphStatement.class);
    DriverExecutionProfile executionProfile = mock(DriverExecutionProfile.class);
    when(executionProfile.isDefined(DseDriverOption.GRAPH_SUB_PROTOCOL)).thenReturn(Boolean.TRUE);
    when(executionProfile.getString(eq(DseDriverOption.GRAPH_SUB_PROTOCOL)))
        .thenReturn(stringConfig);

    DefaultDriverContext context =
        mockNodesInMetadataWithVersions(mock(DefaultDriverContext.class), true, dseVersion);
    GraphProtocol inferredProtocol =
        new GraphSupportChecker().inferGraphProtocol(graphStatement, executionProfile, context);
    assertThat(inferredProtocol.toInternalCode()).isEqualTo(stringConfig);
  }

  @DataProvider
  public static Object[][] graphProtocolStringsAndDseVersions() {
    // putting manual strings here to be sure to be notified if a value in
    // GraphProtocol ever changes
    return new Object[][] {
      {"graphson-1.0", Version.parse("6.7.0")},
      {"graphson-1.0", Version.parse("6.8.0")},
      {"graphson-2.0", Version.parse("6.7.0")},
      {"graphson-2.0", Version.parse("6.8.0")},
      {"graph-binary-1.0", Version.parse("6.7.0")},
      {"graph-binary-1.0", Version.parse("6.8.0")},
    };
  }

  @Test
  @UseDataProvider("dseVersions6")
  public void should_use_correct_default_protocol_when_parsing(Version dseVersion) {
    GraphStatement graphStatement = mock(GraphStatement.class);
    DriverExecutionProfile executionProfile = mock(DriverExecutionProfile.class);
    DefaultDriverContext context =
        mockNodesInMetadataWithVersions(mock(DefaultDriverContext.class), true, dseVersion);
    GraphProtocol inferredProtocol =
        new GraphSupportChecker().inferGraphProtocol(graphStatement, executionProfile, context);
    // For DSE 6.8 and newer, the default should be GraphSON binary
    // for DSE older than 6.8, the default should be GraphSON2
    assertThat(inferredProtocol)
        .isEqualTo(
            (dseVersion.compareTo(Version.parse("6.8.0")) < 0)
                ? GraphProtocol.GRAPHSON_2_0
                : GraphProtocol.GRAPH_BINARY_1_0);
  }

  @DataProvider
  public static Object[][] dseVersions6() {
    return new Object[][] {{Version.parse("6.7.0")}, {Version.parse("6.8.0")}};
  }

  @Test
  public void should_fail_if_graph_protocol_used_is_invalid() {
    assertThatThrownBy(() -> GraphProtocol.fromString("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Graph protocol used [\"invalid\"] unknown. Possible values are: [ \"graphson-1.0\", \"graphson-2.0\", \"graph-binary-1.0\"]");
  }

  @Test
  public void should_fail_if_graph_protocol_used_is_graphson_3() {
    assertThatThrownBy(() -> GraphProtocol.fromString("graphson-3.0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Graph protocol used [\"graphson-3.0\"] unknown. Possible values are: [ \"graphson-1.0\", \"graphson-2.0\", \"graph-binary-1.0\"]");
  }
}
