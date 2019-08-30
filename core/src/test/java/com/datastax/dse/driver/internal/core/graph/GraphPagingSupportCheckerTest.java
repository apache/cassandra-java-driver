/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import static com.datastax.dse.driver.api.core.graph.PagingEnabledOptions.AUTO;
import static com.datastax.dse.driver.api.core.graph.PagingEnabledOptions.DISABLED;
import static com.datastax.dse.driver.api.core.graph.PagingEnabledOptions.ENABLED;
import static com.datastax.dse.driver.internal.core.graph.GraphPagingSupportChecker.GRAPH_PAGING_MIN_DSE_VERSION;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.PagingEnabledOptions;
import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.dse.driver.internal.core.DseProtocolFeature;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
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
public class GraphPagingSupportCheckerTest {

  @UseDataProvider("pagingEnabled")
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
    boolean pagingEnabled =
        new GraphPagingSupportChecker().isPagingEnabled(graphStatement, context);

    // then
    assertThat(pagingEnabled).isEqualTo(expected);
  }

  @Test
  public void should_not_support_paging_when_statement_profile_not_present() {
    // given
    GraphStatement graphStatement = mock(GraphStatement.class);
    InternalDriverContext context = protocolWithPagingSupport(true);
    contextGraphPagingEnabled(context, DISABLED);
    addNodeWithDseVersion(context, Collections.singletonList(GRAPH_PAGING_MIN_DSE_VERSION));

    // when
    boolean pagingEnabled =
        new GraphPagingSupportChecker().isPagingEnabled(graphStatement, context);

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
    addNodeWithDseVersion(context, Collections.singletonList(GRAPH_PAGING_MIN_DSE_VERSION));

    // when
    boolean pagingEnabled =
        new GraphPagingSupportChecker().isPagingEnabled(graphStatement, context);

    // then
    assertThat(pagingEnabled).isEqualTo(true);
  }

  @DataProvider()
  public static Object[][] pagingEnabled() {
    List<Version> listWithGraphPagingNode = Collections.singletonList(GRAPH_PAGING_MIN_DSE_VERSION);
    List<Version> listWithoutGraphPagingNode = Collections.singletonList(Version.parse("6.7.0"));
    List<Version> listWithNull = Collections.singletonList(null);
    List<Version> listWithTwoNodesOneNotSupporting =
        Arrays.asList(Version.parse("6.7.0"), GRAPH_PAGING_MIN_DSE_VERSION);

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
    when(context.getProtocolVersion()).thenReturn(DefaultProtocolVersion.V4);
    ProtocolVersionRegistry protocolVersionRegistry = mock(ProtocolVersionRegistry.class);
    when(protocolVersionRegistry.supports(
            DefaultProtocolVersion.V4, DseProtocolFeature.CONTINUOUS_PAGING))
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
}
