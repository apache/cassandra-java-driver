/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(DataProviderRunner.class)
public class GraphProtocolTest {

  @Mock DriverExecutionProfile executionProfile;

  @Mock GraphStatement graphStatement;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  @UseDataProvider("protocolObjects")
  public void should_pickup_graph_protocol_from_statement(GraphProtocol graphProtocol) {
    when(graphStatement.getSubProtocol()).thenReturn(graphProtocol.toInternalCode());

    GraphProtocol inferredProtocol =
        GraphConversions.inferSubProtocol(graphStatement, executionProfile);

    assertThat(inferredProtocol).isEqualTo(graphProtocol);
    Mockito.verifyZeroInteractions(executionProfile);
  }

  @Test
  @UseDataProvider("protocolStrings")
  public void should_pickup_graph_protocol_and_parse_from_string_config(String stringConfig) {
    when(executionProfile.getString(
            ArgumentMatchers.eq(DseDriverOption.GRAPH_SUB_PROTOCOL), ArgumentMatchers.any()))
        .thenReturn(stringConfig);

    GraphProtocol inferredProtocol =
        GraphConversions.inferSubProtocol(graphStatement, executionProfile);
    assertThat(inferredProtocol.toInternalCode()).isEqualTo(stringConfig);
  }

  @Test
  public void should_use_graphson2_as_default_protocol_when_parsing() {
    when(executionProfile.getString(
            ArgumentMatchers.eq(DseDriverOption.GRAPH_SUB_PROTOCOL), ArgumentMatchers.anyString()))
        .thenAnswer(i -> i.getArguments()[1]);
    GraphProtocol inferredProtocol =
        GraphConversions.inferSubProtocol(graphStatement, executionProfile);
    assertThat(inferredProtocol).isEqualTo(GraphProtocol.GRAPHSON_2_0);
  }

  @Test
  public void should_fail_if_graph_protocol_used_is_invalid() {
    assertThatThrownBy(() -> GraphProtocol.fromString("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Graph protocol used [\"invalid\"] unknown. Possible values are: [ \"graphson-1.0\", \"graphson-2.0\", \"graphson-3.0\", \"graph-binary-1.0\"]");
  }

  @DataProvider
  public static Object[][] protocolObjects() {
    return new Object[][] {
      {GraphProtocol.GRAPHSON_1_0},
      {GraphProtocol.GRAPHSON_2_0},
      {GraphProtocol.GRAPHSON_3_0},
      {GraphProtocol.GRAPH_BINARY_1_0}
    };
  }

  @DataProvider
  public static Object[][] protocolStrings() {
    // putting manual strings here to be sure to be notified if a value in
    // GraphProtocol ever changes
    return new Object[][] {
      {"graphson-1.0"}, {"graphson-2.0"}, {"graphson-3.0"}, {"graph-binary-1.0"}
    };
  }
}
