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
package com.datastax.dse.driver.internal.core.graph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Strict.class)
@SuppressWarnings("deprecation")
public class GraphExecutionInfoConverterTest {

  @Mock GraphStatement<?> request;
  @Mock Node node;

  private List<Entry<Node, Throwable>> errors;
  private List<String> warnings;
  private ImmutableMap<String, ByteBuffer> payload;

  @Before
  public void setUp() {
    errors =
        Collections.singletonList(
            new SimpleEntry<>(node, new ServerError(node, "this is a server error")));
    warnings = Collections.singletonList("this is a warning");
    payload = ImmutableMap.of("key", Bytes.fromHexString("0xcafebabe"));
  }

  @Test
  public void should_convert_to_graph_execution_info() {

    // given
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(executionInfo.getRequest()).thenReturn(request);
    when(executionInfo.getCoordinator()).thenReturn(node);
    when(executionInfo.getSpeculativeExecutionCount()).thenReturn(42);
    when(executionInfo.getSuccessfulExecutionIndex()).thenReturn(10);
    when(executionInfo.getErrors()).thenReturn(errors);
    when(executionInfo.getWarnings()).thenReturn(warnings);
    when(executionInfo.getIncomingPayload()).thenReturn(payload);

    // when
    com.datastax.dse.driver.api.core.graph.GraphExecutionInfo graphExecutionInfo =
        GraphExecutionInfoConverter.convert(executionInfo);

    // then
    assertThat(graphExecutionInfo.getStatement()).isSameAs(request);
    assertThat(graphExecutionInfo.getCoordinator()).isSameAs(node);
    assertThat(graphExecutionInfo.getSpeculativeExecutionCount()).isEqualTo(42);
    assertThat(graphExecutionInfo.getSuccessfulExecutionIndex()).isEqualTo(10);
    assertThat(graphExecutionInfo.getErrors()).isEqualTo(errors);
    assertThat(graphExecutionInfo.getWarnings()).isEqualTo(warnings);
    assertThat(graphExecutionInfo.getIncomingPayload()).isEqualTo(payload);
  }

  @Test
  public void should_convert_from_graph_execution_info() {

    // given
    com.datastax.dse.driver.api.core.graph.GraphExecutionInfo graphExecutionInfo =
        mock(com.datastax.dse.driver.api.core.graph.GraphExecutionInfo.class);
    when(graphExecutionInfo.getStatement()).thenAnswer(args -> request);
    when(graphExecutionInfo.getCoordinator()).thenReturn(node);
    when(graphExecutionInfo.getSpeculativeExecutionCount()).thenReturn(42);
    when(graphExecutionInfo.getSuccessfulExecutionIndex()).thenReturn(10);
    when(graphExecutionInfo.getErrors()).thenReturn(errors);
    when(graphExecutionInfo.getWarnings()).thenReturn(warnings);
    when(graphExecutionInfo.getIncomingPayload()).thenReturn(payload);

    // when
    ExecutionInfo executionInfo = GraphExecutionInfoConverter.convert(graphExecutionInfo);

    // then
    assertThat(executionInfo.getRequest()).isSameAs(request);
    assertThatThrownBy(executionInfo::getStatement).isInstanceOf(ClassCastException.class);
    assertThat(executionInfo.getCoordinator()).isSameAs(node);
    assertThat(executionInfo.getSpeculativeExecutionCount()).isEqualTo(42);
    assertThat(executionInfo.getSuccessfulExecutionIndex()).isEqualTo(10);
    assertThat(executionInfo.getErrors()).isEqualTo(errors);
    assertThat(executionInfo.getWarnings()).isEqualTo(warnings);
    assertThat(executionInfo.getIncomingPayload()).isEqualTo(payload);
    assertThat(executionInfo.getPagingState()).isNull();
    assertThat(executionInfo.isSchemaInAgreement()).isTrue();
    assertThat(executionInfo.getQueryTraceAsync()).isCompletedExceptionally();
    assertThatThrownBy(executionInfo::getQueryTrace)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Tracing was disabled for this request");
    assertThat(executionInfo.getResponseSizeInBytes()).isEqualTo(-1L);
    assertThat(executionInfo.getCompressedResponseSizeInBytes()).isEqualTo(-1L);
  }
}
