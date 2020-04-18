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
package com.datastax.oss.driver.internal.core.metadata.diagnostic.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TopologyDiagnostic;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopologyDiagnosticGeneratorTest {

  @Mock Metadata metadata;

  @Mock(name = "node1")
  Node node1;

  @Mock(name = "node2")
  Node node2;

  @Mock(name = "node3")
  Node node3;

  @Mock(name = "node4")
  Node node4;

  @Before
  public void setUp() {
    UUID id1 = UUID.randomUUID();
    UUID id2 = UUID.randomUUID();
    UUID id3 = UUID.randomUUID();
    UUID id4 = UUID.randomUUID();
    given(metadata.getNodes())
        .willReturn(ImmutableMap.of(id1, node1, id2, node2, id3, node3, id4, node4));
    given(node1.getDatacenter()).willReturn("dc1");
    given(node2.getDatacenter()).willReturn("dc1");
    given(node3.getDatacenter()).willReturn("dc2");
    given(node4.getDatacenter()).willReturn("dc2");
    given(node1.getState()).willReturn(NodeState.UP);
    given(node2.getState()).willReturn(NodeState.UP);
    given(node3.getState()).willReturn(NodeState.DOWN);
    given(node4.getState()).willReturn(NodeState.UNKNOWN);
  }

  @Test
  public void should_generate_diagnostic_for_non_local_CL() {
    // given
    TopologyDiagnosticGenerator generator = new TopologyDiagnosticGenerator(metadata);
    // when
    TopologyDiagnostic diagnostic = generator.generate();
    // then
    assertThat(diagnostic).isExactlyInstanceOf(DefaultTopologyDiagnostic.class);
    assertThat(diagnostic)
        .isEqualTo(
            new DefaultTopologyDiagnostic(
                new DefaultNodeGroupDiagnostic(4, 2, 1, 1),
                ImmutableMap.of(
                    "dc1",
                    new DefaultNodeGroupDiagnostic(2, 2, 0, 0),
                    "dc2",
                    new DefaultNodeGroupDiagnostic(2, 0, 1, 1))));
  }
}
