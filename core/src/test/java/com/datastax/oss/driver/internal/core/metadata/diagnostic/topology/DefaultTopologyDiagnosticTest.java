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

import com.datastax.oss.driver.api.core.metadata.diagnostic.Status;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TopologyDiagnostic.NodeGroupDiagnostic;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DefaultTopologyDiagnosticTest {

  @Test
  @UseDataProvider("nodeGroupAvailabilities")
  public void should_create_diagnostic(
      NodeGroupDiagnostic global, Map<String, NodeGroupDiagnostic> locals, Status expected) {
    // when
    DefaultTopologyDiagnostic diagnostic = new DefaultTopologyDiagnostic(global, locals);
    // then
    assertThat(diagnostic.getGlobalDiagnostic()).isEqualTo(global);
    assertThat(diagnostic.getLocalDiagnostics()).isEqualTo(locals);
    assertThat(diagnostic.getStatus()).isEqualTo(expected);
    assertThat(diagnostic.getDetails())
        .isEqualTo(
            ImmutableMap.builder()
                .put("status", expected)
                .put("total", global.getTotal())
                .put("up", global.getUp())
                .put("down", global.getDown())
                .put("unknown", global.getUnknown())
                .put("dc1", locals.get("dc1").getDetails())
                .put("dc2", locals.get("dc2").getDetails())
                .build());
  }

  @DataProvider
  public static Iterable<?> nodeGroupAvailabilities() {
    return ImmutableList.of(
        ImmutableList.of(
            new DefaultNodeGroupDiagnostic(10, 10, 0, 0),
            ImmutableMap.of(
                "dc1", new DefaultNodeGroupDiagnostic(5, 5, 0, 0),
                "dc2", new DefaultNodeGroupDiagnostic(5, 5, 0, 0)),
            Status.AVAILABLE),
        ImmutableList.of(
            new DefaultNodeGroupDiagnostic(0, 0, 0, 0),
            ImmutableMap.of(
                "dc1", new DefaultNodeGroupDiagnostic(0, 0, 0, 0),
                "dc2", new DefaultNodeGroupDiagnostic(0, 0, 0, 0)),
            Status.UNAVAILABLE),
        ImmutableList.of(
            new DefaultNodeGroupDiagnostic(10, 9, 1, 0),
            ImmutableMap.of(
                "dc1", new DefaultNodeGroupDiagnostic(5, 0, 0, 0),
                "dc2", new DefaultNodeGroupDiagnostic(4, 1, 0, 0)),
            Status.PARTIALLY_AVAILABLE),
        ImmutableList.of(
            new DefaultNodeGroupDiagnostic(10, 9, 0, 1),
            ImmutableMap.of(
                "dc1", new DefaultNodeGroupDiagnostic(5, 0, 0, 0),
                "dc2", new DefaultNodeGroupDiagnostic(4, 0, 1, 0)),
            Status.PARTIALLY_AVAILABLE),
        ImmutableList.of(
            new DefaultNodeGroupDiagnostic(10, 2, 4, 4),
            ImmutableMap.of(
                "dc1", new DefaultNodeGroupDiagnostic(5, 1, 2, 2),
                "dc2", new DefaultNodeGroupDiagnostic(5, 1, 2, 2)),
            Status.PARTIALLY_AVAILABLE));
  }
}
