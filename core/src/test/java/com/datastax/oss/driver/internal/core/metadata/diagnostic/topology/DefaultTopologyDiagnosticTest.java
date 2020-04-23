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
import com.datastax.oss.driver.api.core.metadata.diagnostic.TopologyDiagnostic;
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
  @UseDataProvider("leafNodeAvailabilities")
  public void should_create_leaf_diagnostic(
      int total, int up, int down, int unknown, Status expected) {
    // when
    DefaultTopologyDiagnostic diagnostic = new DefaultTopologyDiagnostic(total, up, down, unknown);
    // then
    assertThat(diagnostic.getTotal()).isEqualTo(total);
    assertThat(diagnostic.getUp()).isEqualTo(up);
    assertThat(diagnostic.getDown()).isEqualTo(down);
    assertThat(diagnostic.getUnknown()).isEqualTo(unknown);
    assertThat(diagnostic.getStatus()).isEqualTo(expected);
    assertThat(diagnostic.getDetails())
        .isEqualTo(
            ImmutableMap.of(
                "status", expected, "total", total, "up", up, "down", down, "unknown", unknown));
  }

  @DataProvider
  public static Iterable<?> leafNodeAvailabilities() {
    return ImmutableList.of(
        ImmutableList.of(10, 10, 0, 0, Status.AVAILABLE),
        ImmutableList.of(0, 0, 0, 0, Status.UNAVAILABLE),
        ImmutableList.of(10, 0, 10, 0, Status.UNAVAILABLE),
        ImmutableList.of(10, 0, 0, 10, Status.UNAVAILABLE),
        ImmutableList.of(10, 0, 5, 5, Status.UNAVAILABLE),
        ImmutableList.of(10, 9, 1, 0, Status.PARTIALLY_AVAILABLE),
        ImmutableList.of(10, 9, 0, 1, Status.PARTIALLY_AVAILABLE),
        ImmutableList.of(10, 2, 4, 4, Status.PARTIALLY_AVAILABLE));
  }

  @Test
  @UseDataProvider("nonLeafAvailabilities")
  public void should_create_non_leaf_diagnostic(
      int total,
      int up,
      int down,
      int unknown,
      Map<String, TopologyDiagnostic> locals,
      Status expected) {
    // when
    DefaultTopologyDiagnostic diagnostic =
        new DefaultTopologyDiagnostic(total, up, down, unknown, locals);
    // then
    assertThat(diagnostic.getTotal()).isEqualTo(total);
    assertThat(diagnostic.getUp()).isEqualTo(up);
    assertThat(diagnostic.getDown()).isEqualTo(down);
    assertThat(diagnostic.getUnknown()).isEqualTo(unknown);
    assertThat(diagnostic.getLocalDiagnostics()).isEqualTo(locals);
    assertThat(diagnostic.getStatus()).isEqualTo(expected);
    assertThat(diagnostic.getDetails())
        .isEqualTo(
            ImmutableMap.builder()
                .put("status", expected)
                .put("total", total)
                .put("up", up)
                .put("down", down)
                .put("unknown", unknown)
                .put("dc1", locals.get("dc1").getDetails())
                .put("dc2", locals.get("dc2").getDetails())
                .build());
  }

  @DataProvider
  public static Iterable<?> nonLeafAvailabilities() {
    return ImmutableList.of(
        ImmutableList.of(
            10,
            10,
            0,
            0,
            ImmutableMap.of(
                "dc1", new DefaultTopologyDiagnostic(5, 5, 0, 0),
                "dc2", new DefaultTopologyDiagnostic(5, 5, 0, 0)),
            Status.AVAILABLE),
        ImmutableList.of(
            0,
            0,
            0,
            0,
            ImmutableMap.of(
                "dc1", new DefaultTopologyDiagnostic(0, 0, 0, 0),
                "dc2", new DefaultTopologyDiagnostic(0, 0, 0, 0)),
            Status.UNAVAILABLE),
        ImmutableList.of(
            10,
            9,
            1,
            0,
            ImmutableMap.of(
                "dc1", new DefaultTopologyDiagnostic(5, 0, 0, 0),
                "dc2", new DefaultTopologyDiagnostic(4, 1, 0, 0)),
            Status.PARTIALLY_AVAILABLE),
        ImmutableList.of(
            10,
            9,
            0,
            1,
            ImmutableMap.of(
                "dc1", new DefaultTopologyDiagnostic(5, 0, 0, 0),
                "dc2", new DefaultTopologyDiagnostic(4, 0, 1, 0)),
            Status.PARTIALLY_AVAILABLE),
        ImmutableList.of(
            10,
            2,
            4,
            4,
            ImmutableMap.of(
                "dc1", new DefaultTopologyDiagnostic(5, 1, 2, 2),
                "dc2", new DefaultTopologyDiagnostic(5, 1, 2, 2)),
            Status.PARTIALLY_AVAILABLE));
  }
}
