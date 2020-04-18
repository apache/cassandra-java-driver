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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DefaultNodeGroupDiagnosticTest {

  @Test
  @UseDataProvider("nodeAvailabilities")
  public void should_create_diagnostic(int total, int up, int down, int unknown, Status expected) {
    // when
    DefaultNodeGroupDiagnostic diagnostic =
        new DefaultNodeGroupDiagnostic(total, up, down, unknown);
    // then
    assertThat(diagnostic.getTotal()).isEqualTo(total);
    assertThat(diagnostic.getUp()).isEqualTo(up);
    assertThat(diagnostic.getDown()).isEqualTo(down);
    assertThat(diagnostic.getUnknown()).isEqualTo(unknown);
    assertThat(diagnostic.getStatus()).isEqualTo(expected);
    assertThat(diagnostic.getDetails())
        .isEqualTo(ImmutableMap.of("total", total, "up", up, "down", down, "unknown", unknown));
  }

  @DataProvider
  public static Iterable<?> nodeAvailabilities() {
    return ImmutableList.of(
        ImmutableList.of(10, 10, 0, 0, Status.AVAILABLE),
        ImmutableList.of(0, 0, 0, 0, Status.UNAVAILABLE),
        ImmutableList.of(10, 9, 1, 0, Status.PARTIALLY_AVAILABLE),
        ImmutableList.of(10, 9, 0, 1, Status.PARTIALLY_AVAILABLE),
        ImmutableList.of(10, 2, 4, 4, Status.PARTIALLY_AVAILABLE));
  }
}
