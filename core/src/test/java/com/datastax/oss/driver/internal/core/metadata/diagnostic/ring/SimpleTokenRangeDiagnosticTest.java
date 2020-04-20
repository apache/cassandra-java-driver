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
package com.datastax.oss.driver.internal.core.metadata.diagnostic.ring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.metadata.diagnostic.Status;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class SimpleTokenRangeDiagnosticTest {

  @Test
  @UseDataProvider("rangeAvailabilities")
  public void should_create_diagnostic(int requiredReplicas, int aliveReplicas, Status expected) {
    // given
    TokenRange tr = mock(TokenRange.class);
    // when
    SimpleTokenRangeDiagnostic diagnostic =
        new SimpleTokenRangeDiagnostic(tr, requiredReplicas, aliveReplicas);
    // then
    assertThat(diagnostic.getTokenRange()).isEqualTo(tr);
    assertThat(diagnostic.getAliveReplicas()).isEqualTo(aliveReplicas);
    assertThat(diagnostic.getRequiredReplicas()).isEqualTo(requiredReplicas);
    assertThat(diagnostic.isAvailable()).isEqualTo(expected == Status.AVAILABLE);
    assertThat(diagnostic.getStatus()).isEqualTo(expected);
    assertThat(diagnostic.getDetails())
        .isEqualTo(ImmutableMap.of("required", requiredReplicas, "alive", aliveReplicas));
  }

  @DataProvider
  public static Iterable<?> rangeAvailabilities() {
    return ImmutableList.of(
        ImmutableList.of(2, 3, Status.AVAILABLE),
        ImmutableList.of(2, 2, Status.AVAILABLE),
        ImmutableList.of(2, 1, Status.UNAVAILABLE),
        ImmutableList.of(2, 0, Status.UNAVAILABLE));
  }
}
