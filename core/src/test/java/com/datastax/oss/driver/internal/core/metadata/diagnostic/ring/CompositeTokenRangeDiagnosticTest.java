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
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRingDiagnostic.TokenRangeDiagnostic;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class CompositeTokenRangeDiagnosticTest {

  @Test
  public void should_create_diagnostic() {
    // given
    TokenRange tr = mock(TokenRange.class);
    Map<String, TokenRangeDiagnostic> childDiagnostics =
        ImmutableMap.of(
            "dc1", new SimpleTokenRangeDiagnostic(tr, 2, 1),
            "dc2", new SimpleTokenRangeDiagnostic(tr, 1, 0));
    // when
    CompositeTokenRangeDiagnostic diagnostic =
        new CompositeTokenRangeDiagnostic(tr, childDiagnostics);
    // then
    assertThat(diagnostic.getTokenRange()).isEqualTo(tr);
    assertThat(diagnostic.getAliveReplicas()).isEqualTo(1);
    assertThat(diagnostic.getRequiredReplicas()).isEqualTo(3);
    assertThat(diagnostic.getChildDiagnostics()).isEqualTo(childDiagnostics);
    assertThat(diagnostic.isAvailable()).isFalse();
    assertThat(diagnostic.getStatus()).isEqualTo(Status.UNAVAILABLE);
    assertThat(diagnostic.getDetails())
        .isEqualTo(
            ImmutableMap.of(
                "dc1",
                ImmutableMap.of("required", 2, "alive", 1),
                "dc2",
                ImmutableMap.of("required", 1, "alive", 0)));
  }
}
