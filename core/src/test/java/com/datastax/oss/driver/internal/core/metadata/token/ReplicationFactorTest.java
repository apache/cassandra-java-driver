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
package com.datastax.oss.driver.internal.core.metadata.token;

import static com.datastax.oss.driver.Assertions.assertThat;

import org.junit.Test;

public class ReplicationFactorTest {
  @Test
  public void should_parse_factor_from_string() {
    ReplicationFactor transFactor = ReplicationFactor.fromString("3/1");
    assertThat(transFactor.fullReplicas()).isEqualTo(2);
    assertThat(transFactor.hasTransientReplicas()).isTrue();
    assertThat(transFactor.transientReplicas()).isEqualTo(1);

    ReplicationFactor factor = ReplicationFactor.fromString("3");
    assertThat(factor.fullReplicas()).isEqualTo(3);
    assertThat(factor.hasTransientReplicas()).isFalse();
    assertThat(factor.transientReplicas()).isEqualTo(0);
  }

  @Test
  public void should_create_string_from_factor() {
    ReplicationFactor transFactor = new ReplicationFactor(3, 1);
    assertThat(transFactor.toString()).isEqualTo("3/1");
    ReplicationFactor factor = new ReplicationFactor(3);
    assertThat(factor.toString()).isEqualTo("3");
  }
}
