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
package com.datastax.oss.driver.internal.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class DefaultMetricIdTest {

  @Test
  public void testGetName() {
    DefaultMetricId id = new DefaultMetricId("metric1", ImmutableMap.of());
    assertThat(id.getName()).isEqualTo("metric1");
  }

  @Test
  public void testGetTags() {
    DefaultMetricId id =
        new DefaultMetricId("metric1", ImmutableMap.of("tag1", "value1", "tag2", "value2"));
    assertThat(id.getTags())
        .hasSize(2)
        .containsEntry("tag1", "value1")
        .containsEntry("tag2", "value2");
  }

  @Test
  public void testEquals() {
    DefaultMetricId id1 =
        new DefaultMetricId("metric1", ImmutableMap.of("tag1", "value1", "tag2", "value2"));
    DefaultMetricId id2 =
        new DefaultMetricId("metric1", ImmutableMap.of("tag1", "value1", "tag2", "value2"));
    DefaultMetricId id3 =
        new DefaultMetricId("metric2", ImmutableMap.of("tag1", "value1", "tag2", "value2"));
    DefaultMetricId id4 = new DefaultMetricId("metric1", ImmutableMap.of("tag2", "value2"));
    assertThat(id1).isEqualTo(id2).isNotEqualTo(id3).isNotEqualTo(id4);
  }

  @Test
  public void testHashCode() {
    DefaultMetricId id1 =
        new DefaultMetricId("metric1", ImmutableMap.of("tag1", "value1", "tag2", "value2"));
    DefaultMetricId id2 =
        new DefaultMetricId("metric1", ImmutableMap.of("tag1", "value1", "tag2", "value2"));
    assertThat(id1).hasSameHashCodeAs(id2);
  }
}
