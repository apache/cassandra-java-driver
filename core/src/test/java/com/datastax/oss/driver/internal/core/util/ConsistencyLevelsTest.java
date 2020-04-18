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
package com.datastax.oss.driver.internal.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationFactor;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class ConsistencyLevelsTest {

  @Test
  @UseDataProvider("incompatibleLevels")
  public void should_filter_incompatible_levels(
      ConsistencyLevel initial, ConsistencyLevel expected) {
    // when
    ConsistencyLevel actual = ConsistencyLevels.filterForSimpleStrategy(initial);
    // then
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_throw_for_unknown_cl_when_filtering() {
    // given
    ConsistencyLevel unknown = mock(ConsistencyLevel.class, "UNKNOWN");
    // when
    @SuppressWarnings("ResultOfMethodCallIgnored")
    Throwable error = catchThrowable(() -> ConsistencyLevels.filterForSimpleStrategy(unknown));
    // then
    assertThat(error)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported consistency level: UNKNOWN");
  }

  @Test
  @UseDataProvider("requiredReplicas")
  public void should_compute_required_replicas(ConsistencyLevel cl, int rf, int expected) {
    // when
    int actual = ConsistencyLevels.requiredReplicas(cl, new ReplicationFactor(rf));
    // then
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_throw_for_unknown_cl_when_computing_required_replicas() {
    // given
    ConsistencyLevel unknown = mock(ConsistencyLevel.class, "UNKNOWN");
    // when
    Throwable error =
        catchThrowable(() -> ConsistencyLevels.requiredReplicas(unknown, new ReplicationFactor(3)));
    // then
    assertThat(error)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported consistency level: UNKNOWN");
  }

  @DataProvider
  public static Iterable<?> incompatibleLevels() {
    return ImmutableList.of(
        // compatible levels
        ImmutableList.of(ConsistencyLevel.ONE, ConsistencyLevel.ONE),
        ImmutableList.of(ConsistencyLevel.TWO, ConsistencyLevel.TWO),
        ImmutableList.of(ConsistencyLevel.THREE, ConsistencyLevel.THREE),
        ImmutableList.of(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
        ImmutableList.of(ConsistencyLevel.ALL, ConsistencyLevel.ALL),
        ImmutableList.of(ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL),
        // incompatible levels
        ImmutableList.of(ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.ONE),
        ImmutableList.of(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.QUORUM),
        ImmutableList.of(ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.QUORUM),
        ImmutableList.of(ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL));
  }

  @DataProvider
  public static Iterable<?> requiredReplicas() {
    return ImmutableList.of(
        ImmutableList.of(ConsistencyLevel.ONE, 1, 1),
        ImmutableList.of(ConsistencyLevel.ONE, 3, 1),
        ImmutableList.of(ConsistencyLevel.LOCAL_ONE, 1, 1),
        ImmutableList.of(ConsistencyLevel.LOCAL_ONE, 3, 1),
        ImmutableList.of(ConsistencyLevel.TWO, 1, 2), // impossible
        ImmutableList.of(ConsistencyLevel.TWO, 3, 2),
        ImmutableList.of(ConsistencyLevel.THREE, 1, 3), // impossible
        ImmutableList.of(ConsistencyLevel.THREE, 3, 3),
        ImmutableList.of(ConsistencyLevel.QUORUM, 1, 1),
        ImmutableList.of(ConsistencyLevel.QUORUM, 3, 2),
        ImmutableList.of(ConsistencyLevel.QUORUM, 5, 3),
        ImmutableList.of(ConsistencyLevel.LOCAL_QUORUM, 1, 1),
        ImmutableList.of(ConsistencyLevel.LOCAL_QUORUM, 3, 2),
        ImmutableList.of(ConsistencyLevel.LOCAL_QUORUM, 5, 3),
        ImmutableList.of(ConsistencyLevel.EACH_QUORUM, 1, 1),
        ImmutableList.of(ConsistencyLevel.EACH_QUORUM, 3, 2),
        ImmutableList.of(ConsistencyLevel.EACH_QUORUM, 5, 3),
        ImmutableList.of(ConsistencyLevel.ALL, 1, 1),
        ImmutableList.of(ConsistencyLevel.ALL, 3, 3),
        ImmutableList.of(ConsistencyLevel.ALL, 5, 5));
  }
}
