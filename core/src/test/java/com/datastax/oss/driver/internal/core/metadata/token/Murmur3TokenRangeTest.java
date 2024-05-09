/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

/** @see TokenRangeTest */
public class Murmur3TokenRangeTest {

  private static final long MIN = -9223372036854775808L;
  private static final long MAX = 9223372036854775807L;

  @Test
  public void should_split_range() {
    assertThat(range(MIN, 4611686018427387904L).splitEvenly(3))
        .containsExactly(
            range(MIN, -4611686018427387904L),
            range(-4611686018427387904L, 0),
            range(0, 4611686018427387904L));
  }

  @Test
  public void should_split_range_that_wraps_around_the_ring() {
    assertThat(range(4611686018427387904L, 0).splitEvenly(3))
        .containsExactly(
            range(4611686018427387904L, -9223372036854775807L),
            range(-9223372036854775807L, -4611686018427387903L),
            range(-4611686018427387903L, 0));
  }

  @Test
  public void should_split_range_when_division_not_integral() {
    assertThat(range(0, 11).splitEvenly(3)).containsExactly(range(0, 4), range(4, 8), range(8, 11));
  }

  @Test
  public void should_split_range_producing_empty_splits() {
    assertThat(range(0, 2).splitEvenly(5))
        .containsExactly(range(0, 1), range(1, 2), range(2, 2), range(2, 2), range(2, 2));
  }

  @Test
  public void should_split_range_producing_empty_splits_near_ring_end() {
    // These are edge cases where we want to make sure we don't accidentally generate the ]min,min]
    // range (which is the whole ring)
    assertThat(range(MAX, MIN).splitEvenly(3))
        .containsExactly(range(MAX, MAX), range(MAX, MAX), range(MAX, MIN));

    assertThat(range(MIN, MIN + 1).splitEvenly(3))
        .containsExactly(range(MIN, MIN + 1), range(MIN + 1, MIN + 1), range(MIN + 1, MIN + 1));
  }

  @Test
  public void should_split_whole_ring() {
    assertThat(range(MIN, MIN).splitEvenly(3))
        .containsExactly(
            range(MIN, -3074457345618258603L),
            range(-3074457345618258603L, 3074457345618258602L),
            range(3074457345618258602L, MIN));
  }

  private Murmur3TokenRange range(long start, long end) {
    return new Murmur3TokenRange(new Murmur3Token(start), new Murmur3Token(end));
  }
}
