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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import org.junit.Test;

public class RandomTokenRangeTest {

  private static final String MIN = "-1";
  private static final String MAX = "170141183460469231731687303715884105728";

  @Test
  public void should_split_range() {
    assertThat(range("0", "127605887595351923798765477786913079296").splitEvenly(3))
        .containsExactly(
            range("0", "42535295865117307932921825928971026432"),
            range(
                "42535295865117307932921825928971026432", "85070591730234615865843651857942052864"),
            range(
                "85070591730234615865843651857942052864",
                "127605887595351923798765477786913079296"));
  }

  @Test
  public void should_split_range_that_wraps_around_the_ring() {
    assertThat(
            range(
                    "127605887595351923798765477786913079296",
                    "85070591730234615865843651857942052864")
                .splitEvenly(3))
        .containsExactly(
            range("127605887595351923798765477786913079296", "0"),
            range("0", "42535295865117307932921825928971026432"),
            range(
                "42535295865117307932921825928971026432",
                "85070591730234615865843651857942052864"));
  }

  @Test
  public void should_split_range_producing_empty_splits_near_ring_end() {
    // These are edge cases where we want to make sure we don't accidentally generate the ]min,min]
    // range (which is the whole ring)
    assertThat(range(MAX, MIN).splitEvenly(3))
        .containsExactly(range(MAX, MAX), range(MAX, MAX), range(MAX, MIN));

    assertThat(range(MIN, "0").splitEvenly(3))
        .containsExactly(range(MIN, "0"), range("0", "0"), range("0", "0"));
  }

  @Test
  public void should_split_whole_ring() {
    assertThat(range(MIN, MIN).splitEvenly(3))
        .containsExactly(
            range(MIN, "56713727820156410577229101238628035242"),
            range(
                "56713727820156410577229101238628035242",
                "113427455640312821154458202477256070485"),
            range("113427455640312821154458202477256070485", MIN));
  }

  private RandomTokenRange range(String start, String end) {
    return new RandomTokenRange(
        new RandomToken(new BigInteger(start)), new RandomToken(new BigInteger(end)));
  }
}
