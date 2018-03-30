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
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;

/**
 * Covers the methods that don't depend on the underlying factory (we use Murmur3 as the
 * implementation here).
 *
 * @see Murmur3TokenRangeTest
 * @see ByteOrderedTokenRangeTest
 * @see RandomTokenRangeTest
 */
public class TokenRangeTest {

  private Murmur3Token min = Murmur3TokenFactory.MIN_TOKEN;

  @Test
  public void should_check_intersection() {
    // NB - to make the test more visual, we use watch face numbers
    assertThat(range(3, 9))
        .doesNotIntersect(range(11, 1))
        .doesNotIntersect(range(1, 2))
        .doesNotIntersect(range(11, 3))
        .doesNotIntersect(range(2, 3))
        .doesNotIntersect(range(3, 3))
        .intersects(range(2, 6))
        .intersects(range(2, 10))
        .intersects(range(6, 10))
        .intersects(range(4, 8))
        .intersects(range(3, 9))
        .doesNotIntersect(range(9, 10))
        .doesNotIntersect(range(10, 11));
    assertThat(range(9, 3))
        .doesNotIntersect(range(5, 7))
        .doesNotIntersect(range(7, 8))
        .doesNotIntersect(range(5, 9))
        .doesNotIntersect(range(8, 9))
        .doesNotIntersect(range(9, 9))
        .intersects(range(8, 2))
        .intersects(range(8, 4))
        .intersects(range(2, 4))
        .intersects(range(10, 2))
        .intersects(range(9, 3))
        .doesNotIntersect(range(3, 4))
        .doesNotIntersect(range(4, 5));
    assertThat(range(3, 3)).doesNotIntersect(range(3, 3));

    // Reminder: minToken serves as both lower and upper bound
    assertThat(minTo(5))
        .doesNotIntersect(range(6, 7))
        .doesNotIntersect(toMax(6))
        .intersects(range(6, 4))
        .intersects(range(2, 4))
        .intersects(minTo(4))
        .intersects(minTo(5));

    assertThat(toMax(5))
        .doesNotIntersect(range(3, 4))
        .doesNotIntersect(minTo(4))
        .intersects(range(6, 7))
        .intersects(range(4, 1))
        .intersects(toMax(6))
        .intersects(toMax(5));

    assertThat(fullRing())
        .intersects(range(3, 4))
        .intersects(toMax(3))
        .intersects(minTo(3))
        .doesNotIntersect(range(3, 3));
  }

  @Test
  public void should_compute_intersection() {
    assertThat(range(3, 9).intersectWith(range(2, 4))).isEqualTo(ImmutableList.of(range(3, 4)));
    assertThat(range(3, 9).intersectWith(range(3, 5))).isEqualTo(ImmutableList.of(range(3, 5)));
    assertThat(range(3, 9).intersectWith(range(4, 6))).isEqualTo(ImmutableList.of(range(4, 6)));
    assertThat(range(3, 9).intersectWith(range(7, 9))).isEqualTo(ImmutableList.of(range(7, 9)));
    assertThat(range(3, 9).intersectWith(range(8, 10))).isEqualTo(ImmutableList.of(range(8, 9)));
  }

  @Test
  public void should_compute_intersection_with_ranges_around_ring() {
    // If a range wraps the ring (like 10, -10 does) this will produce two separate intersected
    // ranges.
    assertThat(range(10, -10).intersectWith(range(-20, 20)))
        .isEqualTo(ImmutableList.of(range(10, 20), range(-20, -10)));
    assertThat(range(-20, 20).intersectWith(range(10, -10)))
        .isEqualTo(ImmutableList.of(range(10, 20), range(-20, -10)));

    // If both ranges wrap the ring, they should be merged together wrapping across the range.
    assertThat(range(10, -30).intersectWith(range(20, -20)))
        .isEqualTo(ImmutableList.of(range(20, -30)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_compute_intersection_when_ranges_dont_intersect() {
    range(1, 2).intersectWith(range(2, 3));
  }

  @Test
  public void should_merge_with_other_range() {
    assertThat(range(3, 9).mergeWith(range(2, 3))).isEqualTo(range(2, 9));
    assertThat(range(3, 9).mergeWith(range(2, 4))).isEqualTo(range(2, 9));
    assertThat(range(3, 9).mergeWith(range(11, 3))).isEqualTo(range(11, 9));
    assertThat(range(3, 9).mergeWith(range(11, 4))).isEqualTo(range(11, 9));

    assertThat(range(3, 9).mergeWith(range(4, 8))).isEqualTo(range(3, 9));
    assertThat(range(3, 9).mergeWith(range(3, 9))).isEqualTo(range(3, 9));
    assertThat(range(3, 9).mergeWith(range(3, 3))).isEqualTo(range(3, 9));
    assertThat(range(3, 3).mergeWith(range(3, 9))).isEqualTo(range(3, 9));

    assertThat(range(3, 9).mergeWith(range(9, 11))).isEqualTo(range(3, 11));
    assertThat(range(3, 9).mergeWith(range(8, 11))).isEqualTo(range(3, 11));
    assertThat(range(3, 9).mergeWith(range(9, 1))).isEqualTo(range(3, 1));
    assertThat(range(3, 9).mergeWith(range(8, 1))).isEqualTo(range(3, 1));

    assertThat(range(3, 9).mergeWith(range(9, 3))).isEqualTo(fullRing());
    assertThat(range(3, 9).mergeWith(range(9, 4))).isEqualTo(fullRing());
    assertThat(range(3, 10).mergeWith(range(9, 4))).isEqualTo(fullRing());

    assertThat(range(9, 3).mergeWith(range(8, 9))).isEqualTo(range(8, 3));
    assertThat(range(9, 3).mergeWith(range(8, 10))).isEqualTo(range(8, 3));
    assertThat(range(9, 3).mergeWith(range(4, 9))).isEqualTo(range(4, 3));
    assertThat(range(9, 3).mergeWith(range(4, 10))).isEqualTo(range(4, 3));

    assertThat(range(9, 3).mergeWith(range(10, 2))).isEqualTo(range(9, 3));
    assertThat(range(9, 3).mergeWith(range(9, 3))).isEqualTo(range(9, 3));
    assertThat(range(9, 3).mergeWith(range(9, 9))).isEqualTo(range(9, 3));
    assertThat(range(9, 9).mergeWith(range(9, 3))).isEqualTo(range(9, 3));

    assertThat(range(9, 3).mergeWith(range(3, 5))).isEqualTo(range(9, 5));
    assertThat(range(9, 3).mergeWith(range(2, 5))).isEqualTo(range(9, 5));
    assertThat(range(9, 3).mergeWith(range(3, 7))).isEqualTo(range(9, 7));
    assertThat(range(9, 3).mergeWith(range(2, 7))).isEqualTo(range(9, 7));

    assertThat(range(9, 3).mergeWith(range(3, 9))).isEqualTo(fullRing());
    assertThat(range(9, 3).mergeWith(range(3, 10))).isEqualTo(fullRing());

    assertThat(range(3, 3).mergeWith(range(3, 3))).isEqualTo(range(3, 3));

    assertThat(toMax(5).mergeWith(range(6, 7))).isEqualTo(toMax(5));
    assertThat(toMax(5).mergeWith(minTo(3))).isEqualTo(range(5, 3));
    assertThat(toMax(5).mergeWith(range(3, 5))).isEqualTo(toMax(3));

    assertThat(minTo(5).mergeWith(range(2, 3))).isEqualTo(minTo(5));
    assertThat(minTo(5).mergeWith(toMax(7))).isEqualTo(range(7, 5));
    assertThat(minTo(5).mergeWith(range(5, 7))).isEqualTo(minTo(7));
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_not_merge_with_nonadjacent_and_disjoint_ranges() {
    range(0, 5).mergeWith(range(7, 14));
  }

  @Test
  public void should_return_non_empty_range_if_other_range_is_empty() {
    assertThat(range(1, 5).mergeWith(range(5, 5))).isEqualTo(range(1, 5));
  }

  @Test
  public void should_unwrap_to_non_wrapping_ranges() {
    assertThat(range(9, 3)).unwrapsTo(toMax(9), minTo(3));
    assertThat(range(3, 9)).isNotWrappedAround();
    assertThat(toMax(3)).isNotWrappedAround();
    assertThat(minTo(3)).isNotWrappedAround();
    assertThat(range(3, 3)).isNotWrappedAround();
    assertThat(fullRing()).isNotWrappedAround();
  }

  @Test
  public void should_split_evenly() {
    // Simply exercise splitEvenly, split logic is exercised in the test of each TokenRange
    // implementation
    List<TokenRange> splits = range(3, 9).splitEvenly(3);

    assertThat(splits).hasSize(3);
    assertThat(splits).containsExactly(range(3, 5), range(5, 7), range(7, 9));
  }

  @Test
  public void should_throw_error_with_less_than_1_splits() {
    for (int i = -255; i < 1; i++) {
      try {
        range(0, 1).splitEvenly(i);
        fail("Expected error when providing " + i + " splits.");
      } catch (IllegalArgumentException e) {
        // expected.
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_not_split_empty_token_range() {
    range(0, 0).splitEvenly(1);
  }

  @Test
  public void should_create_empty_token_ranges_if_too_many_splits() {
    TokenRange range = range(0, 10);

    List<TokenRange> ranges = range.splitEvenly(255);
    assertThat(ranges).hasSize(255);

    for (int i = 0; i < ranges.size(); i++) {
      TokenRange tr = ranges.get(i);
      if (i < 10) {
        assertThat(tr).isEqualTo(range(i, i + 1));
      } else {
        assertThat(tr.isEmpty());
      }
    }
  }

  @Test
  public void should_check_if_range_contains_token() {
    // ]1,2] contains 2, but it does not contain the start of ]2,3]
    assertThat(range(1, 2))
        .contains(new Murmur3Token(2), false)
        .doesNotContain(new Murmur3Token(2), true);
    // ]1,2] does not contain 1, but it contains the start of ]1,3]
    assertThat(range(1, 2))
        .doesNotContain(new Murmur3Token(1), false)
        .contains(new Murmur3Token(1), true);

    // ]2,1] contains the start of ]min,5]
    assertThat(range(2, 1)).contains(min, true);

    // ]min, 1] does not contain min, but it contains the start of ]min, 2]
    assertThat(minTo(1)).doesNotContain(min, false).contains(min, true);
    // ]1, min] contains min, but not the start of ]min, 2]
    assertThat(toMax(1)).contains(min, false).doesNotContain(min, true);

    // An empty range contains nothing
    assertThat(range(1, 1))
        .doesNotContain(new Murmur3Token(1), true)
        .doesNotContain(new Murmur3Token(1), false)
        .doesNotContain(min, true)
        .doesNotContain(min, false);

    // The whole ring contains everything
    assertThat(fullRing())
        .contains(min, true)
        .contains(min, false)
        .contains(new Murmur3Token(1), true)
        .contains(new Murmur3Token(1), false);
  }

  private TokenRange range(long start, long end) {
    return new Murmur3TokenRange(new Murmur3Token(start), new Murmur3Token(end));
  }

  private TokenRange minTo(long end) {
    return new Murmur3TokenRange(min, new Murmur3Token(end));
  }

  private TokenRange toMax(long start) {
    return new Murmur3TokenRange(new Murmur3Token(start), min);
  }

  private TokenRange fullRing() {
    return new Murmur3TokenRange(Murmur3TokenFactory.MIN_TOKEN, Murmur3TokenFactory.MIN_TOKEN);
  }
}
