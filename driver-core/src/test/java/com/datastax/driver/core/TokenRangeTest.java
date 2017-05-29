/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.testng.Assert.fail;

public class TokenRangeTest {
    // The tests in this class don't depend on the kind of factory used, so use Murmur3 everywhere
    Token.Factory factory = Token.getFactory("Murmur3Partitioner");
    private Token minToken = factory.minToken();

    @Test(groups = "unit")
    public void should_check_intersection() {
        // NB - to make the test more visual, we use watch face numbers
        assertThat(tokenRange(3, 9))
                .doesNotIntersect(tokenRange(11, 1))
                .doesNotIntersect(tokenRange(1, 2))
                .doesNotIntersect(tokenRange(11, 3))
                .doesNotIntersect(tokenRange(2, 3))
                .doesNotIntersect(tokenRange(3, 3))
                .intersects(tokenRange(2, 6))
                .intersects(tokenRange(2, 10))
                .intersects(tokenRange(6, 10))
                .intersects(tokenRange(4, 8))
                .intersects(tokenRange(3, 9))
                .doesNotIntersect(tokenRange(9, 10))
                .doesNotIntersect(tokenRange(10, 11))
        ;
        assertThat(tokenRange(9, 3))
                .doesNotIntersect(tokenRange(5, 7))
                .doesNotIntersect(tokenRange(7, 8))
                .doesNotIntersect(tokenRange(5, 9))
                .doesNotIntersect(tokenRange(8, 9))
                .doesNotIntersect(tokenRange(9, 9))
                .intersects(tokenRange(8, 2))
                .intersects(tokenRange(8, 4))
                .intersects(tokenRange(2, 4))
                .intersects(tokenRange(10, 2))
                .intersects(tokenRange(9, 3))
                .doesNotIntersect(tokenRange(3, 4))
                .doesNotIntersect(tokenRange(4, 5))
        ;
        assertThat(tokenRange(3, 3)).doesNotIntersect(tokenRange(3, 3));

        // Reminder: minToken serves as both lower and upper bound
        assertThat(tokenRange(minToken, 5))
                .doesNotIntersect(tokenRange(6, 7))
                .doesNotIntersect(tokenRange(6, minToken))
                .intersects(tokenRange(6, 4))
                .intersects(tokenRange(2, 4))
                .intersects(tokenRange(minToken, 4))
                .intersects(tokenRange(minToken, 5))
        ;

        assertThat(tokenRange(5, minToken))
                .doesNotIntersect(tokenRange(3, 4))
                .doesNotIntersect(tokenRange(minToken, 4))
                .intersects(tokenRange(6, 7))
                .intersects(tokenRange(4, 1))
                .intersects(tokenRange(6, minToken))
                .intersects(tokenRange(5, minToken))
        ;

        assertThat(tokenRange(minToken, minToken))
                .intersects(tokenRange(3, 4))
                .intersects(tokenRange(3, minToken))
                .intersects(tokenRange(minToken, 3))
                .doesNotIntersect(tokenRange(3, 3))
        ;
    }

    @Test(groups = "unit")
    public void should_compute_intersection() {
        assertThat(tokenRange(3, 9).intersectWith(tokenRange(2, 4)))
                .isEqualTo(ImmutableList.of(tokenRange(3, 4)));
        assertThat(tokenRange(3, 9).intersectWith(tokenRange(3, 5)))
                .isEqualTo(ImmutableList.of(tokenRange(3, 5)));
        assertThat(tokenRange(3, 9).intersectWith(tokenRange(4, 6)))
                .isEqualTo(ImmutableList.of(tokenRange(4, 6)));
        assertThat(tokenRange(3, 9).intersectWith(tokenRange(7, 9)))
                .isEqualTo(ImmutableList.of(tokenRange(7, 9)));
        assertThat(tokenRange(3, 9).intersectWith(tokenRange(8, 10)))
                .isEqualTo(ImmutableList.of(tokenRange(8, 9)));
    }

    @Test(groups = "unit")
    public void should_compute_intersection_with_ranges_around_ring() {
        // If a range wraps the ring like 10, -10 does this will produce two separate
        // intersected ranges.
        assertThat(tokenRange(10, -10).intersectWith(tokenRange(-20, 20)))
                .isEqualTo(ImmutableList.of(tokenRange(10, 20), tokenRange(-20, -10)));
        assertThat(tokenRange(-20, 20).intersectWith(tokenRange(10, -10)))
                .isEqualTo(ImmutableList.of(tokenRange(10, 20), tokenRange(-20, -10)));

        // If both ranges wrap the ring, they should be merged together wrapping across
        // the range.
        assertThat(tokenRange(10, -30).intersectWith(tokenRange(20, -20)))
                .isEqualTo(ImmutableList.of(tokenRange(20, -30)));
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_to_compute_intersection_when_ranges_dont_intersect() {
        tokenRange(1, 2).intersectWith(tokenRange(2, 3));
    }

    @Test(groups = "unit")
    public void should_merge_with_other_range() {
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(2, 3))).isEqualTo(tokenRange(2, 9));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(2, 4))).isEqualTo(tokenRange(2, 9));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(11, 3))).isEqualTo(tokenRange(11, 9));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(11, 4))).isEqualTo(tokenRange(11, 9));

        assertThat(tokenRange(3, 9).mergeWith(tokenRange(4, 8))).isEqualTo(tokenRange(3, 9));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(3, 9))).isEqualTo(tokenRange(3, 9));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(3, 3))).isEqualTo(tokenRange(3, 9));
        assertThat(tokenRange(3, 3).mergeWith(tokenRange(3, 9))).isEqualTo(tokenRange(3, 9));

        assertThat(tokenRange(3, 9).mergeWith(tokenRange(9, 11))).isEqualTo(tokenRange(3, 11));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(8, 11))).isEqualTo(tokenRange(3, 11));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(9, 1))).isEqualTo(tokenRange(3, 1));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(8, 1))).isEqualTo(tokenRange(3, 1));

        assertThat(tokenRange(3, 9).mergeWith(tokenRange(9, 3))).isEqualTo(tokenRange(minToken, minToken));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(9, 4))).isEqualTo(tokenRange(minToken, minToken));
        assertThat(tokenRange(3, 10).mergeWith(tokenRange(9, 4))).isEqualTo(tokenRange(minToken, minToken));

        assertThat(tokenRange(9, 3).mergeWith(tokenRange(8, 9))).isEqualTo(tokenRange(8, 3));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(8, 10))).isEqualTo(tokenRange(8, 3));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(4, 9))).isEqualTo(tokenRange(4, 3));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(4, 10))).isEqualTo(tokenRange(4, 3));

        assertThat(tokenRange(9, 3).mergeWith(tokenRange(10, 2))).isEqualTo(tokenRange(9, 3));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(9, 3))).isEqualTo(tokenRange(9, 3));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(9, 9))).isEqualTo(tokenRange(9, 3));
        assertThat(tokenRange(9, 9).mergeWith(tokenRange(9, 3))).isEqualTo(tokenRange(9, 3));

        assertThat(tokenRange(9, 3).mergeWith(tokenRange(3, 5))).isEqualTo(tokenRange(9, 5));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(2, 5))).isEqualTo(tokenRange(9, 5));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(3, 7))).isEqualTo(tokenRange(9, 7));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(2, 7))).isEqualTo(tokenRange(9, 7));

        assertThat(tokenRange(9, 3).mergeWith(tokenRange(3, 9))).isEqualTo(tokenRange(minToken, minToken));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(3, 10))).isEqualTo(tokenRange(minToken, minToken));

        assertThat(tokenRange(3, 3).mergeWith(tokenRange(3, 3))).isEqualTo(tokenRange(3, 3));

        assertThat(tokenRange(5, minToken).mergeWith(tokenRange(6, 7))).isEqualTo(tokenRange(5, minToken));
        assertThat(tokenRange(5, minToken).mergeWith(tokenRange(minToken, 3))).isEqualTo(tokenRange(5, 3));
        assertThat(tokenRange(5, minToken).mergeWith(tokenRange(3, 5))).isEqualTo(tokenRange(3, minToken));

        assertThat(tokenRange(minToken, 5).mergeWith(tokenRange(2, 3))).isEqualTo(tokenRange(minToken, 5));
        assertThat(tokenRange(minToken, 5).mergeWith(tokenRange(7, minToken))).isEqualTo(tokenRange(7, 5));
        assertThat(tokenRange(minToken, 5).mergeWith(tokenRange(5, 7))).isEqualTo(tokenRange(minToken, 7));
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_not_merge_with_nonadjacent_and_disjoint_ranges() {
        tokenRange(0, 5).mergeWith(tokenRange(7, 14));
    }

    @Test(groups = "unit")
    public void should_return_non_empty_range_if_other_range_is_empty() {
        assertThat(tokenRange(1, 5).mergeWith(tokenRange(5, 5))).isEqualTo(tokenRange(1, 5));
    }

    @Test(groups = "unit")
    public void should_unwrap_to_non_wrapping_ranges() {
        assertThat(tokenRange(9, 3)).unwrapsTo(tokenRange(9, minToken), tokenRange(minToken, 3));
        assertThat(tokenRange(3, 9)).isNotWrappedAround();
        assertThat(tokenRange(3, minToken)).isNotWrappedAround();
        assertThat(tokenRange(minToken, 3)).isNotWrappedAround();
        assertThat(tokenRange(3, 3)).isNotWrappedAround();
        assertThat(tokenRange(minToken, minToken)).isNotWrappedAround();
    }

    @Test(groups = "unit")
    public void should_split_evenly() {
        // Simply exercise splitEvenly, split logic is exercised in TokenFactoryTest implementation for each partitioner.
        List<TokenRange> splits = tokenRange(3, 9).splitEvenly(3);

        assertThat(splits).hasSize(3);
        assertThat(splits).containsExactly(tokenRange(3, 5), tokenRange(5, 7), tokenRange(7, 9));
    }

    @Test(groups = "unit")
    public void should_throw_error_with_less_than_1_splits() {
        for (int i = -255; i < 1; i++) {
            try {
                tokenRange(0, 1).splitEvenly(i);
                fail("Expected error when providing " + i + " splits.");
            } catch (IllegalArgumentException e) {
                // expected.
            }
        }
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_not_split_empty_token_range() {
        tokenRange(0, 0).splitEvenly(1);
    }

    @Test(groups = "unit")
    public void should_create_empty_token_ranges_if_too_many_splits() {
        TokenRange range = tokenRange(0, 10);

        List<TokenRange> ranges = range.splitEvenly(255);
        assertThat(ranges).hasSize(255);

        for (int i = 0; i < ranges.size(); i++) {
            TokenRange tr = ranges.get(i);
            if (i < 10) {
                assertThat(tr).isEqualTo(tokenRange(i, i + 1));
            } else {
                assertThat(tr.isEmpty());
            }
        }
    }

    @Test(groups = "unit")
    public void should_check_if_range_contains_token() {
        // ]1,2] contains 2, but it does not contain the start of ]2,3]
        assertThat(tokenRange(1, 2))
                .contains(newM3PToken(2), false)
                .doesNotContain(newM3PToken(2), true);
        // ]1,2] does not contain 1, but it contains the start of ]1,3]
        assertThat(tokenRange(1, 2))
                .doesNotContain(newM3PToken(1), false)
                .contains(newM3PToken(1), true);

        // ]2,1] contains the start of ]min,5]
        assertThat(tokenRange(2, 1))
                .contains(minToken, true);

        // ]min, 1] does not contain min, but it contains the start of ]min, 2]
        assertThat(tokenRange(minToken, 1))
                .doesNotContain(minToken, false)
                .contains(minToken, true);
        // ]1, min] contains min, but not the start of ]min, 2]
        assertThat(tokenRange(1, minToken))
                .contains(minToken, false)
                .doesNotContain(minToken, true);

        // An empty range contains nothing
        assertThat(tokenRange(1, 1))
                .doesNotContain(newM3PToken(1), true)
                .doesNotContain(newM3PToken(1), false)
                .doesNotContain(minToken, true)
                .doesNotContain(minToken, false);

        // The whole ring contains everything
        assertThat(tokenRange(minToken, minToken))
                .contains(minToken, true)
                .contains(minToken, false)
                .contains(newM3PToken(1), true)
                .contains(newM3PToken(1), false);
    }

    private TokenRange tokenRange(long start, long end) {
        return new TokenRange(newM3PToken(start), newM3PToken(end), factory);
    }

    private TokenRange tokenRange(Token start, long end) {
        return new TokenRange(start, newM3PToken(end), factory);
    }

    private TokenRange tokenRange(long start, Token end) {
        return new TokenRange(newM3PToken(start), end, factory);
    }

    private TokenRange tokenRange(Token start, Token end) {
        return new TokenRange(start, end, factory);
    }

    private Token newM3PToken(long value) {
        return factory.fromString(Long.toString(value));
    }
}