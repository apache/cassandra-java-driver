package com.datastax.driver.core;

import java.util.List;

import org.testng.annotations.Test;

import static org.testng.Assert.fail;

import static com.datastax.driver.core.Assertions.assertThat;

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
            .isEqualTo(tokenRange(3, 4));
        assertThat(tokenRange(3, 9).intersectWith(tokenRange(3, 5)))
            .isEqualTo(tokenRange(3, 5));
        assertThat(tokenRange(3, 9).intersectWith(tokenRange(4, 6)))
            .isEqualTo(tokenRange(4, 6));
        assertThat(tokenRange(3, 9).intersectWith(tokenRange(7, 9)))
            .isEqualTo(tokenRange(7, 9));
        assertThat(tokenRange(3, 9).intersectWith(tokenRange(8, 10)))
            .isEqualTo(tokenRange(8, 9));
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
        assertThat(tokenRange(3, 9)).unwrapsToItself();
        assertThat(tokenRange(3, minToken)).unwrapsToItself();
        assertThat(tokenRange(minToken, 3)).unwrapsToItself();
        assertThat(tokenRange(3, 3)).unwrapsToItself();
        assertThat(tokenRange(minToken, minToken)).unwrapsToItself();
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