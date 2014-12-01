package com.datastax.driver.core;

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

public class TokenRangeAssert extends AbstractAssert<TokenRangeAssert, TokenRange> {
    protected TokenRangeAssert(TokenRange actual) {
        super(actual, TokenRangeAssert.class);
    }

    public TokenRangeAssert intersects(TokenRange that) {
        assertThat(actual.intersects(that))
            .as("%s should intersect %s", actual, that)
            .isTrue();
        assertThat(that.intersects(actual))
            .as("%s should intersect %s", that, actual)
            .isTrue();
        return this;
    }

    public TokenRangeAssert doesNotIntersect(TokenRange that) {
        assertThat(actual.intersects(that))
            .as("%s should not intersect %s", actual, that)
            .isFalse();
        assertThat(that.intersects(actual))
            .as("%s should not intersect %s", that, actual)
            .isFalse();
        return this;
    }

    public TokenRangeAssert unwrapsToItself() {
        assertThat(actual.unwrap()).containsExactly(actual);
        return this;
    }

    public TokenRangeAssert unwrapsTo(TokenRange... subRanges) {
        assertThat(actual.unwrap()).containsExactly(subRanges);
        return this;
    }
}
