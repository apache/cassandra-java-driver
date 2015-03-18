package com.datastax.driver.core;

import java.util.Iterator;
import java.util.List;

import org.assertj.core.api.AbstractAssert;

import static com.datastax.driver.core.Assertions.assertThat;

public class TokenRangeAssert extends AbstractAssert<TokenRangeAssert, TokenRange> {
    protected TokenRangeAssert(TokenRange actual) {
        super(actual, TokenRangeAssert.class);
    }

    public TokenRangeAssert startsWith(Token token) {
        assertThat(actual.getStart()).isEqualTo(token);
        return this;
    }

    public TokenRangeAssert endsWith(Token token) {
        assertThat(actual.getEnd()).isEqualTo(token);
        return this;
    }

    public TokenRangeAssert isEmpty() {
        assertThat(actual.isEmpty()).isTrue();
        return this;
    }

    public TokenRangeAssert isNotEmpty() {
        assertThat(actual.isEmpty()).isFalse();
        return this;
    }

    public TokenRangeAssert isWrappedAround() {
        assertThat(actual.isWrappedAround()).isTrue();

        Token.Factory factory = actual.factory;

        List<TokenRange> unwrapped = actual.unwrap();
        assertThat(unwrapped.size())
            .as("%s should unwrap to two ranges, but unwrapped to %s", actual, unwrapped)
            .isEqualTo(2);

        Iterator<TokenRange> unwrappedIt = unwrapped.iterator();
        TokenRange firstRange = unwrappedIt.next();
        assertThat(firstRange).endsWith(factory.minToken());

        TokenRange secondRange = unwrappedIt.next();
        assertThat(secondRange).startsWith(factory.minToken());

        return this;
    }

    public TokenRangeAssert isNotWrappedAround() {
        assertThat(actual.isWrappedAround()).isFalse();
        assertThat(actual.unwrap()).containsExactly(actual);
        return this;
    }

    public TokenRangeAssert unwrapsTo(TokenRange... subRanges) {
        assertThat(actual.unwrap()).containsExactly(subRanges);
        return this;
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

    public TokenRangeAssert doesNotIntersect(TokenRange ... that) {
        for(TokenRange thatRange : that) {
            assertThat(actual.intersects(thatRange))
                .as("%s should not intersect %s", actual, thatRange)
                .isFalse();
            assertThat(thatRange.intersects(actual))
                .as("%s should not intersect %s", thatRange, actual)
                .isFalse();
        }
        return this;
    }
}
