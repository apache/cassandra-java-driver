package com.datastax.driver.core;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.fail;

public class OPPTokenFactoryTest {
    private static final Token.Factory factory = Token.OPPToken.FACTORY;

    private static final Token minToken = factory.deserialize(ByteBuffer.allocate(0));

    @Test(groups = "unit")
    public void should_split_range() {
        List<Token> splits = factory.split(token('a'), token('d'), 3);
        assertThat(splits).containsExactly(
            token('b'),
            token('c')
        );
    }

    @Test(groups = "unit")
    public void should_split_range_producing_empty_splits_near_ring_end() {
        // the first token following min
        Token zero = factory.deserialize(ByteBuffer.wrap(new byte[]{0}));

        // These are edge cases where we want to make sure we don't accidentally generate the ]min,min] range (which is the whole ring)
        List<Token> splits = factory.split(minToken, zero, 3);
        assertThat(splits).containsExactly(
            zero,
            zero
        );
    }

    @Test(groups = "unit")
    public void should_strip_trailing_0_bytes() {
        Token with0Bytes = factory.deserialize(ByteBuffer.wrap(new byte[]{4,0,0,0}));
        Token without0Bytes = factory.deserialize(ByteBuffer.wrap(new byte[]{4}));
        Token fromStringWith0Bytes = factory.fromString("040000");

        assertThat(with0Bytes)
            .isEqualTo(without0Bytes)
            .isEqualTo(fromStringWith0Bytes);

        Token withMixed0Bytes = factory.fromString("0004000400");
        Token withoutMixed0Bytes = factory.fromString("00040004");

        assertThat(withMixed0Bytes)
            .isEqualTo(withoutMixed0Bytes);
    }

    @Test(groups = "unit")
    public void should_split_range_that_wraps_around_the_ring() {
        for(int start = 1; start < 128; start++) {
            for(int end = start; end < start; end++) {
                // Take the midpoint of the ring and offset by the midpoint of start+end.
                long expected = 0x81 + ((start + end) / 2);
                TokenRange tr = new TokenRange(token(start), token(end), factory);
                assertThat(factory.split(tr.getStart(), tr.getEnd(), 2))
                    .as("Expected 0x%X for start: %d, end: %d", expected, start, end)
                    .containsExactly(token(expected));
            }
        }
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_not_be_allowed_to_split_with_min_token() {
        factory.split(minToken, minToken, 1);
    }

    private Token token(char c) {
        return new Token.OPPToken(ByteBuffer.wrap(new byte[]{ (byte)c }));
    }

    private Token token(long i) {
        return new Token.OPPToken(ByteBuffer.wrap(BigInteger.valueOf(i).toByteArray()));
    }
}
