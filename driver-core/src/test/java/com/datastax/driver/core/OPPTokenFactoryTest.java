package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class OPPTokenFactoryTest {
    Token.Factory factory = Token.OPPToken.FACTORY;

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
        Token minToken = factory.deserialize(ByteBuffer.allocate(0));
        // the first token following min
        Token zero = factory.deserialize(ByteBuffer.wrap(new byte[]{0}));

        // These are edge cases where we want to make sure we don't accidentally generate the ]min,min] range (which is the whole ring)
        List<Token> splits = factory.split(minToken, zero, 3);
        assertThat(splits).containsExactly(
            zero,
            zero
        );
    }

    private Token token(char c) {
        return new Token.OPPToken(ByteBuffer.wrap(new byte[]{ (byte)c }));
    }
}
