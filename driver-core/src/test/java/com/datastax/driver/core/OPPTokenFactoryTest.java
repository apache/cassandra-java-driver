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

import org.testng.annotations.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class OPPTokenFactoryTest {
    private static final Token.Factory factory = Token.OPPToken.FACTORY;

    private static final Token minToken = token(ByteBuffer.allocate(0));

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
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{0});
        Token zero = token(buffer);

        // These are edge cases where we want to make sure we don't accidentally generate the ]min,min] range (which is the whole ring)
        List<Token> splits = factory.split(minToken, zero, 3);
        assertThat(splits).containsExactly(
                zero,
                zero
        );
    }

    @Test(groups = "unit")
    public void should_strip_trailing_0_bytes() {
        Token with0Bytes = token(ByteBuffer.wrap(new byte[]{4, 0, 0, 0}));
        Token without0Bytes = token(ByteBuffer.wrap(new byte[]{4}));
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
        for (int start = 1; start < 128; start++) {
            for (int end = start; end < start; end++) {
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

    private static Token token(char c) {
        return new Token.OPPToken(ByteBuffer.wrap(new byte[]{(byte) c}));
    }

    private static Token token(long i) {
        return new Token.OPPToken(ByteBuffer.wrap(BigInteger.valueOf(i).toByteArray()));
    }

    private static Token token(ByteBuffer buffer) {
        // note - protocol version is ignored in OPPFactory, so we actually don't care about the value
        return factory.deserialize(buffer, ProtocolVersion.NEWEST_SUPPORTED);
    }
}
