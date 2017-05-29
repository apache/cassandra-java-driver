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

import com.datastax.driver.core.utils.Bytes;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class RPTokenFactoryTest {
    Token.Factory factory = Token.RPToken.FACTORY;

    @Test(groups = "unit")
    public void should_hash_consistently() {
        ByteBuffer byteBuffer = Bytes.fromHexString("0xCAFEBABE");
        Token tokenA = factory.hash(byteBuffer);
        Token tokenB = factory.hash(byteBuffer);
        assertThat(tokenA)
                .isEqualTo(factory.fromString("59959303159920881837560881824507314222"))
                .isEqualTo(tokenB);
    }

    @Test(groups = "unit")
    public void should_split_range() {
        List<Token> splits = factory.split(factory.fromString("0"), factory.fromString("127605887595351923798765477786913079296"), 3);
        assertThat(splits).containsExactly(
                factory.fromString("42535295865117307932921825928971026432"),
                factory.fromString("85070591730234615865843651857942052864")
        );
    }

    @Test(groups = "unit")
    public void should_split_range_that_wraps_around_the_ring() {
        List<Token> splits = factory.split(
                factory.fromString("127605887595351923798765477786913079296"),
                factory.fromString("85070591730234615865843651857942052864"),
                3);

        assertThat(splits).containsExactly(
                factory.fromString("0"),
                factory.fromString("42535295865117307932921825928971026432")
        );
    }

    @Test(groups = "unit")
    public void should_split_range_producing_empty_splits_near_ring_end() {
        Token minToken = factory.fromString("-1");
        Token maxToken = factory.fromString("170141183460469231731687303715884105728");

        // These are edge cases where we want to make sure we don't accidentally generate the ]min,min] range (which is the whole ring)
        List<Token> splits = factory.split(maxToken, minToken, 3);
        assertThat(splits).containsExactly(
                maxToken,
                maxToken
        );

        splits = factory.split(minToken, factory.fromString("0"), 3);
        assertThat(splits).containsExactly(
                factory.fromString("0"),
                factory.fromString("0")
        );
    }

    @Test(groups = "unit")
    public void should_split_whole_ring() {
        List<Token> splits = factory.split(factory.fromString("-1"), factory.fromString("-1"), 3);
        assertThat(splits).containsExactly(
                factory.fromString("56713727820156410577229101238628035242"),
                factory.fromString("113427455640312821154458202477256070485")
        );
    }
}
