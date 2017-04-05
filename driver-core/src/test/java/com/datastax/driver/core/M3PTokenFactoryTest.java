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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class M3PTokenFactoryTest {
    Token.Factory factory = Token.M3PToken.FACTORY;

    @Test(groups = "unit")
    public void should_split_range() {
        List<Token> splits = factory.split(factory.fromString("-9223372036854775808"), factory.fromString("4611686018427387904"), 3);
        assertThat(splits).containsExactly(
                factory.fromString("-4611686018427387904"),
                factory.fromString("0")
        );
    }

    @Test(groups = "unit")
    public void should_split_range_that_wraps_around_the_ring() {
        List<Token> splits = factory.split(factory.fromString("4611686018427387904"), factory.fromString("0"), 3);
        assertThat(splits).containsExactly(
                factory.fromString("-9223372036854775807"),
                factory.fromString("-4611686018427387903")
        );
    }

    @Test(groups = "unit")
    public void should_split_range_when_division_not_integral() {
        List<Token> splits = factory.split(factory.fromString("0"), factory.fromString("11"), 3);
        assertThat(splits).containsExactly(
                factory.fromString("4"),
                factory.fromString("8")
        );
    }

    @Test(groups = "unit")
    public void should_split_range_producing_empty_splits() {
        List<Token> splits = factory.split(factory.fromString("0"), factory.fromString("2"), 5);
        assertThat(splits).containsExactly(
                factory.fromString("1"),
                factory.fromString("2"),
                factory.fromString("2"),
                factory.fromString("2")
        );
    }

    @Test(groups = "unit")
    public void should_split_range_producing_empty_splits_near_ring_end() {
        Token minToken = factory.fromString("-9223372036854775808");
        Token maxToken = factory.fromString("9223372036854775807");

        // These are edge cases where we want to make sure we don't accidentally generate the ]min,min] range (which is the whole ring)
        List<Token> splits = factory.split(maxToken, minToken, 3);
        assertThat(splits).containsExactly(
                maxToken,
                maxToken
        );

        splits = factory.split(minToken, factory.fromString("-9223372036854775807"), 3);
        assertThat(splits).containsExactly(
                factory.fromString("-9223372036854775807"),
                factory.fromString("-9223372036854775807")
        );
    }

    @Test(groups = "unit")
    public void should_split_whole_ring() {
        List<Token> splits = factory.split(factory.fromString("-9223372036854775808"), factory.fromString("-9223372036854775808"), 3);
        assertThat(splits).containsExactly(
                factory.fromString("-3074457345618258603"),
                factory.fromString("3074457345618258602")
        );
    }

}