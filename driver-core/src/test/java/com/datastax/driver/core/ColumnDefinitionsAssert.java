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

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class ColumnDefinitionsAssert extends AbstractAssert<ColumnDefinitionsAssert, ColumnDefinitions> {

    public ColumnDefinitionsAssert(ColumnDefinitions actual) {
        super(actual, ColumnDefinitionsAssert.class);
    }

    public ColumnDefinitionsAssert hasSize(int expected) {
        assertThat(actual.size()).isEqualTo(expected);
        return this;
    }

    public ColumnDefinitionsAssert containsVariable(String name, DataType type) {
        try {
            assertThat(actual.getType(name)).isEqualTo(type);
        } catch (Exception e) {
            fail(String.format("Expected actual to contain variable %s of type %s, but it did not", name, type), e);
        }
        return this;
    }

    public ColumnDefinitionsAssert doesNotContainVariable(String name) {
        assertThat(actual.getIndexOf(name)).isEqualTo(-1);
        return this;
    }
}
