/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.RegularStatement.ValueDefinition;

public class ValueDefinitionAssert extends AbstractAssert<ValueDefinitionAssert, ValueDefinition> {

    ValueDefinitionAssert(ValueDefinition actual) {
        super(actual, ValueDefinitionAssert.class);
    }

    public ValueDefinitionAssert hasIndex(int expected) {
        assertThat(actual.getIndex()).isEqualTo(expected);
        assertThat(actual.getName()).isNull();
        return this;
    }

    public ValueDefinitionAssert hasName(String expected) {
        assertThat(actual.getName()).isEqualTo(expected);
        assertThat(actual.getIndex()).isEqualTo(-1);
        return this;
    }

    public ValueDefinitionAssert hasType(DataType expected) {
        assertThat(actual.getType()).isEqualTo(expected);
        return this;
    }
}
