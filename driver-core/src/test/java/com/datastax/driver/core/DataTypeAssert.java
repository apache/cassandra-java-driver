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

import static com.datastax.driver.core.Assertions.assertThat;

public class DataTypeAssert extends AbstractAssert<DataTypeAssert, DataType> {
    public DataTypeAssert(DataType actual) {
        super(actual, DataTypeAssert.class);
    }

    public DataTypeAssert hasName(DataType.Name name) {
        assertThat(actual.name).isEqualTo(name);
        return this;
    }

    public DataTypeAssert isFrozen() {
        assertThat(actual.isFrozen()).isTrue();
        return this;
    }

    public DataTypeAssert isNotFrozen() {
        assertThat(actual.isFrozen()).isFalse();
        return this;
    }

    public DataTypeAssert hasTypeArgument(int position, DataType expected) {
        assertThat(actual.getTypeArguments().get(position)).isEqualTo(expected);
        return this;
    }

    public DataTypeAssert hasTypeArguments(DataType... expected) {
        assertThat(actual.getTypeArguments()).containsExactly(expected);
        return this;
    }
}
