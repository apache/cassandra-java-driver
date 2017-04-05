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

public class LocalDateAssert extends AbstractAssert<LocalDateAssert, LocalDate> {
    public LocalDateAssert(LocalDate actual) {
        super(actual, LocalDateAssert.class);
    }

    public LocalDateAssert hasDaysSinceEpoch(int expected) {
        assertThat(actual.getDaysSinceEpoch()).isEqualTo(expected);
        return this;
    }

    public LocalDateAssert hasMillisSinceEpoch(long expected) {
        assertThat(actual.getMillisSinceEpoch()).isEqualTo(expected);
        return this;
    }

    public LocalDateAssert hasYearMonthDay(int expectedYear, int expectedMonth, int expectedDay) {
        assertThat(actual.getYear()).isEqualTo(expectedYear);
        assertThat(actual.getMonth()).isEqualTo(expectedMonth);
        assertThat(actual.getDay()).isEqualTo(expectedDay);
        return this;
    }

    public LocalDateAssert hasToString(String expected) {
        assertThat(actual.toString()).isEqualTo(expected);
        return this;
    }
}
