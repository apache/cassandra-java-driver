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

import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleStatementTest {

    @Test(groups = "unit", expectedExceptions = {IllegalArgumentException.class})
    public void should_fail_if_too_many_variables() {
        List<Object> args = Collections.nCopies(1 << 16, (Object) 1);
        new SimpleStatement("mock query", args.toArray());
    }

    @Test(groups = "unit", expectedExceptions = {IllegalStateException.class})
    public void should_throw_ISE_if_getObject_called_on_statement_without_values() {
        new SimpleStatement("doesn't matter").getObject(0);
    }

    @Test(groups = "unit", expectedExceptions = {IndexOutOfBoundsException.class})
    public void should_throw_IOOBE_if_getObject_called_with_wrong_index() {
        new SimpleStatement("doesn't matter", new Object()).getObject(1);
    }

    @Test(groups = "unit")
    public void should_return_object_at_ith_index() {
        Object expected = new Object();
        Object actual = new SimpleStatement("doesn't matter", expected).getObject(0);
        assertThat(actual).isSameAs(expected);
    }

    @Test(groups = "unit")
    public void should_return_number_of_values() {
        assertThat(
                new SimpleStatement("doesn't matter").valuesCount()
        ).isEqualTo(0);
        assertThat(
                new SimpleStatement("doesn't matter", 1, 2).valuesCount()
        ).isEqualTo(2);
    }

}
