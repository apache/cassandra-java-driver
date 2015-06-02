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

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

public class SimpleStatementTest {
    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void should_fail_if_too_many_variables() {
        List<Object> args = Collections.nCopies(1 << 16, (Object)1);
        new SimpleStatement("mock query", args.toArray());
    }
}