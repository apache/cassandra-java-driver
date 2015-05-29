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
package com.datastax.driver.core.exceptions;

import java.util.Collection;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.utils.CassandraVersion;

@CassandraVersion(major = 2.2)
public class FunctionExecutionExceptionTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            "CREATE TABLE foo (k int primary key)",
            "INSERT INTO foo (k) VALUES (0)",
            "CREATE FUNCTION inverse(i int) RETURNS NULL ON NULL INPUT RETURNS double LANGUAGE java AS 'return 1.0/i;'"
        );
    }

    @Test(groups = "short", expectedExceptions = FunctionExecutionException.class)
    public void should_throw_when_function_execution_fails() {
        session.execute("SELECT inverse(k) FROM foo");
    }
}