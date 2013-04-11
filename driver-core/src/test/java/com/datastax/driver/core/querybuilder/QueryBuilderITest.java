/*
 *      Copyright (C) 2012 DataStax Inc.
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
package com.datastax.driver.core.querybuilder;

import java.nio.ByteBuffer;
import java.util.*;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.SyntaxError;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class QueryBuilderITest extends CCMBridge.PerClassSingleNodeCluster {

    private static final String TABLE_TEXT = "test_text";
    private static final String TABLE_INT = "test_int";

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList(String.format("CREATE TABLE %s (k text PRIMARY KEY, a int, b int)", TABLE_TEXT),
                             String.format("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)", TABLE_INT));
    }

    @Test(groups = "integration")
    public void remainingDeleteTests() throws Exception {

        Statement query;
        TableMetadata table = cluster.getMetadata().getKeyspace(TestUtils.SIMPLE_KEYSPACE).getTable(TABLE_TEXT);
        assertNotNull(table);

        String expected = "DELETE k FROM ks.test_text;";
        query = delete("k").from(table);
        assertEquals(query.toString(), expected);
        try {
            session.execute(query);
            fail();
        } catch (SyntaxError e) {
            // Missing WHERE clause
        }
    }
}
