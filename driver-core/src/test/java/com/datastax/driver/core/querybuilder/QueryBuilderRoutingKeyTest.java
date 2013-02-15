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

import org.junit.Test;
import static org.junit.Assert.*;

import com.datastax.driver.core.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class QueryBuilderRoutingKeyTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final String TABLE_TEXT = "test_text";
    private static final String TABLE_INT = "test_int";

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList(String.format("CREATE TABLE %s (k text PRIMARY KEY, a int, b int)", TABLE_TEXT),
                             String.format("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)", TABLE_INT));
    }

    @Test
    public void textRoutingKeyTest() throws Exception {

        Statement query;
        TableMetadata table = cluster.getMetadata().getKeyspace(TestUtils.SIMPLE_KEYSPACE).getTable(TABLE_TEXT);
        assertNotNull(table);

        String txt = "If she weighs the same as a duck... she's made of wood.";
        query = insertInto(table).values(new String[]{"k", "a", "b"}, new Object[]{txt, 1, 2});
        assertEquals(ByteBuffer.wrap(txt.getBytes()), query.getRoutingKey());
        session.execute(query);

        query = select().from(table).where(eq("k", txt));
        assertEquals(ByteBuffer.wrap(txt.getBytes()), query.getRoutingKey());
        Row row = session.execute(query).one();
        assertEquals(txt, row.getString("k"));
        assertEquals(1, row.getInt("a"));
        assertEquals(2, row.getInt("b"));
    }

    @Test
    public void intRoutingKeyTest() throws Exception {

        Statement query;
        TableMetadata table = cluster.getMetadata().getKeyspace(TestUtils.SIMPLE_KEYSPACE).getTable(TABLE_INT);
        assertNotNull(table);

        query = insertInto(table).values(new String[]{"k", "a", "b"}, new Object[]{42, 1, 2});
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(0, 42);
        assertEquals(bb, query.getRoutingKey());
        session.execute(query);

        query = select().from(table).where(eq("k", 42));
        assertEquals(bb, query.getRoutingKey());
        Row row = session.execute(query).one();
        assertEquals(42, row.getInt("k"));
        assertEquals(1, row.getInt("a"));
        assertEquals(2, row.getInt("b"));
    }
}
