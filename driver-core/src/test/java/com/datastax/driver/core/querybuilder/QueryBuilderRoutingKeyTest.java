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
import java.util.Arrays;
import java.util.Collection;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import org.testng.annotations.Test;

import com.datastax.driver.core.*;

public class QueryBuilderRoutingKeyTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final String TABLE_TEXT = "test_text";
    private static final String TABLE_INT = "test_int";

    @Override
    protected Collection<String> getTableDefinitions() {
        return Arrays.asList(String.format("CREATE TABLE %s (k text PRIMARY KEY, a int, b int)", TABLE_TEXT),
                             String.format("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)", TABLE_INT));
    }

    @Test(groups = "short")
    public void textRoutingKeyTest() throws Exception {

        Statement query;
        TableMetadata table = cluster.getMetadata().getKeyspace(TestUtils.SIMPLE_KEYSPACE).getTable(TABLE_TEXT);
        assertNotNull(table);

        String txt = "If she weighs the same as a duck... she's made of wood.";
        query = insertInto(table).values(new String[]{"k", "a", "b"}, new Object[]{txt, 1, 2});
        assertEquals(query.getRoutingKey(), ByteBuffer.wrap(txt.getBytes()));
        session.execute(query);

        query = select().from(table).where(eq("k", txt));
        assertEquals(query.getRoutingKey(), ByteBuffer.wrap(txt.getBytes()));
        Row row = session.execute(query).one();
        assertEquals(row.getString("k"), txt);
        assertEquals(row.getInt("a"), 1);
        assertEquals(row.getInt("b"), 2);
    }

    @Test(groups = "short")
    public void intRoutingKeyTest() throws Exception {

        Statement query;
        TableMetadata table = cluster.getMetadata().getKeyspace(TestUtils.SIMPLE_KEYSPACE).getTable(TABLE_INT);
        assertNotNull(table);

        query = insertInto(table).values(new String[]{"k", "a", "b"}, new Object[]{42, 1, 2});
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(0, 42);
        assertEquals(query.getRoutingKey(), bb);
        session.execute(query);

        query = select().from(table).where(eq("k", 42));
        assertEquals(query.getRoutingKey(), bb);
        Row row = session.execute(query).one();
        assertEquals(row.getInt("k"), 42);
        assertEquals(row.getInt("a"), 1);
        assertEquals(row.getInt("b"), 2);
    }

    @Test(groups = "short")
    public void intRoutingBatchKeyTest() throws Exception {

        Statement query;
        TableMetadata table = cluster.getMetadata().getKeyspace(TestUtils.SIMPLE_KEYSPACE).getTable(TABLE_INT);
        assertNotNull(table);

        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(0, 42);

        String batch_query;
        Query batch;
        ResultSet rs;

        query = select().from(table).where(eq("k", 42));

        batch_query = "BEGIN BATCH ";
        batch_query += "INSERT INTO ks.test_int(k,a) VALUES (42,1);";
        batch_query += "UPDATE ks.test_int USING TTL 400;";
        batch_query += "APPLY BATCH;";
        batch = batch()
                .add(insertInto(table).values(new String[]{"k", "a"}, new Object[]{42, 1}))
                .add(update(table).using(ttl(400)));
        assertEquals(batch.getRoutingKey(), bb);
        assertEquals(batch.toString(), batch_query);
        // TODO: rs = session.execute(batch); // Not guaranteed to be valid CQL

        batch_query = "BEGIN BATCH ";
        batch_query += "SELECT * FROM ks.test_int WHERE k=42;";
        batch_query += "APPLY BATCH;";
        batch = batch(query);
        assertEquals(batch.getRoutingKey(), bb);
        assertEquals(batch.toString(), batch_query);
        // TODO: rs = session.execute(batch); // Not guaranteed to be valid CQL

        batch_query = "BEGIN BATCH ";
        batch_query += "SELECT * FROM foo WHERE k=42;";
        batch_query += "APPLY BATCH;";
        batch = batch().add(select().from("foo").where(eq("k", 42)));
        assertEquals(batch.getRoutingKey(), null);
        assertEquals(batch.toString(), batch_query);
        // TODO: rs = session.execute(batch); // Not guaranteed to be valid CQL

        batch_query = "BEGIN BATCH USING TIMESTAMP 42 ";
        batch_query += "INSERT INTO foo.bar(a) VALUES (123);";
        batch_query += "APPLY BATCH;";
        batch = batch().using(timestamp(42)).add(insertInto("foo", "bar").value("a", 123));
        assertEquals(batch.getRoutingKey(), null);
        assertEquals(batch.toString(), batch_query);
        // TODO: rs = session.execute(batch); // Not guaranteed to be valid CQL
    }
}
