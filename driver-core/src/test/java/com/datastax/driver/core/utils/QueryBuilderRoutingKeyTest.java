package com.datastax.driver.core.utils;

import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import com.datastax.driver.core.*;
import static com.datastax.driver.core.utils.querybuilder.Assignment.*;
import static com.datastax.driver.core.utils.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.utils.querybuilder.Clause.*;
import static com.datastax.driver.core.utils.querybuilder.Ordering.*;
import static com.datastax.driver.core.utils.querybuilder.Using.*;

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
        query = insert("k", "a", "b").into(table).values(txt, 1, 2);
        assertEquals(ByteBuffer.wrap(txt.getBytes()), query.getRoutingKey());
        session.execute(query);

        query = select().from(table).where(eq("k", txt));
        assertEquals(ByteBuffer.wrap(txt.getBytes()), query.getRoutingKey());
        Row row = session.execute(query).fetchOne();
        assertEquals(txt, row.getString("k"));
        assertEquals(1, row.getInt("a"));
        assertEquals(2, row.getInt("b"));
    }

    @Test
    public void intRoutingKeyTest() throws Exception {

        Statement query;
        TableMetadata table = cluster.getMetadata().getKeyspace(TestUtils.SIMPLE_KEYSPACE).getTable(TABLE_INT);
        assertNotNull(table);

        query = insert("k", "a", "b").into(table).values(42, 1, 2);
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(0, 42);
        assertEquals(bb, query.getRoutingKey());
        session.execute(query);

        query = select().from(table).where(eq("k", 42));
        assertEquals(bb, query.getRoutingKey());
        Row row = session.execute(query).fetchOne();
        assertEquals(42, row.getInt("k"));
        assertEquals(1, row.getInt("a"));
        assertEquals(2, row.getInt("b"));
    }
}
