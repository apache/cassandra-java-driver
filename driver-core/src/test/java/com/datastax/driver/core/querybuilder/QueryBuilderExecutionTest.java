/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.util.*;

import org.testng.annotations.Test;

import com.datastax.driver.core.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import static org.testng.Assert.*;

public class QueryBuilderExecutionTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final String TABLE1 = "test1";

    @Override
    protected Collection<String> getTableDefinitions() {
        return Arrays.asList(String.format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, TABLE1),
                             "CREATE TABLE dateTest (t timestamp PRIMARY KEY)");
    }

    @Test(groups = "short")
    public void executeTest() throws Exception {

        session.execute(insertInto(TABLE1).value("k", "k1").value("t", "This is a test").value("i", 3).value("f", 0.42));
        session.execute(update(TABLE1).with(set("t", "Another test")).where(eq("k", "k2")));

        List<Row> rows = session.execute(select().from(TABLE1).where(in("k", "k1", "k2"))).all();

        assertEquals(2, rows.size());

        Row r1 = rows.get(0);
        assertEquals("k1", r1.getString("k"));
        assertEquals("This is a test", r1.getString("t"));
        assertEquals(3, r1.getInt("i"));
        assertFalse(r1.isNull("f"));

        Row r2 = rows.get(1);
        assertEquals("k2", r2.getString("k"));
        assertEquals("Another test", r2.getString("t"));
        assertTrue(r2.isNull("i"));
        assertTrue(r2.isNull("f"));
    }

    @Test(groups = "short")
    public void dateHandlingTest() throws Exception {

        Date d = new Date();
        session.execute(insertInto("dateTest").value("t", d));
        String query = select().from("dateTest").where(eq(token("t"), fcall("token", d))).toString();
        List<Row> rows = session.execute(query).all();

        assertEquals(1, rows.size());

        Row r1 = rows.get(0);
        assertEquals(d, r1.getDate("t"));
    }

    @Test(groups = "unit")
    public void prepareTest() throws Exception {

        // Just check we correctly avoid values when there is a bind marker
        String query = "INSERT INTO foo(a,b,c,d) VALUES ('foo','bar',?,0);";
        RegularStatement stmt = insertInto("foo").value("a", "foo").value("b", "bar").value("c", bindMarker()).value("d", 0);
        assertEquals(stmt.getQueryString(), query);

        query = "INSERT INTO foo(a,b,c,d) VALUES ('foo','bar',:c,0);";
        stmt = insertInto("foo").value("a", "foo").value("b", "bar").value("c", bindMarker("c")).value("d", 0);
        assertEquals(stmt.getQueryString(), query);
    }

    @Test(groups = "short")
    public void batchNonBuiltStatementTest() throws Exception {
        SimpleStatement simple = new SimpleStatement("INSERT INTO " + TABLE1 + " (k, t) VALUES ('batchTest1', 'val1')");
        RegularStatement built = insertInto(TABLE1).value("k", "batchTest2").value("t", "val2");
        session.execute(batch().add(simple).add(built));

        List<Row> rows = session.execute(select().from(TABLE1).where(in("k", "batchTest1", "batchTest2"))).all();
        assertEquals(2, rows.size());

        Row r1 = rows.get(0);
        assertEquals("batchTest1", r1.getString("k"));
        assertEquals("val1", r1.getString("t"));

        Row r2 = rows.get(1);
        assertEquals("batchTest2", r2.getString("k"));
        assertEquals("val2", r2.getString("t"));
    }
}
