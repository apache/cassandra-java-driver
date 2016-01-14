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
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.*;
import org.testng.annotations.Test;

import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import static org.testng.Assert.*;

public class QueryBuilderExecutionTest extends CCMTestsSupport {

    private static final String TABLE1 = "test1";

    @Override
    public Collection<String> createTestFixtures() {
        return Arrays.asList(
                String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", TABLE1),
                "CREATE TABLE dateTest (t timestamp PRIMARY KEY)",
                "CREATE TABLE test_coll (k int PRIMARY KEY, a list<int>, b map<int,text>, c set<text>)");
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
        assertEquals(d, r1.getTimestamp("t"));
    }

    @Test(groups = "short")
    public void prepareTest() throws Exception {
        // Just check we correctly avoid values when there is a bind marker
        String query = "INSERT INTO foo (a,b,c,d) VALUES ('foo','bar',?,0);";
        BuiltStatement stmt = insertInto("foo").value("a", "foo").value("b", "bar").value("c", bindMarker()).value("d", 0);
        assertEquals(stmt.getQueryString(), query);

        query = "INSERT INTO foo (a,b,c,d) VALUES ('foo','bar',:c,0);";
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

    @Test(groups = "short")
    public void should_delete_list_element() throws Exception {
        //given
        session.execute("INSERT INTO test_coll (k, a, b) VALUES (1, [1,2,3], null)");
        //when
        BuiltStatement statement = delete().listElt("a", 1).from("test_coll").where(eq("k", 1));
        session.execute(statement);
        //then
        List<Integer> actual = session.execute("SELECT a FROM test_coll WHERE k = 1").one().getList("a", Integer.class);
        assertThat(actual).containsExactly(1, 3);
    }

    @Test(groups = "short")
    public void should_delete_list_element_with_bind_marker() throws Exception {
        //given
        session.execute("INSERT INTO test_coll (k, a) VALUES (1, [1,2,3])");
        //when
        BuiltStatement statement = delete().listElt("a", bindMarker()).from("test_coll").where(eq("k", 1));
        PreparedStatement ps = session.prepare(statement);
        session.execute(ps.bind(1));
        //then
        List<Integer> actual = session.execute("SELECT a FROM test_coll WHERE k = 1").one().getList("a", Integer.class);
        assertThat(actual).containsExactly(1, 3);
    }

    @Test(groups = "short")
    public void should_delete_set_element() throws Exception {
        //given
        session.execute("INSERT INTO test_coll (k, c) VALUES (1, {'foo','bar','qix'})");
        //when
        BuiltStatement statement = delete().setElt("c", "foo").from("test_coll").where(eq("k", 1));
        session.execute(statement);
        //then
        Set<String> actual = session.execute("SELECT c FROM test_coll WHERE k = 1").one().getSet("c", String.class);
        assertThat(actual).containsOnly("bar", "qix");
    }

    @Test(groups = "short")
    public void should_delete_set_element_with_bind_marker() throws Exception {
        //given
        session.execute("INSERT INTO test_coll (k, c) VALUES (1, {'foo','bar','qix'})");
        //when
        BuiltStatement statement = delete().setElt("c", bindMarker()).from("test_coll").where(eq("k", 1));
        PreparedStatement ps = session.prepare(statement);
        session.execute(ps.bind("foo"));
        //then
        Set<String> actual = session.execute("SELECT c FROM test_coll WHERE k = 1").one().getSet("c", String.class);
        assertThat(actual).containsOnly("bar", "qix");
    }

    @Test(groups = "short")
    public void should_delete_map_entry() throws Exception {
        //given
        session.execute("INSERT INTO test_coll (k, b) VALUES (1, {1:'foo', 2:'bar'})");
        //when
        BuiltStatement statement = delete().mapElt("b", 1).from("test_coll").where(eq("k", 1));
        session.execute(statement);
        //then
        Map<Integer, String> actual = session.execute("SELECT b FROM test_coll WHERE k = 1").one().getMap("b", Integer.class, String.class);
        assertThat(actual).containsExactly(entry(2, "bar"));
    }

    @Test(groups = "short")
    public void should_delete_map_entry_with_bind_marker() throws Exception {
        //given
        session.execute("INSERT INTO test_coll (k, a, b) VALUES (1, null, {1:'foo', 2:'bar'})");
        //when
        BuiltStatement statement = delete().mapElt("b", bindMarker()).from("test_coll").where(eq("k", 1));
        PreparedStatement ps = session.prepare(statement);
        session.execute(ps.bind().setInt(0, 1));
        //then
        Map<Integer, String> actual = session.execute("SELECT b FROM test_coll WHERE k = 1").one().getMap("b", Integer.class, String.class);
        assertThat(actual).containsExactly(entry(2, "bar"));
    }

}
