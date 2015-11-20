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

import java.util.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;

import com.datastax.driver.core.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class QueryBuilderExecutionTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final String TABLE1 = "test1";

    private QueryBuilder builder;

    @Override
    protected Collection<String> getTableDefinitions() {
        return Arrays.asList(
            String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", TABLE1),
            "CREATE TABLE dateTest (t timestamp PRIMARY KEY)",
            "CREATE TABLE test_coll (k int PRIMARY KEY, a list<int>, b map<int,text>, c set<text>)");
    }

    @BeforeMethod(groups = "short")
    public void setUpQueryBuilder() throws Exception {
        builder = new QueryBuilder(cluster);
    }

    @Test(groups = "short")
    public void executeTest() throws Exception {

        session.execute(builder.insertInto(TABLE1).value("k", "k1").value("t", "This is a test").value("i", 3).value("f", 0.42));
        session.execute(builder.update(TABLE1).with(set("t", "Another test")).where(eq("k", "k2")));

        List<Row> rows = session.execute(builder.select().from(TABLE1).where(in("k", "k1", "k2"))).all();

        assertThat(rows).hasSize(2);

        Row r1 = rows.get(0);
        assertThat(r1.getString("k")).isEqualTo("k1");
        assertThat(r1.getString("t")).isEqualTo("This is a test");
        assertThat(r1.getInt("i")).isEqualTo(3);
        assertThat(r1.isNull("f")).isFalse();

        Row r2 = rows.get(1);
        assertThat(r2.getString("k")).isEqualTo("k2");
        assertThat(r2.getString("t")).isEqualTo("Another test");
        assertThat(r2.isNull("i")).isTrue();
        assertThat(r2.isNull("f")).isTrue();
    }

    @Test(groups = "short")
    public void dateHandlingTest() throws Exception {

        Date d = new Date();
        session.execute(builder.insertInto("dateTest").value("t", d));
        String query = builder.select().from("dateTest").where(eq(token("t"), fcall("token", d))).toString();
        List<Row> rows = session.execute(query).all();

        assertThat(rows).hasSize(1);

        Row r1 = rows.get(0);
        assertThat(r1.getTimestamp("t")).isEqualTo(d);
    }

    @Test(groups = "short")
    public void prepareTest() throws Exception {
        // Just check we correctly avoid values when there is a bind marker
        String query = "INSERT INTO foo (a,b,c,d) VALUES ('foo','bar',?,0);";
        BuiltStatement stmt = builder.insertInto("foo").value("a", "foo").value("b", "bar").value("c", bindMarker()).value("d", 0);
        assertThat(query).isEqualTo(stmt.getQueryString());

        query = "INSERT INTO foo (a,b,c,d) VALUES ('foo','bar',:c,0);";
        stmt = builder.insertInto("foo").value("a", "foo").value("b", "bar").value("c", bindMarker("c")).value("d", 0);
        assertThat(query).isEqualTo(stmt.getQueryString());
    }

    @Test(groups = "short")
    public void should_create_bind_markers_when_no_statement_contains_bind_markers() throws Exception {
        SimpleStatement simple = session.newSimpleStatement("INSERT INTO " + TABLE1 + " (k,t) VALUES ('batchTest1','val1')");
        RegularStatement built = builder.insertInto(TABLE1).value("k", "batchTest2").value("t", "val2");
        Batch batch = builder.batch().add(simple).add(built);
        // batch has a non built statement, so no bind markers are generated
        assertThat(batch.getValueDefinitions()).isEmpty();
        assertThat(batch.getQueryString()).isEqualTo("BEGIN BATCH INSERT INTO test1 (k,t) VALUES ('batchTest1','val1'); INSERT INTO test1 (k,t) VALUES ('batchTest2','val2'); APPLY BATCH;");
        session.execute(batch);

        List<Row> rows = session.execute(builder.select().from(TABLE1).where(in("k", "batchTest1", "batchTest2"))).all();
        assertThat(rows).hasSize(2);

        Row r1 = rows.get(0);
        assertThat(r1.getString("k")).isEqualTo("batchTest1");
        assertThat(r1.getString("t")).isEqualTo("val1");

        Row r2 = rows.get(1);
        assertThat(r2.getString("k")).isEqualTo("batchTest2");
        assertThat(r2.getString("t")).isEqualTo("val2");
    }

    @Test(groups = "short")
    public void should_not_create_additional_bind_markers_when_statement_already_contains_bind_markers() throws Exception {
        SimpleStatement simple = session.newSimpleStatement("INSERT INTO " + TABLE1 + " (k,t) VALUES ('batchTest1',?)", "val1");
        RegularStatement built = builder.insertInto(TABLE1).value("k", "batchTest2").value("t", "val2");
        Batch batch = builder.batch().add(simple).add(built);
        // batch has a non built statement, so no additional bind markers are generated
        assertThat(batch.getValueDefinitions()).hasSize(1);
        assertThat(batch.getQueryString()).isEqualTo("BEGIN BATCH INSERT INTO test1 (k,t) VALUES ('batchTest1',?); INSERT INTO test1 (k,t) VALUES ('batchTest2','val2'); APPLY BATCH;");
        session.execute(batch);

        List<Row> rows = session.execute(builder.select().from(TABLE1).where(in("k", "batchTest1", "batchTest2"))).all();
        assertThat(rows).hasSize(2);

        Row r1 = rows.get(0);
        assertThat(r1.getString("k")).isEqualTo("batchTest1");
        assertThat(r1.getString("t")).isEqualTo("val1");

        Row r2 = rows.get(1);
        assertThat(r2.getString("k")).isEqualTo("batchTest2");
        assertThat(r2.getString("t")).isEqualTo("val2");
    }

    @Test(groups = "short")
    public void should_delete_list_element() throws Exception {
        //given
        session.execute("INSERT INTO test_coll (k, a, b) VALUES (1, [1,2,3], null)");
        //when
        BuiltStatement statement = builder.delete().listElt("a", 1).from("test_coll").where(eq("k", 1));
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
        BuiltStatement statement = builder.delete().listElt("a", bindMarker()).from("test_coll").where(eq("k", 1));
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
        BuiltStatement statement = builder.delete().setElt("c", "foo").from("test_coll").where(eq("k", 1));
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
        BuiltStatement statement = builder.delete().setElt("c", bindMarker()).from("test_coll").where(eq("k", 1));
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
        BuiltStatement statement = builder.delete().mapElt("b", 1).from("test_coll").where(eq("k", 1));
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
        BuiltStatement statement = builder.delete().mapElt("b", bindMarker()).from("test_coll").where(eq("k", 1));
        PreparedStatement ps = session.prepare(statement);
        session.execute(ps.bind().setInt(0, 1));
        //then
        Map<Integer, String> actual = session.execute("SELECT b FROM test_coll WHERE k = 1").one().getMap("b", Integer.class, String.class);
        assertThat(actual).containsExactly(entry(2, "bar"));
    }

}
