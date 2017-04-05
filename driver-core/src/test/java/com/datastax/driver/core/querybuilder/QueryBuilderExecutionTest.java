/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.CassandraVersion;
import org.assertj.core.api.iterable.Extractor;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ResultSetAssert.row;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createTable;
import static org.assertj.core.data.MapEntry.entry;
import static org.testng.Assert.*;

public class QueryBuilderExecutionTest extends CCMTestsSupport {

    private static final String TABLE1 = "test1";
    private static final String TABLE2 = "test2";

    @Override
    public void onTestContextInitialized() {
        execute(
                String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", TABLE1),
                String.format("CREATE TABLE %s (k text, t text, i int, f float, PRIMARY KEY (k, t))", TABLE2),
                "CREATE TABLE dateTest (t timestamp PRIMARY KEY)",
                "CREATE TABLE test_coll (k int PRIMARY KEY, a list<int>, b map<int,text>, c set<text>)",
                "CREATE TABLE test_ppl (a int, b int, c int, PRIMARY KEY (a, b))",
                insertInto(TABLE2).value("k", "cast_t").value("t", "a").value("i", 1).value("f", 1.1).toString(),
                insertInto(TABLE2).value("k", "cast_t").value("t", "b").value("i", 2).value("f", 2.5).toString(),
                insertInto(TABLE2).value("k", "cast_t").value("t", "c").value("i", 3).value("f", 3.7).toString(),
                insertInto(TABLE2).value("k", "cast_t").value("t", "d").value("i", 4).value("f", 5.0).toString()
        );
        // for per partition limit tests
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                session().execute(String.format("INSERT INTO test_ppl (a, b, c) VALUES (%d, %d, %d)", i, j, j));
            }
        }
    }

    @Test(groups = "short")
    public void executeTest() throws Exception {

        session().execute(insertInto(TABLE1).value("k", "k1").value("t", "This is a test").value("i", 3).value("f", 0.42));
        session().execute(update(TABLE1).with(set("t", "Another test")).where(eq("k", "k2")));

        List<Row> rows = session().execute(select().from(TABLE1).where(in("k", "k1", "k2"))).all();

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
        session().execute(insertInto("dateTest").value("t", d));
        String query = select().from("dateTest").where(eq(token("t"), fcall("token", d))).toString();
        List<Row> rows = session().execute(query).all();

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
        session().execute(batch().add(simple).add(built));

        List<Row> rows = session().execute(select().from(TABLE1).where(in("k", "batchTest1", "batchTest2"))).all();
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
        session().execute("INSERT INTO test_coll (k, a, b) VALUES (1, [1,2,3], null)");
        //when
        BuiltStatement statement = delete().listElt("a", 1).from("test_coll").where(eq("k", 1));
        session().execute(statement);
        //then
        List<Integer> actual = session().execute("SELECT a FROM test_coll WHERE k = 1").one().getList("a", Integer.class);
        assertThat(actual).containsExactly(1, 3);
    }

    @Test(groups = "short")
    public void should_delete_list_element_with_bind_marker() throws Exception {
        //given
        session().execute("INSERT INTO test_coll (k, a) VALUES (1, [1,2,3])");
        //when
        BuiltStatement statement = delete().listElt("a", bindMarker()).from("test_coll").where(eq("k", 1));
        PreparedStatement ps = session().prepare(statement);
        session().execute(ps.bind(1));
        //then
        List<Integer> actual = session().execute("SELECT a FROM test_coll WHERE k = 1").one().getList("a", Integer.class);
        assertThat(actual).containsExactly(1, 3);
    }

    @Test(groups = "short")
    public void should_delete_set_element() throws Exception {
        //given
        session().execute("INSERT INTO test_coll (k, c) VALUES (1, {'foo','bar','qix'})");
        //when
        BuiltStatement statement = delete().setElt("c", "foo").from("test_coll").where(eq("k", 1));
        session().execute(statement);
        //then
        Set<String> actual = session().execute("SELECT c FROM test_coll WHERE k = 1").one().getSet("c", String.class);
        assertThat(actual).containsOnly("bar", "qix");
    }

    @Test(groups = "short")
    public void should_delete_set_element_with_bind_marker() throws Exception {
        //given
        session().execute("INSERT INTO test_coll (k, c) VALUES (1, {'foo','bar','qix'})");
        //when
        BuiltStatement statement = delete().setElt("c", bindMarker()).from("test_coll").where(eq("k", 1));
        PreparedStatement ps = session().prepare(statement);
        session().execute(ps.bind("foo"));
        //then
        Set<String> actual = session().execute("SELECT c FROM test_coll WHERE k = 1").one().getSet("c", String.class);
        assertThat(actual).containsOnly("bar", "qix");
    }

    @Test(groups = "short")
    public void should_delete_map_entry() throws Exception {
        //given
        session().execute("INSERT INTO test_coll (k, b) VALUES (1, {1:'foo', 2:'bar'})");
        //when
        BuiltStatement statement = delete().mapElt("b", 1).from("test_coll").where(eq("k", 1));
        session().execute(statement);
        //then
        Map<Integer, String> actual = session().execute("SELECT b FROM test_coll WHERE k = 1").one().getMap("b", Integer.class, String.class);
        assertThat(actual).containsExactly(entry(2, "bar"));
    }

    @Test(groups = "short")
    public void should_delete_map_entry_with_bind_marker() throws Exception {
        //given
        session().execute("INSERT INTO test_coll (k, a, b) VALUES (1, null, {1:'foo', 2:'bar'})");
        //when
        BuiltStatement statement = delete().mapElt("b", bindMarker()).from("test_coll").where(eq("k", 1));
        PreparedStatement ps = session().prepare(statement);
        session().execute(ps.bind().setInt(0, 1));
        //then
        Map<Integer, String> actual = session().execute("SELECT b FROM test_coll WHERE k = 1").one().getMap("b", Integer.class, String.class);
        assertThat(actual).containsExactly(entry(2, "bar"));
    }

    /**
     * Validates that {@link QueryBuilder} may be used to create a query that casts a column from one type to another,
     * i.e.:
     * <p/>
     * <code>select CAST(f as int) as fint, i from table2 where k='cast_t'</code>
     * <p/>
     * and validates that the query executes successfully with the anticipated results.
     *
     * @jira_ticket JAVA-1086
     * @test_category queries:builder
     * @since 3.0.1
     */
    @Test(groups = "short")
    @CassandraVersion("3.2")
    public void should_support_cast_function_on_column() {
        //when
        ResultSet r = session().execute(select().cast("f", DataType.cint()).as("fint").column("i").from(TABLE2).where(eq("k", "cast_t")));
        //then
        assertThat(r.getAvailableWithoutFetching()).isEqualTo(4);
        for (Row row : r) {
            Integer i = row.getInt("i");
            assertThat(row.getColumnDefinitions().getType("fint")).isEqualTo(DataType.cint());
            Integer f = row.getInt("fint");
            switch (i) {
                case 1:
                    assertThat(f).isEqualTo(1);
                    break;
                case 2:
                    assertThat(f).isEqualTo(2);
                    break;
                case 3:
                    assertThat(f).isEqualTo(3);
                    break;
                case 4:
                    assertThat(f).isEqualTo(5);
                    break;
                default:
                    fail("Unexpected values: " + i + "," + f);
            }
        }
    }

    /**
     * Validates that {@link QueryBuilder} may be used to create a query that makes an aggregate function call, casting
     * the column(s) that the function operates on from one type to another.
     * i.e.:
     * <p/>
     * <code>select avg(CAST(i as float)) as iavg from table2 where k='cast_t'</code>
     * <p/>
     * and validates that the query executes successfully with the anticipated results.
     *
     * @jira_ticket JAVA-1086
     * @test_category queries:builder
     * @since 3.0.1
     */
    @Test(groups = "short")
    @CassandraVersion("3.2")
    public void should_support_fcall_on_cast_column() {
        //when
        ResultSet ar = session().execute(select().fcall("avg", cast(column("i"), DataType.cfloat())).as("iavg").from(TABLE2).where(eq("k", "cast_t")));
        //then
        assertThat(ar.getAvailableWithoutFetching()).isEqualTo(1);
        Row row = ar.one();
        assertThat(row.getColumnDefinitions().getType("iavg")).isEqualTo(DataType.cfloat());
        Float f = row.getFloat("iavg");
        // (1.0+2.0+3.0+4.0) / 4 = 2.5
        assertThat(f).isEqualTo(2.5f);
    }

    /**
     * Validates that {@link QueryBuilder} can construct a query using the 'LIKE' operator to retrieve data from a
     * table on a column that has a SASI index, i.e.:
     * <p/>
     * <code>select n from s_table where n like 'Hello%'</code>
     * <p/>
     *
     * @test_category queries:builder
     * @jira_ticket JAVA-1113
     * @since 3.0.1
     */
    @Test(groups = "short")
    @CassandraVersion("3.6")
    public void should_retrieve_using_like_operator_on_table_with_sasi_index() {
        //given
        String table = "s_table";
        session().execute(createTable(table).addPartitionKey("k", DataType.text())
                .addClusteringColumn("cc", DataType.cint())
                .addColumn("n", DataType.text())
        );
        session().execute(String.format(
                "CREATE CUSTOM INDEX on %s (n) USING 'org.apache.cassandra.index.sasi.SASIIndex';", table));
        session().execute(insertInto(table).value("k", "a").value("cc", 0).value("n", "Hello World"));
        session().execute(insertInto(table).value("k", "a").value("cc", 1).value("n", "Goodbye World"));
        session().execute(insertInto(table).value("k", "b").value("cc", 2).value("n", "Hello Moon"));

        //when
        BuiltStatement query = select("n").from(table).where(like("n", "Hello%"));
        ResultSet r = session().execute(query);

        //then
        assertThat(r.getAvailableWithoutFetching()).isEqualTo(2);
        assertThat(r.all()).extracting(new Extractor<Row, String>() {
            @Override
            public String extract(Row input) {
                return input.getString("n");
            }
        }).containsOnly("Hello World", "Hello Moon");
    }

    /**
     * Validates that {@link QueryBuilder} can construct a query using the 'PER PARTITION LIMIT' operator to restrict
     * the number of rows returned per partition in a query, i.e.:
     * <p/>
     * <code>SELECT * FROM test_ppl PER PARTITION LIMIT 2</code>
     * <p/>
     *
     * @test_category queries:builder
     * @jira_ticket JAVA-1153
     * @since 3.1.0
     */
    @CassandraVersion(value = "3.6", description = "Support for PER PARTITION LIMIT was added to C* 3.6 (CASSANDRA-7017)")
    @Test(groups = "short")
    public void should_support_per_partition_limit() throws Exception {
        assertThat(session().execute(select().all().from("test_ppl").perPartitionLimit(2)))
                .contains(
                        row(0, 0, 0),
                        row(0, 1, 1),
                        row(1, 0, 0),
                        row(1, 1, 1),
                        row(2, 0, 0),
                        row(2, 1, 1),
                        row(3, 0, 0),
                        row(3, 1, 1),
                        row(4, 0, 0),
                        row(4, 1, 1));
        // Combined Per Partition and "global" limit
        assertThat(session().execute(select().all().from("test_ppl").perPartitionLimit(2).limit(6))).hasSize(6);
        // odd amount of results
        assertThat(session().execute(select().all().from("test_ppl").perPartitionLimit(2).limit(5)))
                .contains(
                        row(0, 0, 0),
                        row(0, 1, 1),
                        row(1, 0, 0),
                        row(1, 1, 1),
                        row(2, 0, 0));
        // IN query
        assertThat(session().execute(select().all().from("test_ppl").where(in("a", 2, 3)).perPartitionLimit(2)))
                .contains(
                        row(2, 0, 0),
                        row(2, 1, 1),
                        row(3, 0, 0),
                        row(3, 1, 1));
        assertThat(session().execute(select().all().from("test_ppl").where(in("a", 2, 3))
                .perPartitionLimit(bindMarker()).limit(3).getQueryString(), 2))
                .hasSize(3);
        assertThat(session().execute(select().all().from("test_ppl").where(in("a", 1, 2, 3))
                .perPartitionLimit(bindMarker()).limit(3).getQueryString(), 2))
                .hasSize(3);
        // with restricted partition key
        assertThat(session().execute(select().all().from("test_ppl").where(eq("a", bindMarker()))
                .perPartitionLimit(bindMarker()).getQueryString(), 2, 3))
                .containsExactly(
                        row(2, 0, 0),
                        row(2, 1, 1),
                        row(2, 2, 2));
        // with ordering
        assertThat(session().execute(select().all().from("test_ppl").where(eq("a", bindMarker()))
                .orderBy(desc("b")).perPartitionLimit(bindMarker()).getQueryString(), 2, 3))
                .containsExactly(
                        row(2, 4, 4),
                        row(2, 3, 3),
                        row(2, 2, 2));
        // with filtering
        assertThat(session().execute(select().all().from("test_ppl").where(eq("a", bindMarker()))
                .and(gt("b", bindMarker())).perPartitionLimit(bindMarker()).allowFiltering().getQueryString(), 2, 0, 2))
                .containsExactly(
                        row(2, 1, 1),
                        row(2, 2, 2));
        assertThat(session().execute(select().all().from("test_ppl").where(eq("a", bindMarker()))
                .and(gt("b", bindMarker())).orderBy(desc("b")).perPartitionLimit(bindMarker()).allowFiltering().getQueryString(), 2, 2, 2))
                .containsExactly(
                        row(2, 4, 4),
                        row(2, 3, 3));
    }

}
