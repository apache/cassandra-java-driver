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

    @Override
    protected Collection<String> getTableDefinitions() {
        return Arrays.asList(String.format("CREATE TABLE %s (k text PRIMARY KEY, a int, b int)", TABLE_TEXT),
                             String.format("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)", TABLE_INT));
    }

    @Test(groups = "short")
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

    @Test(groups = "short")
    public void selectInjectionTests() throws Exception {

        String query;
        Query select;
        PreparedStatement ps;
        BoundStatement bs;

        session.execute("CREATE KEYSPACE foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        session.execute("CREATE TABLE foo.foo ( k ascii PRIMARY KEY , i int, s ascii )");
        session.execute("USE foo");

        query = "SELECT * FROM foo WHERE k=?;";
        select = select().all().from("foo").where(eq("k", QueryBuilder.bindMarker()));
        System.out.println(select.toString());
        System.out.println(session);
        ps = session.prepare(select.toString());
        bs = ps.bind();
        assertEquals(select.toString(), query);
        session.execute(bs.setString("k", "4 AND c=5"));

        query = "SELECT * FROM foo WHERE k='4'' AND c=''5';";
        select = select().all().from("foo").where(eq("k", "4' AND c='5"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k='4'' OR ''1''=''1';";
        select = select().all().from("foo").where(eq("k", "4' OR '1'='1"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k='4; --test comment;';";
        select = select().all().from("foo").where(eq("k", "4; --test comment;"));
        assertEquals(select.toString(), query);

        query = "SELECT \"*\" FROM foo;";
        select = select("*").from("foo");
        assertEquals(select.toString(), query);

        query = "SELECT a,b FROM foo WHERE a IN ('b','c''); --comment');";
        select = select("a", "b").from("foo")
                .where(in("a", "b", "c'); --comment"));
        assertEquals(select.toString(), query);

        // User Injection?
        query = "SELECT * FROM bar; --(b) FROM foo;";
        select = select().fcall("* FROM bar; --", column("b")).from("foo");
        assertEquals(select.toString(), query);

        query = "SELECT writetime(\"a) FROM bar; --\"),ttl(a) FROM foo ALLOW FILTERING;";
        select = select().writeTime("a) FROM bar; --").ttl("a").from("foo").allowFiltering();
        assertEquals(select.toString(), query);

        query = "SELECT writetime(a),ttl(\"a) FROM bar; --\") FROM foo ALLOW FILTERING;";
        select = select().writeTime("a").ttl("a) FROM bar; --").from("foo").allowFiltering();
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE \"k=1 OR k\">42 LIMIT 42;";
        select = select().all().from("foo").where(gt("k=1 OR k", 42)).limit(42);
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE token(\"k)>0 OR token(k\")>token(42);";
        select = select().all().from("foo").where(gt(token("k)>0 OR token(k"), fcall("token", 42)));
        assertEquals(select.toString(), query);
    }

    @Test(groups = "short")
    public void insertInjectionTest() throws Exception {

        String query;
        Query insert;

        query = "INSERT INTO foo(a) VALUES ('123); --comment');";
        insert = insertInto("foo")
                .value("a", "123); --comment");
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo(\"a,b\") VALUES (123);";
        insert = insertInto("foo")
                .value("a,b", 123);
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo(a,b) VALUES ({'2''} space','3','4'},3.4) USING TTL 24 AND TIMESTAMP 42;";
        insert = insertInto("foo").values(new String[]{ "a", "b"}, new Object[]{ new TreeSet(){{ add("2'} space"); add("3"); add("4"); }}, 3.4 }).using(ttl(24)).and(timestamp(42));
        assertEquals(insert.toString(), query);
    }

    @Test(groups = "short")
    public void updateInjectionTest() throws Exception {

        String query;
        Query update;

        query = "UPDATE foo.bar USING TIMESTAMP 42 SET a=12 WHERE k='2 OR 1=1';";
        update = update("foo", "bar").using(timestamp(42)).with(set("a", 12)).where(eq("k", "2 OR 1=1"));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET b='null WHERE k=1; --comment' WHERE k=2;";
        update = update("foo").where().and(eq("k", 2)).with(set("b", "null WHERE k=1; --comment"));
        assertEquals(update.toString(), query);

        query = "UPDATE foo USING TIMESTAMP 42 SET \"b WHERE k=1; --comment\"=[3,2,1]+\"b WHERE k=1; --comment\" WHERE k=2;";
        update = update("foo").where().and(eq("k", 2)).with(prependAll("b WHERE k=1; --comment", Arrays.asList(3, 2, 1))).using(timestamp(42));
        assertEquals(update.toString(), query);
    }

    @Test(groups = "short")
    public void deleteInjectionTests() throws Exception {

        String query;
        Query delete;

        query = "DELETE  FROM \"foo WHERE k=4\";";
        delete = delete().from("foo WHERE k=4");
        assertEquals(delete.toString(), query);

        query = "DELETE  FROM foo WHERE k='4 AND c=5';";
        delete = delete().from("foo").where(eq("k", "4 AND c=5"));
        assertEquals(delete.toString(), query);

        query = "DELETE  FROM foo WHERE k='4'' AND c=''5';";
        delete = delete().from("foo").where(eq("k", "4' AND c='5"));
        assertEquals(delete.toString(), query);

        query = "DELETE  FROM foo WHERE k='4'' OR ''1''=''1';";
        delete = delete().from("foo").where(eq("k", "4' OR '1'='1"));
        assertEquals(delete.toString(), query);

        query = "DELETE  FROM foo WHERE k='4; --test comment;';";
        delete = delete().from("foo").where(eq("k", "4; --test comment;"));
        assertEquals(delete.toString(), query);

        query = "DELETE \"*\" FROM foo;";
        delete = delete("*").from("foo");
        assertEquals(delete.toString(), query);

        query = "DELETE a,b FROM foo WHERE a IN ('b','c''); --comment');";
        delete = delete("a", "b").from("foo")
                .where(in("a", "b", "c'); --comment"));
        assertEquals(delete.toString(), query);

        query = "DELETE  FROM foo WHERE \"k=1 OR k\">42;";
        delete = delete().from("foo").where(gt("k=1 OR k", 42));
        assertEquals(delete.toString(), query);

        query = "DELETE  FROM foo WHERE token(\"k)>0 OR token(k\")>token(42);";
        delete = delete().from("foo").where(gt(token("k)>0 OR token(k"), fcall("token", 42)));
        assertEquals(delete.toString(), query);
    }

}
