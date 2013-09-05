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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Query;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class QueryBuilderTest {

    @Test(groups = "unit")
    public void selectTest() throws Exception {

        String query;
        Query select;

        query = "SELECT * FROM foo WHERE k=4 AND c>'a' AND c<='z';";
        select = select().all().from("foo").where(eq("k", 4)).and(gt("c", "a")).and(lte("c", "z"));
        assertEquals(select.toString(), query);

        // Ensure where() and where(...) are equal
        select = select().all().from("foo").where().and(eq("k", 4)).and(gt("c", "a")).and(lte("c", "z"));
        assertEquals(select.toString(), query);

        query = "SELECT a,b,\"C\" FROM foo WHERE a IN ('127.0.0.1','127.0.0.3') AND \"C\"='foo' ORDER BY a ASC,b DESC LIMIT 42;";
        select = select("a", "b", quote("C")).from("foo")
                   .where(in("a", InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.3")))
                      .and(eq(quote("C"), "foo"))
                   .orderBy(asc("a"), desc("b"))
                   .limit(42);
        assertEquals(select.toString(), query);

        query = "SELECT writetime(a),ttl(a) FROM foo ALLOW FILTERING;";
        select = select().writeTime("a").ttl("a").from("foo").allowFiltering();
        assertEquals(select.toString(), query);

        query = "SELECT count(*) FROM foo;";
        select = select().countAll().from("foo");
        assertEquals(select.toString(), query);

        // Ensure getQueryString() == to.String()
        assertEquals(select().countAll().from("foo").getQueryString(), query);

        query = "SELECT intToBlob(b) FROM foo;";
        select = select().fcall("intToBlob", column("b")).from("foo");
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k>42 LIMIT 42;";
        select = select().all().from("foo").where(gt("k", 42)).limit(42);
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE token(k)>token(42);";
        select = select().all().from("foo").where(gt(token("k"), fcall("token", 42)));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo2 WHERE token(a,b)>token(42,101);";
        select = select().all().from("foo2").where(gt(token("a", "b"), fcall("token", 42, 101)));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM words WHERE w='):,ydL ;O,D';";
        select = select().all().from("words").where(eq("w", "):,ydL ;O,D"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM words WHERE w='WA(!:gS)r(UfW';";
        select = select().all().from("words").where(eq("w", "WA(!:gS)r(UfW"));
        assertEquals(select.toString(), query);

        try {
            select = select("a").from("foo").where(in("a"));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Missing values for IN clause");
        }

        try {
            select = select().countAll().from("foo").orderBy(asc("a"), desc("b")).orderBy(asc("a"), desc("b"));
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "An ORDER BY clause has already been provided");
        }

        try {
            select = select().column("a").all().from("foo");
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Some columns ([a]) have already been selected.");
        }

        try {
            select = select().column("a").countAll().from("foo");
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Some columns ([a]) have already been selected.");
        }

        try {
            select = select().all().from("foo").limit(-42);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid LIMIT value, must be strictly positive");
        }

        try {
            select = select().all().from("foo").limit(42).limit(42);
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "A LIMIT value has already been provided");
        }
    }

    @Test(groups = "unit")
    public void insertTest() throws Exception {

        String query;
        Query insert;

        query = "INSERT INTO foo(a,b,\"C\",d) VALUES (123,'127.0.0.1','foo''bar',{'x':3,'y':2}) USING TIMESTAMP 42 AND TTL 24;";
        insert = insertInto("foo")
                   .value("a", 123)
                   .value("b", InetAddress.getByName("127.0.0.1"))
                   .value(quote("C"), "foo'bar")
                   .value("d", new TreeMap(){{ put("x", 3); put("y", 2); }})
                   .using(timestamp(42)).and(ttl(24));
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo(a,b) VALUES (2,null);";
        insert = insertInto("foo")
                   .value("a", 2)
                   .value("b", null);
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo(a,b) VALUES ({2,3,4},3.4) USING TTL 24 AND TIMESTAMP 42;";
        insert = insertInto("foo").values(new String[]{ "a", "b"}, new Object[]{ new TreeSet(){{ add(2); add(3); add(4); }}, 3.4 }).using(ttl(24)).and(timestamp(42));
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo.bar(a,b) VALUES ({2,3,4},3.4) USING TTL 24 AND TIMESTAMP 42;";
        insert = insertInto("foo", "bar")
                    .values(new String[]{ "a", "b"}, new Object[]{ new TreeSet(){{ add(2); add(3); add(4); }}, 3.4 })
                    .using(ttl(24))
                    .and(timestamp(42));
        assertEquals(insert.toString(), query);

        // commutative result of TIMESTAMP
        query = "INSERT INTO foo.bar(a,b,c) VALUES ({2,3,4},3.4,123) USING TIMESTAMP 42;";
        insert = insertInto("foo", "bar")
                    .using(timestamp(42))
                    .values(new String[]{ "a", "b"}, new Object[]{ new TreeSet(){{ add(2); add(3); add(4); }}, 3.4 })
                    .value("c", 123);
        assertEquals(insert.toString(), query);

        // commutative result of value() and values()
        query = "INSERT INTO foo(c,a,b) VALUES (123,{2,3,4},3.4) USING TIMESTAMP 42;";
        insert = insertInto("foo")
                    .using(timestamp(42))
                    .value("c", 123)
                    .values(new String[]{ "a", "b"}, new Object[]{ new TreeSet(){{ add(2); add(3); add(4); }}, 3.4 });
        assertEquals(insert.toString(), query);

        try {
            insert = insertInto("foo").values(new String[]{ "a", "b"}, new Object[]{ 1, 2, 3 });
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Got 2 names but 3 values");
        }
    }

    @Test(groups = "unit")
    public void updateTest() throws Exception {

        String query;
        Query update;

        query = "UPDATE foo.bar USING TIMESTAMP 42 SET a=12,b=[3,2,1],c=c+3 WHERE k=2;";
        update = update("foo", "bar").using(timestamp(42)).with(set("a", 12)).and(set("b", Arrays.asList(3, 2, 1))).and(incr("c", 3)).where(eq("k", 2));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET b=null WHERE k=2;";
        update = update("foo").where().and(eq("k", 2)).with(set("b", null));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET a[2]='foo',b=[3,2,1]+b,c=c-{'a'} WHERE k=2 AND l='foo' AND m<4 AND n>=1;";
        update = update("foo").with(setIdx("a", 2, "foo")).and(prependAll("b", Arrays.asList(3, 2, 1))).and(remove("c", "a")).where(eq("k", 2)).and(eq("l", "foo")).and(lt("m", 4)).and(gte("n", 1));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET b=[3]+b,c=c+['a'],d=d+[1,2,3],e=e-[1];";
        update = update("foo").with().and(prepend("b", 3)).and(append("c", "a")).and(appendAll("d", Arrays.asList(1, 2, 3))).and(discard("e", 1));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET b=b-[1,2,3],c=c+{1},d=d+{2,3,4};";
        update = update("foo").with(discardAll("b", Arrays.asList(1, 2, 3))).and(add("c", 1)).and(addAll("d", new TreeSet(){{ add(2); add(3); add(4); }}));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET b=b-{2,3,4},c['k']='v',d=d+{'x':3,'y':2};";
        update = update("foo").with(removeAll("b", new TreeSet(){{ add(2); add(3); add(4); }}))
                    .and(put("c", "k", "v"))
                    .and(putAll("d", new TreeMap(){{ put("x", 3); put("y", 2); }}));
        assertEquals(update.toString(), query);

        query = "UPDATE foo USING TTL 400;";
        update = update("foo").using(ttl(400));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET a="+new BigDecimal(3.2)+",b=42 WHERE k=2;";
        update = update("foo").with(set("a", new BigDecimal(3.2))).and(set("b", new BigInteger("42"))).where(eq("k", 2));
        assertEquals(update.toString(), query);

        query = "UPDATE foo USING TIMESTAMP 42 SET b=[3,2,1]+b WHERE k=2 AND l='foo';";
        update = update("foo").where().and(eq("k", 2)).and(eq("l", "foo")).with(prependAll("b", Arrays.asList(3, 2, 1))).using(timestamp(42));
        assertEquals(update.toString(), query);

        // Test commutative USING
        update = update("foo").where().and(eq("k", 2)).and(eq("l", "foo")).using(timestamp(42)).with(prependAll("b", Arrays.asList(3, 2, 1)));
        assertEquals(update.toString(), query);

        // Test commutative USING
        update = update("foo").using(timestamp(42)).where(eq("k", 2)).and(eq("l", "foo")).with(prependAll("b", Arrays.asList(3, 2, 1)));
        assertEquals(update.toString(), query);

        try {
            update = update("foo").using(ttl(-400));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid ttl, must be positive");
        }
    }

    @Test(groups = "unit")
    public void deleteTest() throws Exception {

        String query;
        Query delete;

        query = "DELETE a,b,c FROM foo USING TIMESTAMP 0 WHERE k=1;";
        delete = delete("a", "b", "c").from("foo").using(timestamp(0)).where(eq("k", 1));
        assertEquals(delete.toString(), query);

        query = "DELETE a[3],b['foo'],c FROM foo WHERE k=1;";
        delete = delete().listElt("a", 3).mapElt("b", "foo").column("c").from("foo").where(eq("k", 1));
        assertEquals(delete.toString(), query);

        // Invalid CQL, testing edge case
        query = "DELETE a,b,c FROM foo;";
        delete = delete("a", "b", "c").from("foo");
        assertEquals(delete.toString(), query);

        query = "DELETE  FROM foo USING TIMESTAMP 1240003134 WHERE k='value';";
        delete = delete().all().from("foo").using(timestamp(1240003134L)).where(eq("k", "value"));
        assertEquals(delete.toString(), query);
        delete = delete().from("foo").using(timestamp(1240003134L)).where(eq("k", "value"));
        assertEquals(delete.toString(), query);

        query = "DELETE a,b,c FROM foo.bar USING TIMESTAMP 1240003134 WHERE k=1;";
        delete = delete("a", "b", "c").from("foo", "bar").where().and(eq("k", 1)).using(timestamp(1240003134L));
        assertEquals(delete.toString(), query);

        try {
            delete = delete().column("a").all().from("foo");
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Some columns ([a]) have already been selected.");
        }

        try {
            delete = delete().from("foo").using(timestamp(-1240003134L));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid timestamp, must be positive");
        }
    }

    @Test(groups = "unit")
    public void batchTest() throws Exception {
        String query;
        Query batch;

        query = "BEGIN BATCH USING TIMESTAMP 42 ";
        query += "INSERT INTO foo(a,b) VALUES ({2,3,4},3.4);";
        query += "UPDATE foo SET a[2]='foo',b=[3,2,1]+b,c=c-{'a'} WHERE k=2;";
        query += "DELETE a[3],b['foo'],c FROM foo WHERE k=1;";
        query += "APPLY BATCH;";
        batch = batch()
            .add(insertInto("foo").values(new String[]{ "a", "b"}, new Object[]{ new TreeSet(){{ add(2); add(3); add(4); }}, 3.4 }))
            .add(update("foo").with(setIdx("a", 2, "foo")).and(prependAll("b", Arrays.asList(3, 2, 1))).and(remove("c", "a")).where(eq("k", 2)))
            .add(delete().listElt("a", 3).mapElt("b", "foo").column("c").from("foo").where(eq("k", 1)))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);

        // Test passing batch(statement)
        query = "BEGIN BATCH ";
        query += "DELETE a[3] FROM foo WHERE k=1;";
        query += "APPLY BATCH;";
        batch = batch(delete().listElt("a", 3).from("foo").where(eq("k", 1)));
        assertEquals(batch.toString(), query);
    }

    @Test(groups = "unit")
    public void batchCounterTest() throws Exception {
        String query;
        Query batch;

        // Test value increments
        query = "BEGIN COUNTER BATCH USING TIMESTAMP 42 ";
        query += "UPDATE foo SET a=a+1;";
        query += "UPDATE foo SET b=b+2;";
        query += "UPDATE foo SET c=c+3;";
        query += "APPLY BATCH;";
        batch = batch()
            .add(update("foo").with(incr("a", 1)))
            .add(update("foo").with(incr("b", 2)))
            .add(update("foo").with(incr("c", 3)))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);

        // Test single increments
        query = "BEGIN COUNTER BATCH USING TIMESTAMP 42 ";
        query += "UPDATE foo SET a=a+1;";
        query += "UPDATE foo SET b=b+1;";
        query += "UPDATE foo SET c=c+1;";
        query += "APPLY BATCH;";
        batch = batch()
            .add(update("foo").with(incr("a")))
            .add(update("foo").with(incr("b")))
            .add(update("foo").with(incr("c")))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);

        // Test value decrements
        query = "BEGIN COUNTER BATCH USING TIMESTAMP 42 ";
        query += "UPDATE foo SET a=a-1;";
        query += "UPDATE foo SET b=b-2;";
        query += "UPDATE foo SET c=c-3;";
        query += "APPLY BATCH;";
        batch = batch()
            .add(update("foo").with(decr("a", 1)))
            .add(update("foo").with(decr("b", 2)))
            .add(update("foo").with(decr("c", 3)))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);

        // Test single decrements
        query = "BEGIN COUNTER BATCH USING TIMESTAMP 42 ";
        query += "UPDATE foo SET a=a-1;";
        query += "UPDATE foo SET b=b-1;";
        query += "UPDATE foo SET c=c-1;";
        query += "APPLY BATCH;";
        batch = batch()
            .add(update("foo").with(decr("a")))
            .add(update("foo").with(decr("b")))
            .add(update("foo").with(decr("c")))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);

        // Test negative decrements and negative increments
        query = "BEGIN COUNTER BATCH USING TIMESTAMP 42 ";
        query += "UPDATE foo SET a=a+1;";
        query += "UPDATE foo SET b=b+-2;";
        query += "UPDATE foo SET c=c-3;";
        query += "APPLY BATCH;";
        batch = batch()
            .add(update("foo").with(decr("a", -1)))
            .add(update("foo").with(incr("b", -2)))
            .add(update("foo").with(decr("c", 3)))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);
    }

    @Test(groups = "unit", expectedExceptions={IllegalArgumentException.class})
    public void batchMixedCounterTest() throws Exception {
        String query;
        Query batch;

        batch = batch()
            .add(update("foo").with(incr("a", 1)))
            .add(update("foo").with(set("b", 2)))
            .add(update("foo").with(incr("c", 3)))
            .using(timestamp(42));
    }

    @Test(groups = "unit")
    public void markerTest() throws Exception {
        String query;
        Query insert;

        query = "INSERT INTO test(k,c) VALUES (0,?);";
        insert = insertInto("test")
                   .value("k", 0)
                   .value("c", bindMarker());
        assertEquals(insert.toString(), query);
    }

    @Test(groups = "unit")
    public void rawEscapingTest() throws Exception {

        String query;
        Query select;

        query = "SELECT * FROM t WHERE c='C''est la vie!';";
        select = select().from("t").where(eq("c", "C'est la vie!"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM t WHERE c='C'est la vie!';";
        select = select().from("t").where(eq("c", raw("C'est la vie!")));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM t WHERE c=now();";
        select = select().from("t").where(eq("c", fcall("now")));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM t WHERE c='now()';";
        select = select().from("t").where(eq("c", raw("now()")));
        assertEquals(select.toString(), query);
    }


    @Test(groups = "unit")
    public void selectInjectionTests() throws Exception {

        String query;
        Query select;

        query = "SELECT * FROM \"foo WHERE k=4\";";
        select = select().all().from("foo WHERE k=4");
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k='4 AND c=5';";
        select = select().all().from("foo").where(eq("k", "4 AND c=5"));
        assertEquals(select.toString(), query);

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
        select = select("a", "b").from("foo").where(in("a", "b", "c'); --comment"));
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

    @Test(groups = "unit")
    public void insertInjectionTest() throws Exception {

        String query;
        Query insert;

        query = "INSERT INTO foo(a) VALUES ('123); --comment');";
        insert = insertInto("foo").value("a", "123); --comment");
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo(\"a,b\") VALUES (123);";
        insert = insertInto("foo").value("a,b", 123);
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo(a,b) VALUES ({'2''} space','3','4'},3.4) USING TTL 24 AND TIMESTAMP 42;";
        insert = insertInto("foo").values(new String[]{ "a", "b"}, new Object[]{ new TreeSet(){{ add("2'} space"); add("3"); add("4"); }}, 3.4 }).using(ttl(24)).and(timestamp(42));
        assertEquals(insert.toString(), query);
    }

    @Test(groups = "unit")
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

    @Test(groups = "unit")
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

    @Test(groups = "unit")
    public void statementForwardingTest() throws Exception {

        Update upd = update("foo");
        upd.setConsistencyLevel(ConsistencyLevel.QUORUM);
        upd.enableTracing();

        Query query = upd.using(timestamp(42)).with(set("a", 12)).and(incr("c", 3)).where(eq("k", 2));

        assertEquals(query.getConsistencyLevel(), ConsistencyLevel.QUORUM);
        assertTrue(query.isTracing());
    }

    @Test(groups = "unit")
    public void rejectUnknownValueTest() throws Exception {

        try {
            update("foo").with(set("a", new byte[13])).where(eq("k", 2)).toString();
            fail("Byte arrays should not be valid, ByteBuffer should be used instead");
        } catch (IllegalArgumentException e) {
            System.out.println(">> " + e);
            // Ok, that's what we expect
        }
    }
}
