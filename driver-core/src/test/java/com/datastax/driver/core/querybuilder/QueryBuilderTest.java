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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class QueryBuilderTest {

    private QueryBuilder builder = new QueryBuilder(TestUtils.getDesiredProtocolVersion(), CodecRegistry.DEFAULT_INSTANCE);

    @Test(groups = "unit")
    public void selectTest() throws Exception {

        String query;
        Statement select;

        query = "SELECT * FROM foo WHERE k=4 AND c>'a' AND c<='z';";
        select = builder.select().all().from("foo").where(eq("k", 4)).and(gt("c", "a")).and(lte("c", "z"));
        assertEquals(select.toString(), query);

        // Ensure where() and where(...) are equal
        select = builder.select().all().from("foo").where().and(eq("k", 4)).and(gt("c", "a")).and(lte("c", "z"));
        assertEquals(select.toString(), query);

        query = "SELECT a,b,\"C\" FROM foo WHERE a IN ('127.0.0.1','127.0.0.3') AND \"C\"='foo' ORDER BY a ASC,b DESC LIMIT 42;";
        select = builder.select("a", "b", quote("C")).from("foo")
            .where(in("a", InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.3")))
            .and(eq(quote("C"), "foo"))
            .orderBy(asc("a"), desc("b"))
            .limit(42);
        assertEquals(select.toString(), query);

        query = "SELECT writetime(a),ttl(a) FROM foo ALLOW FILTERING;";
        select = builder.select().writeTime("a").ttl("a").from("foo").allowFiltering();
        assertEquals(select.toString(), query);

        query = "SELECT DISTINCT longName AS a,ttl(longName) AS ttla FROM foo LIMIT :limit;";
        select = builder.select().distinct().column("longName").as("a").ttl("longName").as("ttla").from("foo").limit(bindMarker("limit"));
        assertEquals(select.toString(), query);

        query = "SELECT DISTINCT longName AS a,ttl(longName) AS ttla FROM foo WHERE k IN () LIMIT :limit;";
        select = builder.select().distinct().column("longName").as("a").ttl("longName").as("ttla").from("foo").where(in("k")).limit(bindMarker("limit"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE bar=:barmark AND baz=:bazmark LIMIT :limit;";
        select = builder.select().all().from("foo").where().and(eq("bar", bindMarker("barmark"))).and(eq("baz", bindMarker("bazmark"))).limit(bindMarker("limit"));
        assertEquals(select.toString(), query);

        query = "SELECT a FROM foo WHERE k IN ();";
        select = builder.select("a").from("foo").where(in("k"));
        assertEquals(select.toString(), query);

        query = "SELECT a FROM foo WHERE k IN ?;";
        select = builder.select("a").from("foo").where(in("k", bindMarker()));
        assertEquals(select.toString(), query);

        query = "SELECT DISTINCT a FROM foo WHERE k=1;";
        select = builder.select("a").distinct().from("foo").where(eq("k", 1));
        assertEquals(select.toString(), query);

        query = "SELECT DISTINCT a,b FROM foo WHERE k=1;";
        select = builder.select("a", "b").distinct().from("foo").where(eq("k", 1));
        assertEquals(select.toString(), query);

        query = "SELECT count(*) FROM foo;";
        select = builder.select().countAll().from("foo");
        assertEquals(select.toString(), query);

        query = "SELECT intToBlob(b) FROM foo;";
        select = builder.select().fcall("intToBlob", column("b")).from("foo");
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k>42 LIMIT 42;";
        select = builder.select().all().from("foo").where(gt("k", 42)).limit(42);
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE token(k)>token(42);";
        select = builder.select().all().from("foo").where(gt(token("k"), fcall("token", 42)));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo2 WHERE token(a,b)>token(42,101);";
        select = builder.select().all().from("foo2").where(gt(token("a", "b"), fcall("token", 42, 101)));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM words WHERE w='):,ydL ;O,D';";
        select = builder.select().all().from("words").where(eq("w", "):,ydL ;O,D"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM words WHERE w='WA(!:gS)r(UfW';";
        select = builder.select().all().from("words").where(eq("w", "WA(!:gS)r(UfW"));
        assertEquals(select.toString(), query);

        Date date = new Date();
        date.setTime(1234325);
        query = "SELECT * FROM foo WHERE d=1234325;";
        select = builder.select().all().from("foo").where(eq("d", date));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE b=0xcafebabe;";
        select = builder.select().all().from("foo").where(eq("b", Bytes.fromHexString("0xCAFEBABE")));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE e CONTAINS 'text';";
        select = builder.select().from("foo").where(contains("e", "text"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE e CONTAINS KEY 'key1';";
        select = builder.select().from("foo").where(containsKey("e", "key1"));
        assertEquals(select.toString(), query);

        try {
            builder.select().countAll().from("foo").orderBy(asc("a"), desc("b")).orderBy(asc("a"), desc("b"));
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "An ORDER BY clause has already been provided");
        }

        try {
            builder.select().column("a").all().from("foo");
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Some columns ([a]) have already been selected.");
        }

        try {
            builder.select().column("a").countAll().from("foo");
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Some columns ([a]) have already been selected.");
        }

        try {
            builder.select().all().from("foo").limit(-42);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid LIMIT value, must be strictly positive");
        }

        try {
            builder.select().all().from("foo").limit(42).limit(42);
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "A LIMIT value has already been provided");
        }
    }

    @Test(groups = "unit")
    @SuppressWarnings({ "serial", "deprecation" })
    public void insertTest() throws Exception {

        String query;
        Statement insert;

        query = "INSERT INTO foo(a,b,\"C\",d) VALUES (123,'127.0.0.1','foo''bar',{'x':3,'y':2}) USING TIMESTAMP 42 AND TTL 24;";
        insert = builder.insertInto("foo")
            .value("a", 123)
            .value("b", InetAddress.getByName("127.0.0.1"))
            .value(quote("C"), "foo'bar")
            .value("d", new TreeMap<String, Integer>() {{
                put("x", 3);
                put("y", 2);
            }})
            .using(timestamp(42)).and(ttl(24));
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo(a,b) VALUES (2,null);";
        insert = builder.insertInto("foo")
            .value("a", 2)
            .value("b", null);
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo(a,b) VALUES ({2,3,4},3.4) USING TTL 24 AND TIMESTAMP 42;";
        insert = builder.insertInto("foo").values(new String[]{ "a", "b" }, new Object[]{ new TreeSet<Integer>() {{
            add(2);
            add(3);
            add(4);
        }}, 3.4 }).using(ttl(24)).and(timestamp(42));
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo.bar(a,b) VALUES ({2,3,4},3.4) USING TTL ? AND TIMESTAMP ?;";
        insert = builder.insertInto("foo", "bar")
            .values(new String[]{ "a", "b" }, new Object[]{ new TreeSet<Integer>() {{
                add(2);
                add(3);
                add(4);
            }}, 3.4 })
            .using(ttl(bindMarker()))
            .and(timestamp(bindMarker()));
        assertEquals(insert.toString(), query);

        // commutative result of TIMESTAMP
        query = "INSERT INTO foo.bar(a,b,c) VALUES ({2,3,4},3.4,123) USING TIMESTAMP 42;";
        insert = builder.insertInto("foo", "bar")
            .using(timestamp(42))
            .values(new String[]{ "a", "b" }, new Object[]{ new TreeSet<Integer>() {{
                add(2);
                add(3);
                add(4);
            }}, 3.4 })
            .value("c", 123);
        assertEquals(insert.toString(), query);

        // commutative result of value() and values()
        query = "INSERT INTO foo(c,a,b) VALUES (123,{2,3,4},3.4) USING TIMESTAMP 42;";
        insert = builder.insertInto("foo")
            .using(timestamp(42))
            .value("c", 123)
            .values(new String[]{ "a", "b" }, new Object[]{ new TreeSet<Integer>() {{
                add(2);
                add(3);
                add(4);
            }}, 3.4 });
        assertEquals(insert.toString(), query);

        try {
            builder.insertInto("foo").values(new String[]{ "a", "b"}, new Object[]{ 1, 2, 3 });
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Got 2 names but 3 values");
        }

        // CAS test
        query = "INSERT INTO foo(k,x) VALUES (0,1) IF NOT EXISTS;";
        insert = builder.insertInto("foo").value("k", 0).value("x", 1).ifNotExists();
        assertEquals(insert.toString(), query);

        // Tuples: see QueryBuilderTupleExecutionTest
        // UDT: see QueryBuilderExecutionTest
    }

    @Test(groups = "unit")
    @SuppressWarnings("serial")
    public void updateTest() throws Exception {

        String query;
        Statement update;

        query = "UPDATE foo.bar USING TIMESTAMP 42 SET a=12,b=[3,2,1],c=c+3 WHERE k=2;";
        update = builder.update("foo", "bar").using(timestamp(42)).with(set("a", 12)).and(set("b", Arrays.asList(3, 2, 1))).and(incr("c", 3)).where(eq("k", 2));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET b=null WHERE k=2;";
        update = builder.update("foo").where().and(eq("k", 2)).with(set("b", null));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET a[2]='foo',b=[3,2,1]+b,c=c-{'a'} WHERE k=2 AND l='foo' AND m<4 AND n>=1;";
        update = builder.update("foo").with(setIdx("a", 2, "foo")).and(prependAll("b", Arrays.asList(3, 2, 1))).and(remove("c", "a")).where(eq("k", 2)).and(eq("l", "foo")).and(lt("m", 4)).and(gte("n", 1));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET b=[3]+b,c=c+['a'],d=d+[1,2,3],e=e-[1];";
        update = builder.update("foo").with().and(prepend("b", 3)).and(append("c", "a")).and(appendAll("d", Arrays.asList(1, 2, 3))).and(discard("e", 1));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET b=b-[1,2,3],c=c+{1},d=d+{2,3,4};";
        update = builder.update("foo").with(discardAll("b", Arrays.asList(1, 2, 3))).and(add("c", 1)).and(addAll("d", new TreeSet<Integer>() {{
            add(2);
            add(3);
            add(4);
        }}));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET b=b-{2,3,4},c['k']='v',d=d+{'x':3,'y':2};";
        update = builder.update("foo").with(removeAll("b", new TreeSet<Integer>() {{
            add(2);
            add(3);
            add(4);
        }}))
            .and(put("c", "k", "v"))
            .and(putAll("d", new TreeMap<String, Integer>() {{
                put("x", 3);
                put("y", 2);
            }}));
        assertEquals(update.toString(), query);

        query = "UPDATE foo USING TTL 400;";
        update = builder.update("foo").using(ttl(400));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET a=" + new BigDecimal(3.2) + ",b=42 WHERE k=2;";
        update = builder.update("foo").with(set("a", new BigDecimal(3.2))).and(set("b", new BigInteger("42"))).where(eq("k", 2));
        assertEquals(update.toString(), query);

        query = "UPDATE foo USING TIMESTAMP 42 SET b=[3,2,1]+b WHERE k=2 AND l='foo';";
        update = builder.update("foo").where().and(eq("k", 2)).and(eq("l", "foo")).with(prependAll("b", Arrays.asList(3, 2, 1))).using(timestamp(42));
        assertEquals(update.toString(), query);

        // Test commutative USING
        update = builder.update("foo").where().and(eq("k", 2)).and(eq("l", "foo")).using(timestamp(42)).with(prependAll("b", Arrays.asList(3, 2, 1)));
        assertEquals(update.toString(), query);

        // Test commutative USING
        update = builder.update("foo").using(timestamp(42)).where(eq("k", 2)).and(eq("l", "foo")).with(prependAll("b", Arrays.asList(3, 2, 1)));
        assertEquals(update.toString(), query);

        try {
            builder.update("foo").using(ttl(-400));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid ttl, must be positive");
        }

        // CAS test
        query = "UPDATE foo SET x=4 WHERE k=0 IF x=1;";
        update = builder.update("foo").with(set("x", 4)).where(eq("k", 0)).onlyIf(eq("x", 1));
        assertEquals(update.toString(), query);

        // IF EXISTS CAS test
        update = builder.update("foo").with(set("x", 3)).where(eq("k", 2)).ifExists();
        assertThat(update.toString()).isEqualTo("UPDATE foo SET x=3 WHERE k=2 IF EXISTS;");
    }
    
    @Test(groups = "unit")
    public void deleteTest() throws Exception {

        String query;
        Statement delete;

        query = "DELETE a,b,c FROM foo USING TIMESTAMP 0 WHERE k=1;";
        delete = builder.delete("a", "b", "c").from("foo").using(timestamp(0)).where(eq("k", 1));
        assertEquals(delete.toString(), query);

        query = "DELETE a[3],b['foo'],c FROM foo WHERE k=1;";
        delete = builder.delete().listElt("a", 3).mapElt("b", "foo").column("c").from("foo").where(eq("k", 1));
        assertEquals(delete.toString(), query);

        query = "DELETE a[?],b[?],c FROM foo WHERE k=1;";
        delete = builder.delete().listElt("a", bindMarker()).mapElt("b", bindMarker()).column("c").from("foo").where(eq("k", 1));
        assertEquals(delete.toString(), query);

        // Invalid CQL, testing edge case
        query = "DELETE a,b,c FROM foo;";
        delete = builder.delete("a", "b", "c").from("foo");
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo USING TIMESTAMP 1240003134 WHERE k='value';";
        delete = builder.delete().all().from("foo").using(timestamp(1240003134L)).where(eq("k", "value"));
        assertEquals(delete.toString(), query);
        delete = builder.delete().from("foo").using(timestamp(1240003134L)).where(eq("k", "value"));
        assertEquals(delete.toString(), query);

        query = "DELETE a,b,c FROM foo.bar USING TIMESTAMP 1240003134 WHERE k=1;";
        delete = builder.delete("a", "b", "c").from("foo", "bar").where().and(eq("k", 1)).using(timestamp(1240003134L));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo.bar WHERE k1='foo' AND k2=1;";
        delete = builder.delete().from("foo", "bar").where(eq("k1", "foo")).and(eq("k2", 1));
        assertEquals(delete.toString(), query);

        try {
            builder.delete().column("a").all().from("foo");
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Some columns ([a]) have already been selected.");
        }

        try {
            builder.delete().from("foo").using(timestamp(-1240003134L));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid timestamp, must be positive");
        }

        query = "DELETE FROM foo.bar WHERE k1='foo' IF EXISTS;";
        delete = builder.delete().from("foo", "bar").where(eq("k1", "foo")).ifExists();
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo.bar WHERE k1='foo' IF a=1 AND b=2;";
        delete = builder.delete().from("foo", "bar").where(eq("k1", "foo")).onlyIf(eq("a", 1)).and(eq("b", 2));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE k=:key;";
        delete = builder.delete().from("foo").where(eq("k", bindMarker("key")));
        assertEquals(delete.toString(), query);
    }

    @Test(groups = "unit")
    @SuppressWarnings("serial")
    public void batchTest() throws Exception {
        String query;
        Statement batch;

        query = "BEGIN BATCH USING TIMESTAMP 42 ";
        query += "INSERT INTO foo(a,b) VALUES ({2,3,4},3.4);";
        query += "UPDATE foo SET a[2]='foo',b=[3,2,1]+b,c=c-{'a'} WHERE k=2;";
        query += "DELETE a[3],b['foo'],c FROM foo WHERE k=1;";
        query += "APPLY BATCH;";
        batch = builder.batch()
            .add(builder.insertInto("foo").values(new String[]{ "a", "b" }, new Object[]{ new TreeSet<Integer>() {{
                add(2);
                add(3);
                add(4);
            }}, 3.4 }))
            .add(builder.update("foo").with(setIdx("a", 2, "foo")).and(prependAll("b", Arrays.asList(3, 2, 1))).and(remove("c", "a")).where(eq("k", 2)))
            .add(builder.delete().listElt("a", 3).mapElt("b", "foo").column("c").from("foo").where(eq("k", 1)))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);

        // Test passing batch(statement)
        query = "BEGIN BATCH ";
        query += "DELETE a[3] FROM foo WHERE k=1;";
        query += "APPLY BATCH;";
        batch = builder.batch(builder.delete().listElt("a", 3).from("foo").where(eq("k", 1)));
        assertEquals(batch.toString(), query);

        assertEquals(builder.batch().toString(), "BEGIN BATCH APPLY BATCH;");
    }

    @Test(groups = "unit")
    public void batchCounterTest() throws Exception {
        String query;
        Statement batch;

        // Test value increments
        query = "BEGIN COUNTER BATCH USING TIMESTAMP 42 ";
        query += "UPDATE foo SET a=a+1;";
        query += "UPDATE foo SET b=b+2;";
        query += "UPDATE foo SET c=c+3;";
        query += "APPLY BATCH;";
        batch = builder.batch()
            .add(builder.update("foo").with(incr("a", 1)))
            .add(builder.update("foo").with(incr("b", 2)))
            .add(builder.update("foo").with(incr("c", 3)))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);

        // Test single increments
        query = "BEGIN COUNTER BATCH USING TIMESTAMP 42 ";
        query += "UPDATE foo SET a=a+1;";
        query += "UPDATE foo SET b=b+1;";
        query += "UPDATE foo SET c=c+1;";
        query += "APPLY BATCH;";
        batch = builder.batch()
            .add(builder.update("foo").with(incr("a")))
            .add(builder.update("foo").with(incr("b")))
            .add(builder.update("foo").with(incr("c")))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);

        // Test value decrements
        query = "BEGIN COUNTER BATCH USING TIMESTAMP 42 ";
        query += "UPDATE foo SET a=a-1;";
        query += "UPDATE foo SET b=b-2;";
        query += "UPDATE foo SET c=c-3;";
        query += "APPLY BATCH;";
        batch = builder.batch()
            .add(builder.update("foo").with(decr("a", 1)))
            .add(builder.update("foo").with(decr("b", 2)))
            .add(builder.update("foo").with(decr("c", 3)))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);

        // Test single decrements
        query = "BEGIN COUNTER BATCH USING TIMESTAMP 42 ";
        query += "UPDATE foo SET a=a-1;";
        query += "UPDATE foo SET b=b-1;";
        query += "UPDATE foo SET c=c-1;";
        query += "APPLY BATCH;";
        batch = builder.batch()
            .add(builder.update("foo").with(decr("a")))
            .add(builder.update("foo").with(decr("b")))
            .add(builder.update("foo").with(decr("c")))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);

        // Test negative decrements and negative increments
        query = "BEGIN COUNTER BATCH USING TIMESTAMP 42 ";
        query += "UPDATE foo SET a=a+1;";
        query += "UPDATE foo SET b=b+-2;";
        query += "UPDATE foo SET c=c-3;";
        query += "APPLY BATCH;";
        batch = builder.batch()
            .add(builder.update("foo").with(decr("a", -1)))
            .add(builder.update("foo").with(incr("b", -2)))
            .add(builder.update("foo").with(decr("c", 3)))
            .using(timestamp(42));
        assertEquals(batch.toString(), query);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void batchMixedCounterTest() throws Exception {
        builder.batch()
            .add(builder.update("foo").with(incr("a", 1)))
            .add(builder.update("foo").with(set("b", 2)))
            .add(builder.update("foo").with(incr("c", 3)))
            .using(timestamp(42));
    }

    @Test(groups = "unit")
    public void markerTest() throws Exception {
        String query;
        Statement insert;

        query = "INSERT INTO test(k,c) VALUES (0,?);";
        insert = builder.insertInto("test")
            .value("k", 0)
            .value("c", bindMarker());
        assertEquals(insert.toString(), query);
    }

    @Test(groups = "unit")
    public void rawEscapingTest() throws Exception {

        String query;
        Statement select;

        query = "SELECT * FROM t WHERE c='C''est la vie!';";
        select = builder.select().from("t").where(eq("c", "C'est la vie!"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM t WHERE c=C'est la vie!;";
        select = builder.select().from("t").where(eq("c", raw("C'est la vie!")));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM t WHERE c=now();";
        select = builder.select().from("t").where(eq("c", fcall("now")));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM t WHERE c='now()';";
        select = builder.select().from("t").where(eq("c", raw("'now()'")));
        assertEquals(select.toString(), query);
    }

    @Test(groups = "unit")
    public void selectInjectionTests() throws Exception {

        String query;
        Statement select;

        query = "SELECT * FROM \"foo WHERE k=4\";";
        select = builder.select().all().from("foo WHERE k=4");
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k='4 AND c=5';";
        select = builder.select().all().from("foo").where(eq("k", "4 AND c=5"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k='4'' AND c=''5';";
        select = builder.select().all().from("foo").where(eq("k", "4' AND c='5"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k='4'' OR ''1''=''1';";
        select = builder.select().all().from("foo").where(eq("k", "4' OR '1'='1"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k='4; --test comment;';";
        select = builder.select().all().from("foo").where(eq("k", "4; --test comment;"));
        assertEquals(select.toString(), query);

        query = "SELECT \"*\" FROM foo;";
        select = builder.select("*").from("foo");
        assertEquals(select.toString(), query);

        query = "SELECT a,b FROM foo WHERE a IN ('b','c''); --comment');";
        select = builder.select("a", "b").from("foo").where(in("a", "b", "c'); --comment"));
        assertEquals(select.toString(), query);

        // User Injection?
        query = "SELECT * FROM bar; --(b) FROM foo;";
        select = builder.select().fcall("* FROM bar; --", column("b")).from("foo");
        assertEquals(select.toString(), query);

        query = "SELECT writetime(\"a) FROM bar; --\"),ttl(a) FROM foo ALLOW FILTERING;";
        select = builder.select().writeTime("a) FROM bar; --").ttl("a").from("foo").allowFiltering();
        assertEquals(select.toString(), query);

        query = "SELECT writetime(a),ttl(\"a) FROM bar; --\") FROM foo ALLOW FILTERING;";
        select = builder.select().writeTime("a").ttl("a) FROM bar; --").from("foo").allowFiltering();
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE \"k=1 OR k\">42 LIMIT 42;";
        select = builder.select().all().from("foo").where(gt("k=1 OR k", 42)).limit(42);
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE token(\"k)>0 OR token(k\")>token(42);";
        select = builder.select().all().from("foo").where(gt(token("k)>0 OR token(k"), fcall("token", 42)));
        assertEquals(select.toString(), query);
    }

    @Test(groups = "unit")
    @SuppressWarnings("serial")
    public void insertInjectionTest() throws Exception {

        String query;
        Statement insert;

        query = "INSERT INTO foo(a) VALUES ('123); --comment');";
        insert = builder.insertInto("foo").value("a", "123); --comment");
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo(\"a,b\") VALUES (123);";
        insert = builder.insertInto("foo").value("a,b", 123);
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo(a,b) VALUES ({'2''} space','3','4'},3.4) USING TTL 24 AND TIMESTAMP 42;";
        insert = builder.insertInto("foo").values(new String[]{ "a", "b" }, new Object[]{ new TreeSet<String>() {{
            add("2'} space");
            add("3");
            add("4");
        }}, 3.4 }).using(ttl(24)).and(timestamp(42));
        assertEquals(insert.toString(), query);
    }

    @Test(groups = "unit")
    public void updateInjectionTest() throws Exception {

        String query;
        Statement update;

        query = "UPDATE foo.bar USING TIMESTAMP 42 SET a=12 WHERE k='2 OR 1=1';";
        update = builder.update("foo", "bar").using(timestamp(42)).with(set("a", 12)).where(eq("k", "2 OR 1=1"));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET b='null WHERE k=1; --comment' WHERE k=2;";
        update = builder.update("foo").where().and(eq("k", 2)).with(set("b", "null WHERE k=1; --comment"));
        assertEquals(update.toString(), query);

        query = "UPDATE foo USING TIMESTAMP 42 SET \"b WHERE k=1; --comment\"=[3,2,1]+\"b WHERE k=1; --comment\" WHERE k=2;";
        update = builder.update("foo").where().and(eq("k", 2)).with(prependAll("b WHERE k=1; --comment", Arrays.asList(3, 2, 1))).using(timestamp(42));
        assertEquals(update.toString(), query);
    }

    @Test(groups = "unit")
    public void deleteInjectionTests() throws Exception {

        String query;
        Statement delete;

        query = "DELETE FROM \"foo WHERE k=4\";";
        delete = builder.delete().from("foo WHERE k=4");
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE k='4 AND c=5';";
        delete = builder.delete().from("foo").where(eq("k", "4 AND c=5"));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE k='4'' AND c=''5';";
        delete = builder.delete().from("foo").where(eq("k", "4' AND c='5"));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE k='4'' OR ''1''=''1';";
        delete = builder.delete().from("foo").where(eq("k", "4' OR '1'='1"));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE k='4; --test comment;';";
        delete = builder.delete().from("foo").where(eq("k", "4; --test comment;"));
        assertEquals(delete.toString(), query);

        query = "DELETE \"*\" FROM foo;";
        delete = builder.delete("*").from("foo");
        assertEquals(delete.toString(), query);

        query = "DELETE a,b FROM foo WHERE a IN ('b','c''); --comment');";
        delete = builder.delete("a", "b").from("foo")
            .where(in("a", "b", "c'); --comment"));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE \"k=1 OR k\">42;";
        delete = builder.delete().from("foo").where(gt("k=1 OR k", 42));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE token(\"k)>0 OR token(k\")>token(42);";
        delete = builder.delete().from("foo").where(gt(token("k)>0 OR token(k"), fcall("token", 42)));
        assertEquals(delete.toString(), query);
    }

    @Test(groups = "unit")
    public void statementForwardingTest() throws Exception {

        Update upd = builder.update("foo");
        upd.setConsistencyLevel(ConsistencyLevel.QUORUM);
        upd.enableTracing();

        Statement query = upd.using(timestamp(42)).with(set("a", 12)).and(incr("c", 3)).where(eq("k", 2));

        assertEquals(query.getConsistencyLevel(), ConsistencyLevel.QUORUM);
        assertTrue(query.isTracing());
    }

    @Test(groups = "unit")
    public void rejectUnknownValueTest() throws Exception {
        try {
            //noinspection ResultOfMethodCallIgnored
            builder.update("foo").with(set("a", new byte[13])).where(eq("k", 2)).toString();
            fail("Byte arrays should not be valid, ByteBuffer should be used instead");
        } catch (CodecNotFoundException e) {
            // Ok, that's what we expect
        }
    }

    @Test(groups = "unit")
    public void truncateTest() throws Exception {
        assertEquals(builder.truncate("foo").toString(), "TRUNCATE foo;");
        assertEquals(builder.truncate("foo", quote("Bar")).toString(), "TRUNCATE foo.\"Bar\";");
    }

    @Test(groups = "unit")
    public void quotingTest() {
        assertEquals(builder.select().from("Metrics", "epochs").toString(),
            "SELECT * FROM Metrics.epochs;");
        assertEquals(builder.select().from("Metrics", quote("epochs")).toString(),
            "SELECT * FROM Metrics.\"epochs\";");
        assertEquals(builder.select().from(quote("Metrics"), "epochs").toString(),
            "SELECT * FROM \"Metrics\".epochs;");
        assertEquals(builder.select().from(quote("Metrics"), quote("epochs")).toString(),
            "SELECT * FROM \"Metrics\".\"epochs\";");

        assertEquals(builder.insertInto("Metrics", "epochs").toString(),
            "INSERT INTO Metrics.epochs() VALUES ();");
        assertEquals(builder.insertInto("Metrics", quote("epochs")).toString(),
            "INSERT INTO Metrics.\"epochs\"() VALUES ();");
        assertEquals(builder.insertInto(quote("Metrics"), "epochs").toString(),
            "INSERT INTO \"Metrics\".epochs() VALUES ();");
        assertEquals(builder.insertInto(quote("Metrics"), quote("epochs")).toString(),
            "INSERT INTO \"Metrics\".\"epochs\"() VALUES ();");
    }

    @Test(groups = "unit")
    public void compoundWhereClauseTest() throws Exception {
        String query;
        Statement select;

        query = "SELECT * FROM foo WHERE k=4 AND (c1,c2)>('a',2);";
        select = builder.select().all().from("foo").where(eq("k", 4)).and(gt(Arrays.asList("c1", "c2"), Arrays.<Object>asList("a", 2)));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k=4 AND (c1,c2)>=('a',2) AND (c1,c2)<('b',0);";
        select = builder.select().all().from("foo").where(eq("k", 4)).and(gte(Arrays.asList("c1", "c2"), Arrays.<Object>asList("a", 2)))
            .and(lt(Arrays.asList("c1", "c2"), Arrays.<Object>asList("b", 0)));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k=4 AND (c1,c2)<=('a',2);";
        select = builder.select().all().from("foo").where(eq("k", 4)).and(lte(Arrays.asList("c1", "c2"), Arrays.<Object>asList("a", 2)));
        assertEquals(select.toString(), query);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_if_in_clause_has_too_many_values() {
        List<Object> values = Collections.<Object>nCopies(65536, "a");
        builder.select().all().from("foo").where(in("bar", values.toArray()));
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_if_built_statement_has_too_many_values() {
        List<Object> values = Collections.<Object>nCopies(65535, "a");

        // If the excessive count results from successive DSL calls, we don't check it on the fly so this statement works:
        BuiltStatement statement = builder.select().all().from("foo")
            .where(eq("bar", "a"))
            .and(in("baz", values.toArray()));

        // But we still want to check it client-side, to fail fast instead of sending a bad query to Cassandra.
        // getValues() is called on any RegularStatement before we send it (see SessionManager.makeRequestMessage).
        statement.getValues();
    }

    @Test(groups = "unit")
    @CassandraVersion(major = 2.1, minor = 3)
    public void should_handle_nested_collections() {
        String query;
        Statement statement;

        query = "UPDATE foo SET l=[[1],[2]] WHERE k=1;";
        ImmutableList<ImmutableList<Integer>> list = ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2));
        statement = builder.update("foo").with(set("l", list)).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);

        query = "UPDATE foo SET m={1:[[1],[2]],2:[[1],[2]]} WHERE k=1;";
        statement = builder.update("foo").with(set("m", ImmutableMap.of(1, list, 2, list))).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);

        query = "UPDATE foo SET m=m+{1:[[1],[2]],2:[[1],[2]]} WHERE k=1;";
        statement = builder.update("foo").with(putAll("m", ImmutableMap.of(1, list, 2, list))).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);

        query = "UPDATE foo SET l=[[1]]+l WHERE k=1;";
        statement = builder.update("foo").with(prepend("l", ImmutableList.of(1))).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);

        query = "UPDATE foo SET l=[[1],[2]]+l WHERE k=1;";
        statement = builder.update("foo").with(prependAll("l", list)).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);
    }

    @Test(groups = "unit", expectedExceptions = InvalidQueryException.class)
    public void should_not_allow_bind_marker_for_add() {
        // This generates the query "UPDATE foo SET s = s + {?} WHERE k = 1", which is invalid in Cassandra
        builder.update("foo").with(add("s", bindMarker())).where(eq("k", 1));
    }

    @Test(groups = "unit", expectedExceptions = InvalidQueryException.class)
    public void should_now_allow_bind_marker_for_prepend() {
        builder.update("foo").with(prepend("l", bindMarker())).where(eq("k", 1));
    }

    @Test(groups = "unit", expectedExceptions = InvalidQueryException.class)
    public void should_not_allow_bind_marker_for_append() {
        builder.update("foo").with(append("l", bindMarker())).where(eq("k", 1));
    }

    @Test(groups = "unit", expectedExceptions = InvalidQueryException.class)
    public void should_not_allow_bind_marker_for_remove() {
        builder.update("foo").with(remove("s", bindMarker())).where(eq("k", 1));
    }

    @Test(groups = "unit", expectedExceptions = InvalidQueryException.class)
    public void should_not_allow_bind_marker_for_discard() {
        builder.update("foo").with(discard("l", bindMarker())).where(eq("k", 1));
    }

    @Test(groups = "unit")
    public void should_quote_complex_column_names() {
        // A column name can be anything as long as it's quoted, so "foo.bar" is a valid name
        String query = "SELECT * FROM foo WHERE \"foo.bar\"=1;";
        Statement statement = builder.select().from("foo").where(eq(quote("foo.bar"), 1));

        assertThat(statement.toString()).isEqualTo(query);
    }

    @Test(groups = "unit")
    public void should_quote_column_names_with_escaped_quotes() {
        // A column name can include quotes as long as it is escaped with another set of quotes, so "foo""bar" is a valid name.
        String query = "SELECT * FROM foo WHERE \"foo \"\" bar\"=1;";
        Statement statement = builder.select().from("foo").where(eq(quote("foo \"\" bar"), 1));

        assertThat(statement.toString()).isEqualTo(query);
    }

    @Test(groups = "unit")
    public void should_not_serialize_raw_query_values() {
        RegularStatement select = builder.select().from("test").where(gt("i", raw("1")));
        assertThat(select.getQueryString()).doesNotContain("?");
        assertThat(select.getValues()).isNull();
    }

    @Test(groups = "unit", expectedExceptions = { IllegalStateException.class })
    public void should_throw_ISE_if_getObject_called_on_statement_without_values() {
        builder.select().from("test").where(eq("foo", 42)).getObject(0); // integers are appended to the CQL string
    }

    @Test(groups = "unit", expectedExceptions = { IndexOutOfBoundsException.class })
    public void should_throw_IOOBE_if_getObject_called_with_wrong_index() {
        builder.select().from("test").where(eq("foo", new Object())).getObject(1);
    }

    @Test(groups = "unit")
    public void should_return_object_at_ith_index() {
        Object expected = new Object();
        Object actual = builder.select().from("test").where(eq("foo", expected)).getObject(0);
        assertThat(actual).isSameAs(expected);
    }

}
