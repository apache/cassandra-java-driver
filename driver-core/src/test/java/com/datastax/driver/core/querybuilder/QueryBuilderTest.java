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
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class QueryBuilderTest {

    @Test(groups = "unit")
    public void selectTest() throws Exception {

        String query;
        Statement select;

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

        query = "SELECT DISTINCT longName AS a,ttl(longName) AS ttla FROM foo LIMIT :limit;";
        select = select().distinct().column("longName").as("a").ttl("longName").as("ttla").from("foo").limit(bindMarker("limit"));
        assertEquals(select.toString(), query);

        query = "SELECT DISTINCT longName AS a,ttl(longName) AS ttla FROM foo WHERE k IN () LIMIT :limit;";
        select = select().distinct().column("longName").as("a").ttl("longName").as("ttla").from("foo").where(in("k")).limit(bindMarker("limit"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE bar=:barmark AND baz=:bazmark LIMIT :limit;";
        select = select().all().from("foo").where().and(eq("bar", bindMarker("barmark"))).and(eq("baz", bindMarker("bazmark"))).limit(bindMarker("limit"));
        assertEquals(select.toString(), query);

        query = "SELECT a FROM foo WHERE k IN ();";
        select = select("a").from("foo").where(in("k"));
        assertEquals(select.toString(), query);

        query = "SELECT a FROM foo WHERE k IN ?;";
        select = select("a").from("foo").where(in("k", bindMarker()));
        assertEquals(select.toString(), query);

        query = "SELECT DISTINCT a FROM foo WHERE k=1;";
        select = select("a").distinct().from("foo").where(eq("k", 1));
        assertEquals(select.toString(), query);

        query = "SELECT DISTINCT a,b FROM foo WHERE k=1;";
        select = select("a", "b").distinct().from("foo").where(eq("k", 1));
        assertEquals(select.toString(), query);

        query = "SELECT count(*) FROM foo;";
        select = select().countAll().from("foo");
        assertEquals(select.toString(), query);

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

        Date date = new Date();
        date.setTime(1234325);
        query = "SELECT * FROM foo WHERE d=1234325;";
        select = select().all().from("foo").where(eq("d", date));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE b=0xcafebabe;";
        select = select().all().from("foo").where(eq("b", Bytes.fromHexString("0xCAFEBABE")));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE e CONTAINS 'text';";
        select = select().from("foo").where(contains("e", "text"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE e CONTAINS KEY 'key1';";
        select = select().from("foo").where(containsKey("e", "key1"));
        assertEquals(select.toString(), query);

        query = "SELECT CAST(writetime(country) AS text) FROM artists LIMIT 2;";
        select = select().cast(fcall("writetime", column("country")), DataType.text()).from("artists").limit(2);
        assertEquals(select.toString(), query);

        query = "SELECT avg(CAST(v AS float)) FROM e;";
        select = select().fcall("avg", cast(column("v"), DataType.cfloat())).from("e");
        assertEquals(select.toString(), query);

        query = "SELECT CAST(writetime(country) AS text) FROM artists LIMIT 2;";
        select = select().raw("CAST(writetime(country) AS text)").from("artists").limit(2);
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE e LIKE 'a%';";
        select = select().from("foo").where(like("e", "a%"));
        assertEquals(select.toString(), query);

        try {
            select().countAll().from("foo").orderBy(asc("a"), desc("b")).orderBy(asc("a"), desc("b"));
            fail("Expected an IllegalStateException");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "An ORDER BY clause has already been provided");
        }

        try {
            select().column("a").all().from("foo");
            fail("Expected an IllegalStateException");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Some columns ([a]) have already been selected.");
        }

        try {
            select().column("a").countAll().from("foo");
            fail("Expected an IllegalStateException");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Some columns ([a]) have already been selected.");
        }

        try {
            select().all().from("foo").limit(-42);
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid LIMIT value, must be strictly positive");
        }

        try {
            select().all().from("foo").limit(42).limit(42);
            fail("Expected an IllegalStateException");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "A LIMIT value has already been provided");
        }
    }

    @Test(groups = "unit")
    @SuppressWarnings({"serial", "deprecation"})
    public void insertTest() throws Exception {

        String query;
        Statement insert;

        query = "INSERT INTO foo (a,b,\"C\",d) VALUES (123,'127.0.0.1','foo''bar',{'x':3,'y':2}) USING TIMESTAMP 42 AND TTL 24;";
        insert = insertInto("foo")
                .value("a", 123)
                .value("b", InetAddress.getByName("127.0.0.1"))
                .value(quote("C"), "foo'bar")
                .value("d", new TreeMap<String, Integer>() {{
                    put("x", 3);
                    put("y", 2);
                }})
                .using(timestamp(42)).and(ttl(24));
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo (a,b) VALUES (2,null);";
        insert = insertInto("foo")
                .value("a", 2)
                .value("b", null);
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo (a,b) VALUES ({2,3,4},3.4) USING TTL 24 AND TIMESTAMP 42;";
        insert = insertInto("foo").values(new String[]{"a", "b"}, new Object[]{new TreeSet<Integer>() {{
            add(2);
            add(3);
            add(4);
        }}, 3.4}).using(ttl(24)).and(timestamp(42));
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo.bar (a,b) VALUES ({2,3,4},3.4) USING TTL ? AND TIMESTAMP ?;";
        insert = insertInto("foo", "bar")
                .values(new String[]{"a", "b"}, new Object[]{new TreeSet<Integer>() {{
                    add(2);
                    add(3);
                    add(4);
                }}, 3.4})
                .using(ttl(bindMarker()))
                .and(timestamp(bindMarker()));
        assertEquals(insert.toString(), query);

        // commutative result of TIMESTAMP
        query = "INSERT INTO foo.bar (a,b,c) VALUES ({2,3,4},3.4,123) USING TIMESTAMP 42;";
        insert = insertInto("foo", "bar")
                .using(timestamp(42))
                .values(new String[]{"a", "b"}, new Object[]{new TreeSet<Integer>() {{
                    add(2);
                    add(3);
                    add(4);
                }}, 3.4})
                .value("c", 123);
        assertEquals(insert.toString(), query);

        // commutative result of value() and values()
        query = "INSERT INTO foo (c,a,b) VALUES (123,{2,3,4},3.4) USING TIMESTAMP 42;";
        insert = insertInto("foo")
                .using(timestamp(42))
                .value("c", 123)
                .values(new String[]{"a", "b"}, new Object[]{new TreeSet<Integer>() {{
                    add(2);
                    add(3);
                    add(4);
                }}, 3.4});
        assertEquals(insert.toString(), query);

        try {
            insertInto("foo").values(new String[]{"a", "b"}, new Object[]{1, 2, 3});
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Got 2 names but 3 values");
        }

        // CAS test
        query = "INSERT INTO foo (k,x) VALUES (0,1) IF NOT EXISTS;";
        insert = insertInto("foo").value("k", 0).value("x", 1).ifNotExists();
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
        update = update("foo").with(discardAll("b", Arrays.asList(1, 2, 3))).and(add("c", 1)).and(addAll("d", new TreeSet<Integer>() {{
            add(2);
            add(3);
            add(4);
        }}));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET b=b-{2,3,4},c['k']='v',d=d+{'x':3,'y':2};";
        update = update("foo").with(removeAll("b", new TreeSet<Integer>() {{
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
        update = update("foo").using(ttl(400));
        assertEquals(update.toString(), query);

        query = "UPDATE foo SET a=" + new BigDecimal(3.2) + ",b=42 WHERE k=2;";
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
            update("foo").using(ttl(-400));
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid ttl, must be positive");
        }

        // CAS test
        query = "UPDATE foo SET x=4 WHERE k=0 IF x=1;";
        update = update("foo").with(set("x", 4)).where(eq("k", 0)).onlyIf(eq("x", 1));
        assertEquals(update.toString(), query);

        // IF EXISTS CAS test
        update = update("foo").with(set("x", 3)).where(eq("k", 2)).ifExists();
        assertThat(update.toString()).isEqualTo("UPDATE foo SET x=3 WHERE k=2 IF EXISTS;");
    }

    @Test(groups = "unit")
    public void deleteTest() throws Exception {

        String query;
        Statement delete;

        query = "DELETE a,b,c FROM foo USING TIMESTAMP 0 WHERE k=1;";
        delete = delete("a", "b", "c").from("foo").using(timestamp(0)).where(eq("k", 1));
        assertEquals(delete.toString(), query);

        query = "DELETE a[3],b['foo'],c FROM foo WHERE k=1;";
        delete = delete().listElt("a", 3).mapElt("b", "foo").column("c").from("foo").where(eq("k", 1));
        assertEquals(delete.toString(), query);

        query = "DELETE a[?],b[?],c FROM foo WHERE k=1;";
        delete = delete().listElt("a", bindMarker()).mapElt("b", bindMarker()).column("c").from("foo").where(eq("k", 1));
        assertEquals(delete.toString(), query);

        // Invalid CQL, testing edge case
        query = "DELETE a,b,c FROM foo;";
        delete = delete("a", "b", "c").from("foo");
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo USING TIMESTAMP 1240003134 WHERE k='value';";
        delete = delete().all().from("foo").using(timestamp(1240003134L)).where(eq("k", "value"));
        assertEquals(delete.toString(), query);
        delete = delete().from("foo").using(timestamp(1240003134L)).where(eq("k", "value"));
        assertEquals(delete.toString(), query);

        query = "DELETE a,b,c FROM foo.bar USING TIMESTAMP 1240003134 WHERE k=1;";
        delete = delete("a", "b", "c").from("foo", "bar").where().and(eq("k", 1)).using(timestamp(1240003134L));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo.bar WHERE k1='foo' AND k2=1;";
        delete = delete().from("foo", "bar").where(eq("k1", "foo")).and(eq("k2", 1));
        assertEquals(delete.toString(), query);

        try {
            delete().column("a").all().from("foo");
            fail("Expected an IllegalStateException");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Some columns ([a]) have already been selected.");
        }

        try {
            delete().from("foo").using(timestamp(-1240003134L));
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid timestamp, must be positive");
        }

        query = "DELETE FROM foo.bar WHERE k1='foo' IF EXISTS;";
        delete = delete().from("foo", "bar").where(eq("k1", "foo")).ifExists();
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo.bar WHERE k1='foo' IF a=1 AND b=2;";
        delete = delete().from("foo", "bar").where(eq("k1", "foo")).onlyIf(eq("a", 1)).and(eq("b", 2));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE k=:key;";
        delete = delete().from("foo").where(eq("k", bindMarker("key")));
        assertEquals(delete.toString(), query);
    }

    @Test(groups = "unit")
    @SuppressWarnings("serial")
    public void batchTest() throws Exception {
        String query;
        Statement batch;

        query = "BEGIN BATCH USING TIMESTAMP 42 ";
        query += "INSERT INTO foo (a,b) VALUES ({2,3,4},3.4);";
        query += "UPDATE foo SET a[2]='foo',b=[3,2,1]+b,c=c-{'a'} WHERE k=2;";
        query += "DELETE a[3],b['foo'],c FROM foo WHERE k=1;";
        query += "APPLY BATCH;";
        batch = batch()
                .add(insertInto("foo").values(new String[]{"a", "b"}, new Object[]{new TreeSet<Integer>() {{
                    add(2);
                    add(3);
                    add(4);
                }}, 3.4}))
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

        assertEquals(batch().toString(), "BEGIN BATCH APPLY BATCH;");
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

    @Test(groups = "unit", expectedExceptions = {IllegalArgumentException.class})
    public void batchMixedCounterTest() throws Exception {
        batch()
                .add(update("foo").with(incr("a", 1)))
                .add(update("foo").with(set("b", 2)))
                .add(update("foo").with(incr("c", 3)))
                .using(timestamp(42));
    }

    @Test(groups = "unit")
    public void markerTest() throws Exception {
        String query;
        Statement insert;

        query = "INSERT INTO test (k,c) VALUES (0,?);";
        insert = insertInto("test")
                .value("k", 0)
                .value("c", bindMarker());
        assertEquals(insert.toString(), query);
    }

    @Test(groups = "unit")
    public void rawEscapingTest() throws Exception {

        String query;
        Statement select;

        query = "SELECT * FROM t WHERE c='C''est la vie!';";
        select = select().from("t").where(eq("c", "C'est la vie!"));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM t WHERE c=C'est la vie!;";
        select = select().from("t").where(eq("c", raw("C'est la vie!")));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM t WHERE c=now();";
        select = select().from("t").where(eq("c", fcall("now")));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM t WHERE c='now()';";
        select = select().from("t").where(eq("c", raw("'now()'")));
        assertEquals(select.toString(), query);
    }

    @Test(groups = "unit")
    public void selectInjectionTests() throws Exception {

        String query;
        Statement select;

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
    @SuppressWarnings("serial")
    public void insertInjectionTest() throws Exception {

        String query;
        Statement insert;

        query = "INSERT INTO foo (a) VALUES ('123); --comment');";
        insert = insertInto("foo").value("a", "123); --comment");
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo (\"a,b\") VALUES (123);";
        insert = insertInto("foo").value("a,b", 123);
        assertEquals(insert.toString(), query);

        query = "INSERT INTO foo (a,b) VALUES ({'2''} space','3','4'},3.4) USING TTL 24 AND TIMESTAMP 42;";
        insert = insertInto("foo").values(new String[]{"a", "b"}, new Object[]{new TreeSet<String>() {{
            add("2'} space");
            add("3");
            add("4");
        }}, 3.4}).using(ttl(24)).and(timestamp(42));
        assertEquals(insert.toString(), query);
    }

    @Test(groups = "unit")
    public void updateInjectionTest() throws Exception {

        String query;
        Statement update;

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
        Statement delete;

        query = "DELETE FROM \"foo WHERE k=4\";";
        delete = delete().from("foo WHERE k=4");
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE k='4 AND c=5';";
        delete = delete().from("foo").where(eq("k", "4 AND c=5"));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE k='4'' AND c=''5';";
        delete = delete().from("foo").where(eq("k", "4' AND c='5"));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE k='4'' OR ''1''=''1';";
        delete = delete().from("foo").where(eq("k", "4' OR '1'='1"));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE k='4; --test comment;';";
        delete = delete().from("foo").where(eq("k", "4; --test comment;"));
        assertEquals(delete.toString(), query);

        query = "DELETE \"*\" FROM foo;";
        delete = delete("*").from("foo");
        assertEquals(delete.toString(), query);

        query = "DELETE a,b FROM foo WHERE a IN ('b','c''); --comment');";
        delete = delete("a", "b").from("foo")
                .where(in("a", "b", "c'); --comment"));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE \"k=1 OR k\">42;";
        delete = delete().from("foo").where(gt("k=1 OR k", 42));
        assertEquals(delete.toString(), query);

        query = "DELETE FROM foo WHERE token(\"k)>0 OR token(k\")>token(42);";
        delete = delete().from("foo").where(gt(token("k)>0 OR token(k"), fcall("token", 42)));
        assertEquals(delete.toString(), query);
    }

    @Test(groups = "unit")
    public void statementForwardingTest() throws Exception {

        Update upd = update("foo");
        upd.setConsistencyLevel(ConsistencyLevel.QUORUM);
        upd.enableTracing();

        Statement query = upd.using(timestamp(42)).with(set("a", 12)).and(incr("c", 3)).where(eq("k", 2));

        assertEquals(query.getConsistencyLevel(), ConsistencyLevel.QUORUM);
        assertTrue(query.isTracing());
    }

    @Test(groups = "unit", expectedExceptions = CodecNotFoundException.class)
    public void rejectUnknownValueTest() throws Exception {
        RegularStatement s = update("foo").with(set("a", new byte[13])).where(eq("k", 2))
                .setForceNoValues(true);
        s.getQueryString();
    }

    @Test(groups = "unit")
    public void truncateTest() throws Exception {
        assertEquals(truncate("foo").toString(), "TRUNCATE foo;");
        assertEquals(truncate("foo", quote("Bar")).toString(), "TRUNCATE foo.\"Bar\";");
    }

    @Test(groups = "unit")
    public void quotingTest() {
        assertEquals(select().from("Metrics", "epochs").toString(),
                "SELECT * FROM Metrics.epochs;");
        assertEquals(select().from("Metrics", quote("epochs")).toString(),
                "SELECT * FROM Metrics.\"epochs\";");
        assertEquals(select().from(quote("Metrics"), "epochs").toString(),
                "SELECT * FROM \"Metrics\".epochs;");
        assertEquals(select().from(quote("Metrics"), quote("epochs")).toString(),
                "SELECT * FROM \"Metrics\".\"epochs\";");

        assertEquals(insertInto("Metrics", "epochs").toString(),
                "INSERT INTO Metrics.epochs () VALUES ();");
        assertEquals(insertInto("Metrics", quote("epochs")).toString(),
                "INSERT INTO Metrics.\"epochs\" () VALUES ();");
        assertEquals(insertInto(quote("Metrics"), "epochs").toString(),
                "INSERT INTO \"Metrics\".epochs () VALUES ();");
        assertEquals(insertInto(quote("Metrics"), quote("epochs")).toString(),
                "INSERT INTO \"Metrics\".\"epochs\" () VALUES ();");
    }

    @Test(groups = "unit")
    public void compoundWhereClauseTest() throws Exception {
        String query;
        Statement select;

        query = "SELECT * FROM foo WHERE k=4 AND (c1,c2)=('a',2);";
        select = select().all().from("foo").where(eq("k", 4)).and(eq(Arrays.asList("c1", "c2"), Arrays.<Object>asList("a", 2)));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k=4 AND (c1,c2)>('a',2);";
        select = select().all().from("foo").where(eq("k", 4)).and(gt(Arrays.asList("c1", "c2"), Arrays.<Object>asList("a", 2)));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k=4 AND (c1,c2)>=('a',2) AND (c1,c2)<('b',0);";
        select = select().all().from("foo").where(eq("k", 4)).and(gte(Arrays.asList("c1", "c2"), Arrays.<Object>asList("a", 2)))
                .and(lt(Arrays.asList("c1", "c2"), Arrays.<Object>asList("b", 0)));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k=4 AND (c1,c2)<=('a',2);";
        select = select().all().from("foo").where(eq("k", 4)).and(lte(Arrays.asList("c1", "c2"), Arrays.<Object>asList("a", 2)));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k=4 AND (c1,c2) IN ((1,'foo'),(2,'bar'),(3,'qix'));";
        List<String> names = ImmutableList.of("c1", "c2");
        List<?> values = ImmutableList.<List<?>>of(
                ImmutableList.of(1, "foo"),
                ImmutableList.of(2, "bar"),
                ImmutableList.of(3, "qix"));
        select = select().all().from("foo").where(eq("k", 4)).and(in(names, values));
        assertEquals(select.toString(), query);

        query = "SELECT * FROM foo WHERE k=4 AND (c1,c2) IN ((1,'foo'),(2,?),?);";
        names = ImmutableList.of("c1", "c2");
        values = ImmutableList.of(
                ImmutableList.of(1, "foo"),
                ImmutableList.of(2, bindMarker()),
                bindMarker());
        select = select().all().from("foo").where(eq("k", 4)).and(in(names, values));
        assertEquals(select.toString(), query);

        // special case, single element list with bind marker should be (?) instead of ((?))
        query = "SELECT * FROM foo WHERE k=4 AND (c1) IN (?);";
        names = ImmutableList.of("c1");
        values = ImmutableList.of(ImmutableList.of(bindMarker()));
        select = select().all().from("foo").where(eq("k", 4)).and(in(names, values));
        assertEquals(select.toString(), query);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Too many values for IN clause, the maximum allowed is 65535")
    public void should_fail_if_compound_in_clause_has_too_many_values() {
        List<Object> values = Collections.<Object>nCopies(65536, "a");
        select().all().from("foo").where(eq("k", 4)).and(in(ImmutableList.of("name"), values));
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Missing values for IN clause")
    public void should_fail_if_compound_in_clause_given_null_values() {
        select().all().from("foo").where(eq("k", 4)).and(in(ImmutableList.of("name"), null));
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The number of names \\(4\\) and values \\(3\\) don't match")
    public void should_fail_if_compound_in_clause_has_mismatch_of_names_and_values() {
        select().all().from("foo").where(eq("k", 4)).and(in(ImmutableList.of("a", "b", "c", "d"),
                ImmutableList.of(
                        ImmutableList.of(1, 2, 3, 4), // Adequately sized (4)
                        ImmutableList.of(1, 2, 3) // Inadequately sized (3)
                )));
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Wrong element type for values list, expected List or BindMarker, got java.lang.Integer")
    public void shoud_fail_if_compound_in_clause_has_value_pair_that_is_not_list_or_bind_marker() {
        select().all().from("foo").where(eq("k", 4)).and(in(ImmutableList.of("a", "b", "c", "d"),
                ImmutableList.of(1))); // Invalid value 1, must be list or bind marker.
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Missing values for IN clause")
    public void should_fail_if_in_clause_has_null_values() {
        select().all().from("foo").where(in("bar", (List<?>) null));
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_if_in_clause_has_too_many_values() {
        List<Object> values = Collections.<Object>nCopies(65536, "a");
        select().all().from("foo").where(in("bar", values.toArray()));
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_if_built_statement_has_too_many_values() {
        List<Object> values = Collections.<Object>nCopies(65535, "a");

        // If the excessive count results from successive DSL calls, we don't check it on the fly so this statement works:
        BuiltStatement statement = select().all().from("foo")
                .where(eq("bar", "a"))
                .and(in("baz", values.toArray()));

        // But we still want to check it client-side, to fail fast instead of sending a bad query to Cassandra.
        // getValues() is called on any RegularStatement before we send it (see SessionManager.makeRequestMessage).
        statement.getValues(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE);
    }

    @Test(groups = "unit")
    public void should_handle_nested_collections() {
        String query;
        Statement statement;

        query = "UPDATE foo SET l=[[1],[2]] WHERE k=1;";
        ImmutableList<ImmutableList<Integer>> list = ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2));
        statement = update("foo").with(set("l", list)).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);

        query = "UPDATE foo SET m={1:[[1],[2]],2:[[1],[2]]} WHERE k=1;";
        statement = update("foo").with(set("m", ImmutableMap.of(1, list, 2, list))).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);

        query = "UPDATE foo SET m=m+{1:[[1],[2]],2:[[1],[2]]} WHERE k=1;";
        statement = update("foo").with(putAll("m", ImmutableMap.of(1, list, 2, list))).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);

        query = "UPDATE foo SET l=[[1]]+l WHERE k=1;";
        statement = update("foo").with(prepend("l", ImmutableList.of(1))).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);

        query = "UPDATE foo SET l=[[1],[2]]+l WHERE k=1;";
        statement = update("foo").with(prependAll("l", list)).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);
    }

    @Test(groups = "unit", expectedExceptions = InvalidQueryException.class)
    public void should_not_allow_bind_marker_for_add() {
        // This generates the query "UPDATE foo SET s = s + {?} WHERE k = 1", which is invalid in Cassandra
        update("foo").with(add("s", bindMarker())).where(eq("k", 1));
    }

    @Test(groups = "unit", expectedExceptions = InvalidQueryException.class)
    public void should_now_allow_bind_marker_for_prepend() {
        update("foo").with(prepend("l", bindMarker())).where(eq("k", 1));
    }

    @Test(groups = "unit", expectedExceptions = InvalidQueryException.class)
    public void should_not_allow_bind_marker_for_append() {
        update("foo").with(append("l", bindMarker())).where(eq("k", 1));
    }

    @Test(groups = "unit", expectedExceptions = InvalidQueryException.class)
    public void should_not_allow_bind_marker_for_remove() {
        update("foo").with(remove("s", bindMarker())).where(eq("k", 1));
    }

    @Test(groups = "unit", expectedExceptions = InvalidQueryException.class)
    public void should_not_allow_bind_marker_for_discard() {
        update("foo").with(discard("l", bindMarker())).where(eq("k", 1));
    }

    @Test(groups = "unit")
    public void should_quote_complex_column_names() {
        // A column name can be anything as long as it's quoted, so "foo.bar" is a valid name
        String query = "SELECT * FROM foo WHERE \"foo.bar\"=1;";
        Statement statement = select().from("foo").where(eq(quote("foo.bar"), 1));

        assertThat(statement.toString()).isEqualTo(query);
    }

    @Test(groups = "unit")
    public void should_quote_column_names_with_escaped_quotes() {
        // A column name can include quotes as long as it is escaped with another set of quotes, so "foo""bar" is a valid name.
        String query = "SELECT * FROM foo WHERE \"foo \"\" bar\"=1;";
        Statement statement = select().from("foo").where(eq(quote("foo \" bar"), 1));

        assertThat(statement.toString()).isEqualTo(query);
    }

    @Test(groups = "unit")
    public void should_not_serialize_raw_query_values() {
        RegularStatement select = select().from("test").where(gt("i", raw("1")));
        assertThat(select.getQueryString()).doesNotContain("?");
        assertThat(select.getValues(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE)).isNull();
    }

    @Test(groups = "unit", expectedExceptions = {IllegalStateException.class})
    public void should_throw_ISE_if_getObject_called_on_statement_without_values() {
        select().from("test").where(eq("foo", 42)).getObject(0); // integers are appended to the CQL string
    }

    @Test(groups = "unit", expectedExceptions = {IndexOutOfBoundsException.class})
    public void should_throw_IOOBE_if_getObject_called_with_wrong_index() {
        select().from("test").where(eq("foo", new Object())).getObject(1);
    }

    @Test(groups = "unit")
    public void should_return_object_at_ith_index() {
        Object expected = new Object();
        Object actual = select().from("test").where(eq("foo", expected)).getObject(0);
        assertThat(actual).isSameAs(expected);
    }

    @Test(groups = "unit")
    public void should_serialize_collections_of_serializable_elements() {
        Set<UUID> set = Sets.newHashSet(UUID.randomUUID());
        List<Date> list = Lists.newArrayList(new Date());
        Map<BigInteger, String> map = ImmutableMap.of(new BigInteger("1"), "foo");
        BuiltStatement query = insertInto("foo").value("v", set);
        assertThat(query.getQueryString()).isEqualTo("INSERT INTO foo (v) VALUES (?);");
        assertThat(query.getObject(0)).isEqualTo(set);
        query = insertInto("foo").value("v", list);
        assertThat(query.getQueryString()).isEqualTo("INSERT INTO foo (v) VALUES (?);");
        assertThat(query.getObject(0)).isEqualTo(list);
        query = insertInto("foo").value("v", map);
        assertThat(query.getQueryString()).isEqualTo("INSERT INTO foo (v) VALUES (?);");
        assertThat(query.getObject(0)).isEqualTo(map);
    }

    @Test(groups = "unit")
    public void should_not_attempt_to_serialize_function_calls_in_collections() {
        BuiltStatement query = insertInto("foo").value("v", Sets.newHashSet(fcall("func", 1)));
        assertThat(query.getQueryString()).isEqualTo("INSERT INTO foo (v) VALUES ({func(1)});");
        assertThat(query.getValues(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE)).isNullOrEmpty();
    }

    @Test(groups = "unit")
    public void should_not_attempt_to_serialize_bind_markers_in_collections() {
        BuiltStatement query = insertInto("foo").value("v", Lists.newArrayList(1, 2, bindMarker()));
        assertThat(query.getQueryString()).isEqualTo("INSERT INTO foo (v) VALUES ([1,2,?]);");
        assertThat(query.getValues(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE)).isNullOrEmpty();
    }

    @Test(groups = "unit")
    public void should_not_attempt_to_serialize_raw_values_in_collections() {
        BuiltStatement query = insertInto("foo").value("v", ImmutableMap.of(1, raw("x")));
        assertThat(query.getQueryString()).isEqualTo("INSERT INTO foo (v) VALUES ({1:x});");
        assertThat(query.getValues(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE)).isNullOrEmpty();
    }

    @Test(groups = "unit")
    public void should_not_attempt_to_serialize_collections_containing_numbers() {
        BuiltStatement query;
        // lists
        List<Integer> list = Lists.newArrayList(1, 2, 3);
        query = insertInto("foo").value("v", list);
        assertThat(query.getQueryString()).isEqualTo("INSERT INTO foo (v) VALUES ([1,2,3]);");
        assertThat(query.hasValues()).isFalse();
        // sets
        Set<Integer> set = Sets.newHashSet(1, 2, 3);
        query = insertInto("foo").value("v", set);
        assertThat(query.getQueryString()).isEqualTo("INSERT INTO foo (v) VALUES ({1,2,3});");
        assertThat(query.hasValues()).isFalse();
        // maps
        Map<Integer, Float> map = ImmutableMap.of(1, 12.34f);
        query = insertInto("foo").value("v", map);
        assertThat(query.getQueryString()).isEqualTo("INSERT INTO foo (v) VALUES ({1:12.34});");
        assertThat(query.hasValues()).isFalse();
    }

    @Test(groups = "unit")
    public void should_include_original_cause_when_arguments_invalid() {
        // Collection elements in protocol v2 must be at most 65535 bytes
        ByteBuffer bb = ByteBuffer.allocate(65536); // too big
        List<ByteBuffer> value = Lists.newArrayList(bb);

        BuiltStatement s = insertInto("foo").value("l", value);
        try {
            s.getValues(ProtocolVersion.V2, CodecRegistry.DEFAULT_INSTANCE);
            fail("Expected an IllegalArgumentException");
        } catch (InvalidTypeException e) {
            assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class);
            StringWriter writer = new StringWriter();
            e.getCause().printStackTrace(new PrintWriter(writer));
            String stackTrace = writer.toString();
            assertThat(stackTrace).contains(
                    "Native protocol version 2 supports only elements with size up to 65535 bytes - " +
                            "but element size is 65536 bytes");
        }
    }

    @Test(groups = "unit")
    public void should_handle_per_partition_limit_clause() {
        assertThat(
                select().all().from("foo").perPartitionLimit(2).toString())
                .isEqualTo("SELECT * FROM foo PER PARTITION LIMIT 2;");
        assertThat(
                select().all().from("foo").perPartitionLimit(bindMarker()).toString())
                .isEqualTo("SELECT * FROM foo PER PARTITION LIMIT ?;");
        assertThat(
                select().all().from("foo").perPartitionLimit(bindMarker("limit")).toString())
                .isEqualTo("SELECT * FROM foo PER PARTITION LIMIT :limit;");
        assertThat(
                select().all().from("foo").perPartitionLimit(2).limit(bindMarker()).toString())
                .isEqualTo("SELECT * FROM foo PER PARTITION LIMIT 2 LIMIT ?;");
        assertThat(
                select().all().from("foo").where(in("a", 2, 4)).perPartitionLimit(2).limit(3).toString())
                .isEqualTo("SELECT * FROM foo WHERE a IN (2,4) PER PARTITION LIMIT 2 LIMIT 3;");
        assertThat(
                select().all().from("foo").where(eq("a", bindMarker())).perPartitionLimit(bindMarker()).limit(3).toString())
                .isEqualTo("SELECT * FROM foo WHERE a=? PER PARTITION LIMIT ? LIMIT 3;");
        assertThat(
                select().all().from("foo").where(eq("a", bindMarker())).orderBy(desc("b")).perPartitionLimit(2).limit(3).toString())
                .isEqualTo("SELECT * FROM foo WHERE a=? ORDER BY b DESC PER PARTITION LIMIT 2 LIMIT 3;");
        assertThat(
                select().all().from("foo").where(eq("a", bindMarker())).and(gt("b", bindMarker()))
                        .orderBy(desc("b")).perPartitionLimit(bindMarker()).limit(3).allowFiltering().toString())
                .isEqualTo("SELECT * FROM foo WHERE a=? AND b>? ORDER BY b DESC PER PARTITION LIMIT ? LIMIT 3 ALLOW FILTERING;");
        try {
            select().distinct().all().from("foo").perPartitionLimit(3);
            fail("Should not allow DISTINCT + PER PARTITION LIMIT");
        } catch (Exception e) {
            assertThat(e).hasMessage("PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries");
        }
        try {
            select().all().from("foo").perPartitionLimit(-1);
            fail("Should not allow negative limit");
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("Invalid PER PARTITION LIMIT value, must be strictly positive");
        }
        try {
            select().all().from("foo").perPartitionLimit(1).perPartitionLimit(bindMarker());
            fail("Should not allow to set limit twice");
        } catch (IllegalStateException e) {
            assertThat(e).hasMessage("A PER PARTITION LIMIT value has already been provided");
        }
    }

    @Test(groups = "unit")
    public void should_handle_select_json() throws Exception {
        assertThat(
                select().json().from("users").toString())
                .isEqualTo("SELECT JSON * FROM users;");
        assertThat(
                select("id", "age").json().from("users").toString())
                .isEqualTo("SELECT JSON id,age FROM users;");
        assertThat(
                select().json().column("id").writeTime("age").ttl("state").as("ttl").from("users").toString())
                .isEqualTo("SELECT JSON id,writetime(age),ttl(state) AS ttl FROM users;");
        assertThat(
                select().distinct().json().column("id").from("users").toString())
                .isEqualTo("SELECT JSON DISTINCT id FROM users;"); // note that the correct syntax is JSON DISTINCT
    }

    @Test(groups = "unit")
    public void should_handle_insert_json() throws Exception {
        assertThat(
                insertInto("example").json("{\"id\": 0, \"tupleval\": [1, \"abc\"], \"numbers\": [1, 2, 3], \"letters\": [\"a\", \"b\", \"c\"]}").toString())
                .isEqualTo("INSERT INTO example JSON '{\"id\": 0, \"tupleval\": [1, \"abc\"], \"numbers\": [1, 2, 3], \"letters\": [\"a\", \"b\", \"c\"]}';");
        assertThat(
                insertInto("users").json("{\"id\": \"user123\", \"\\\"Age\\\"\": 42, \"\\\"State\\\"\": \"TX\"}").toString())
                .isEqualTo("INSERT INTO users JSON '{\"id\": \"user123\", \"\\\"Age\\\"\": 42, \"\\\"State\\\"\": \"TX\"}';");
        assertThat(
                insertInto("users").json(bindMarker()).toString())
                .isEqualTo("INSERT INTO users JSON ?;");
        assertThat(
                insertInto("users").json(bindMarker("json")).toString())
                .isEqualTo("INSERT INTO users JSON :json;");
    }

    @Test(groups = "unit")
    public void should_handle_to_json() throws Exception {
        assertThat(
                select().toJson("id").as("id").toJson("age").as("age").from("users").toString())
                .isEqualTo("SELECT toJson(id) AS id,toJson(age) AS age FROM users;");
        assertThat(
                select().distinct().toJson("id").as("id").from("users").toString())
                .isEqualTo("SELECT DISTINCT toJson(id) AS id FROM users;");
    }

    @Test(groups = "unit")
    public void should_handle_from_json() throws Exception {
        assertThat(
                update("users").with(set("age", fromJson("42"))).where(eq("id", fromJson("\"user123\""))).toString())
                .isEqualTo("UPDATE users SET age=fromJson('42') WHERE id=fromJson('\"user123\"');");
        assertThat(
                insertInto("users").value("id", fromJson("\"user123\"")).value("age", fromJson("42")).toString())
                .isEqualTo("INSERT INTO users (id,age) VALUES (fromJson('\"user123\"'),fromJson('42'));");
        assertThat(
                insertInto("users").value("id", fromJson(bindMarker())).toString())
                .isEqualTo("INSERT INTO users (id) VALUES (fromJson(?));");
        assertThat(
                insertInto("users").value("id", fromJson(bindMarker("id"))).toString())
                .isEqualTo("INSERT INTO users (id) VALUES (fromJson(:id));");
    }

    static class Foo {
        int bar;

        public Foo(int bar) {
            this.bar = bar;
        }
    }

    static class FooCodec extends TypeCodec<Foo> {

        public FooCodec() {
            super(DataType.cint(), Foo.class);
        }

        @Override
        public ByteBuffer serialize(Foo value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            // not relevant for this test
            return null;
        }

        @Override
        public Foo deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            // not relevant for this test
            return null;
        }

        @Override
        public Foo parse(String value) throws InvalidTypeException {
            // not relevant for this test
            return null;
        }

        @Override
        public String format(Foo foo) throws InvalidTypeException {
            return Integer.toString(foo.bar);
        }
    }

    /**
     * Ensures that a statement can be printed with and without
     * a required custom codec.
     * The expectation is that if the codec is not registered,
     * then the query string should contain bind markers for all variables;
     * if however all codecs are properly registered, then
     * the query string should contain all variables inlined and formatted properly.
     *
     * @jira_ticket JAVA-1272
     */
    @Test(groups = "unit")
    public void should_inline_custom_codec() throws Exception {
        assertThat(
                insertInto("users").value("id", new Foo(42)).toString())
                .isEqualTo("INSERT INTO users (id) VALUES (?);");
        CodecRegistry.DEFAULT_INSTANCE.register(new FooCodec());
        assertThat(
                insertInto("users").value("id", new Foo(42)).toString())
                .isEqualTo("INSERT INTO users (id) VALUES (42);");
    }

    /**
     * @jira_ticket JAVA-1312
     */
    @Test(groups = "unit")
    public void should_not_append_last_column_twice() throws Exception {
        Select.SelectionOrAlias select = select().column("a").column("b");
        Select fromUsers1 = select.from("users");
        Select fromUsers2 = select.from("users");
        assertThat(fromUsers1.getQueryString())
                .isEqualTo(fromUsers2.getQueryString())
                .isEqualTo("SELECT a,b FROM users;");
    }

    /**
     * @test_category queries:builder
     * @jira_ticket JAVA-1286
     * @jira_ticket CASSANDRA-7423
     */
    @Test(groups = "unit")
    public void should_handle_setting_udt_fields() throws Exception {
        assertThat(
                update("tbl")
                        .with(set(path("a", quote("B")), "foo"))
                        .and(set(raw("c.\"D\""), "bar"))
                        .where(eq("k", 0)).getQueryString())
                .isEqualTo("UPDATE tbl SET a.\"B\"=?,c.\"D\"=? WHERE k=0;");
    }

    /**
     * @test_category queries:builder
     * @jira_ticket JAVA-1286
     * @jira_ticket CASSANDRA-7423
     */
    @Test(groups = "unit")
    public void should_handle_retrieving_udt_fields() throws Exception {
        assertThat(
                select()
                        .path("a", Metadata.quote("B"))
                        .raw("c.\"D\"")
                        .from("tbl").getQueryString())
                .isEqualTo("SELECT a.\"B\",c.\"D\" FROM tbl;");
    }

}
