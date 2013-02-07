package com.datastax.driver.core.querybuilder;

import java.net.InetAddress;
import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import com.datastax.driver.core.Query;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class QueryBuilderTest {

    @Test
    public void selectTest() throws Exception {

        String query;
        Query select;

        query = "SELECT * FROM foo WHERE k=4 AND c>'a' AND c<='z';";
        select = select().all().from("foo").where(eq("k", 4)).and(gt("c", "a")).and(lte("c", "z"));
        assertEquals(query, select.toString());

        query = "SELECT a,b,\"C\" FROM foo WHERE a IN (127.0.0.1,127.0.0.3) AND \"C\"='foo' ORDER BY a ASC,b DESC LIMIT 42;";
        select = select("a", "b", quote("C")).from("foo")
                   .where(in("a", InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.3")))
                      .and(eq(quote("C"), "foo"))
                   .orderBy(asc("a"), desc("b"))
                   .limit(42);
        assertEquals(query, select.toString());

        query = "SELECT writetime(a),ttl(a) FROM foo ALLOW FILTERING;";
        select = select().writeTime("a").ttl("a").from("foo").allowFiltering();
        assertEquals(query, select.toString());

        query = "SELECT count(*) FROM foo;";
        select = select().countAll().from("foo");
        assertEquals(query, select.toString());
    }

    @Test
    public void insertTest() throws Exception {

        String query;
        Query insert;

        query = "INSERT INTO foo(a,b,\"C\",d) VALUES (123,127.0.0.1,'foo''bar',{'x':3,'y':2}) USING TIMESTAMP 42 AND TTL 24;";
        insert = insertInto("foo")
                   .value("a", 123)
                   .value("b", InetAddress.getByName("127.0.0.1"))
                   .value(quote("C"), "foo'bar")
                   .value("d", new TreeMap(){{ put("x", 3); put("y", 2); }})
                   .using(timestamp(42)).and(ttl(24));
        assertEquals(query, insert.toString());

        query = "INSERT INTO foo(a,b) VALUES ({2,3,4},3.4) USING TTL 24 AND TIMESTAMP 42;";
        insert = insertInto("foo").values(new String[]{ "a", "b"}, new Object[]{ new TreeSet(){{ add(2); add(3); add(4); }}, 3.4 }).using(ttl(24)).and(timestamp(42));
        assertEquals(query, insert.toString());
    }

    @Test
    public void updateTest() throws Exception {

        String query;
        Query update;

        query = "UPDATE foo.bar USING TIMESTAMP 42 SET a=12,b=[3,2,1],c=c+3 WHERE k=2;";
        update = update("foo", "bar").using(timestamp(42)).with(set("a", 12)).and(set("b", Arrays.asList(3, 2, 1))).and(incr("c", 3)).where(eq("k", 2));
        assertEquals(query, update.toString());

        query = "UPDATE foo SET a[2]='foo',b=[3,2,1]+b,c=c-{'a'} WHERE k=2 AND l='foo';";
        update = update("foo").with(setIdx("a", 2, "foo")).and(prependAll("b", Arrays.asList(3, 2, 1))).and(remove("c", "a")).where(eq("k", 2)).and(eq("l", "foo"));
        assertEquals(query, update.toString());
    }

    @Test
    public void deleteTest() throws Exception {

        String query;
        Query delete;

        query = "DELETE a,b,c FROM foo USING TIMESTAMP 0 WHERE k=1;";
        delete = delete("a", "b", "c").from("foo").using(timestamp(0)).where(eq("k", 1));
        assertEquals(query, delete.toString());

        query = "DELETE a[3],b['foo'],c FROM foo WHERE k=1;";
        delete = delete().listElt("a", 3).mapElt("b", "foo").column("c").from("foo").where(eq("k", 1));
        assertEquals(query, delete.toString());
    }

    @Test
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
        assertEquals(query, batch.toString());
    }
}
