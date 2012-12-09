package com.datastax.driver.core.utils;

import java.net.InetAddress;
import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import com.datastax.driver.core.utils.querybuilder.*;
import static com.datastax.driver.core.utils.querybuilder.Assignment.*;
import static com.datastax.driver.core.utils.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.utils.querybuilder.Clause.*;
import static com.datastax.driver.core.utils.querybuilder.Ordering.*;
import static com.datastax.driver.core.utils.querybuilder.Using.*;

public class QueryBuilderTest {

    @Test
    public void selectTest() throws Exception {

        String query;
        Select select;

        query = "SELECT * FROM foo WHERE k=4 AND c>'a' AND c<='z';";
        select = select(all()).from("foo").where(eq("k", 4), gt("c", "a"), lte("c", "z"));
        assertEquals(query, select.toString());

        query = "SELECT a,b,\"C\" FROM foo WHERE a IN (127.0.0.1,127.0.0.3) AND \"C\"='foo' ORDER BY (a ASC,b DESC) LIMIT 42;";
        select = select("a", "b", quote("C")).from("foo")
                   .where(in("a", InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.3")),
                          eq(quote("C"), "foo"))
                   .orderBy(asc("a"), desc("b"))
                   .limit(42);
        assertEquals(query, select.toString());

        query = "SELECT writetime(a),ttl(a) FROM foo;";
        select = select(writeTime("a"), ttl("a")).from("foo");
        assertEquals(query, select.toString());

        query = "SELECT count(*) FROM foo;";
        select = select(count()).from("foo");
        assertEquals(query, select.toString());
    }

    @Test
    public void insertTest() throws Exception {

        String query;
        Insert insert;

        query = "INSERT INTO foo(a,b,\"C\",d) VALUES (123,127.0.0.1,'foo''bar',{'x':3,'y':2}) USING TIMESTAMP 42 AND TTL 24;";
        insert = insert("a", "b", quote("C"), "d").into("foo")
                   .values(123, InetAddress.getByName("127.0.0.1"), "foo'bar", new TreeMap(){{ put("x", 3); put("y", 2); }})
                   .using(timestamp(42), ttl(24));
        assertEquals(query, insert.toString());

        query = "INSERT INTO foo(a,b) VALUES ({2,3,4},3.4) USING TTL 24 AND TIMESTAMP 42;";
        insert = insert("a", "b").into("foo").values(new TreeSet(){{ add(2); add(3); add(4); }}, 3.4).using(ttl(24), timestamp(42));
        assertEquals(query, insert.toString());
    }

    @Test(expected = IllegalStateException.class)
    public void insertTestWithEmptyColumns() {
        insert().into("foo").values().using(ttl(24), timestamp(42));
    }

    @Test
    public void fluentInsertTest() throws Exception {

        String query;
        Insert insert;

        query = "INSERT INTO foo(a,b,\"C\",d) VALUES (123,127.0.0.1,'foo''bar',{'x':3,'y':2}) USING TIMESTAMP 42 AND TTL 24;";
        insert = insert().into("foo")
                .column("a", 123)
                .and().column("b", InetAddress.getByName("127.0.0.1"))
                .and().column(quote("C"), "foo'bar")
                .and().column("d", new TreeMap() {{
                    put("x", 3);
                    put("y", 2);
                }})
                .using(timestamp(42), ttl(24));
        assertEquals(query, insert.toString());

        query = "INSERT INTO foo(a,b) VALUES ({2,3,4},3.4) USING TTL 24 AND TIMESTAMP 42;";
        insert = insert("a", "b").into("foo").column("a", new TreeSet() {{
            add(2);
            add(3);
            add(4);
        }}).and().column("b", 3.4).using(ttl(24), timestamp(42));
        assertEquals(query, insert.toString());
    }

    @Test
    public void updateTest() throws Exception {

        String query;
        Update update;

        query = "UPDATE foo.bar USING TIMESTAMP 42 SET a=12,b=[3,2,1],c=c+3 WHERE k=2;";
        update = update("foo", "bar").using(timestamp(42)).set(set("a", 12), set("b", Arrays.asList(3, 2, 1)), incr("c", 3)).where(eq("k", 2));
        assertEquals(query, update.toString());

        query = "UPDATE foo SET a[2]='foo',b=[3,2,1]+b,c=c-{'a'} WHERE k=2;";
        update = update("foo").set(setIdx("a", 2, "foo"), prependAll("b", Arrays.asList(3, 2, 1)), remove("c", "a")).where(eq("k", 2));
        assertEquals(query, update.toString());
    }

    @Test
    public void deleteTest() throws Exception {

        String query;
        Delete delete;

        query = "DELETE a,b,c FROM foo USING TIMESTAMP 0 WHERE k=1;";
        delete = delete("a", "b", "c").from("foo").using(timestamp(0)).where(eq("k", 1));
        assertEquals(query, delete.toString());

        query = "DELETE a[3],b['foo'],c FROM foo WHERE k=1;";
        delete = delete(listElt("a", 3), mapElt("b", "foo"), "c").from("foo").where(eq("k", 1));
        assertEquals(query, delete.toString());
    }

    @Test
    public void batchTest() throws Exception {
        String query;
        Batch batch;

        query = "BEGIN BATCH USING TIMESTAMP 42 ";
        query += "INSERT INTO foo(a,b) VALUES ({2,3,4},3.4);";
        query += "UPDATE foo SET a[2]='foo',b=[3,2,1]+b,c=c-{'a'} WHERE k=2;";
        query += "DELETE a[3],b['foo'],c FROM foo WHERE k=1;";
        query += "APPLY BATCH;";
        batch = batch(
            insert("a", "b").into("foo").values(new TreeSet(){{ add(2); add(3); add(4); }}, 3.4),
            update("foo").set(setIdx("a", 2, "foo"), prependAll("b", Arrays.asList(3, 2, 1)), remove("c", "a")).where(eq("k", 2)),
            delete(listElt("a", 3), mapElt("b", "foo"), "c").from("foo").where(eq("k", 1))
        ).using(timestamp(42));
        assertEquals(query, batch.toString());
    }
}
