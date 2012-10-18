package com.datastax.driver.core;

import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;
import static junit.framework.Assert.*;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.datastax.driver.core.exceptions.*;

public class SessionTest {

    // I really think we should make sure the library doesn't complain about
    // log4j by default, but for now let's deal with it locally
    @BeforeClass
    public static void classSetUp() {
        Logger rootLogger = Logger.getRootLogger();
        if (!rootLogger.getAllAppenders().hasMoreElements()) {
            rootLogger.setLevel(Level.TRACE);
            rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%-5p [%t]: %m%n")));
        }
    }

    //@Test
    //public void MultiNodeExecuteTest() throws Exception {

    //    Cluster cluster = new Cluster.Builder().addContactPoints("127.0.0.1", "127.0.0.2").build();
    //    Session session = cluster.connect();

    //    session.execute("CREATE KEYSPACE test_ks WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1");
    //    session.use("test_ks");
    //    session.execute("CREATE TABLE test (k text PRIMARY KEY, i int, f float)");

    //    ResultSet rs;

    //    rs = session.execute("INSERT INTO test (k, i, f) VALUES ('foo', 0, 0.2)");
    //    assertTrue(rs.isExhausted());

    //    rs = session.execute("INSERT INTO test (k, i, f) VALUES ('bar', 1, 3.4)");
    //    assertTrue(rs.isExhausted());

    //    rs = session.execute("SELECT * FROM test");
    //    List<CQLRow> l = rs.fetchAll();
    //    assertEquals(2, l.size());

    //    CQLRow r;
    //    r = l.get(0);
    //    assertEquals("bar", r.getString(0));
    //    assertEquals("bar", r.getString("k"));
    //    assertEquals(1,     r.getInt("i"));
    //    assertEquals(3.4,   r.getFloat("f"), 0.01);

    //    r = l.get(1);
    //    assertEquals("foo", r.getString("k"));
    //    assertEquals(0,     r.getInt("i"));
    //    assertEquals(0.2,   r.getFloat("f"), 0.01);
    //}

    //@Test
    //public void PreparedStatementTest() throws Exception {

    //    Cluster cluster = new Cluster.Builder().addContactPoint("localhost").build();
    //    Session session = cluster.connect();

    //    try
    //    {
    //        session.execute("CREATE KEYSPACE test_ks WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
    //        session.execute("USE test_ks");
    //        session.execute("CREATE TABLE test_2 (k text, i int, f float, PRIMARY KEY(k, i))");
    //    } catch (Exception e) {
    //        // Skip if already created
    //        session.execute("USE test_ks");
    //    }

    //    PreparedStatement insertStmt = session.prepare("INSERT INTO test_2 (k, i, f) VALUES (?, ?, ?)");
    //    PreparedStatement selectStmt = session.prepare("SELECT * FROM test_2 WHERE k = ?");

    //    ResultSet rs;
    //    BoundStatement bs;

    //    bs = insertStmt.newBoundStatement().setString(0, "prep").setInt("i", 1).setFloat(2, 0.1f);
    //    rs = session.executePrepared(bs);

    //    bs = insertStmt.newBoundStatement().setString(0, "prep").setFloat("f", 0.2f).setInt(1, 2);
    //    rs = session.executePrepared(bs);

    //    session.executePrepared(insertStmt.bind("prep", 3, 42.0f));

    //    bs = selectStmt.newBoundStatement().setString("k", "prep");
    //    rs = session.executePrepared(bs);

    //    List<CQLRow> l = rs.fetchAll();
    //    assertEquals(3, l.size());

    //    CQLRow r;
    //    r = l.get(0);
    //    assertEquals("prep", r.getString(0));
    //    assertEquals(1,     r.getInt("i"));
    //    assertEquals(0.1,   r.getFloat("f"), 0.01);

    //    r = l.get(1);
    //    assertEquals("prep", r.getString("k"));
    //    assertEquals(2,     r.getInt("i"));
    //    assertEquals(0.2,   r.getFloat("f"), 0.01);

    //    r = l.get(2);
    //    assertEquals("prep", r.getString("k"));
    //    assertEquals(3,     r.getInt("i"));
    //    assertEquals(42.0f, r.getFloat("f"), 0.01);
    //}

    //@Test
    //public void CollectionsTest() throws Exception {

    //    Cluster cluster = new Cluster.Builder().addContactPoints("127.0.0.1").build();
    //    Session session = cluster.connect();

    //    try {
    //        session.execute("CREATE KEYSPACE test_ks WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
    //        // We should deal with that sleep
    //        try { Thread.sleep(1000); } catch (Exception e) {}
    //        session.execute("USE test_ks");
    //        session.execute("CREATE TABLE test (k text PRIMARY KEY, l list<int>, s set<text>, m map<timestamp, int>)");
    //    } catch (Exception e) {
    //        // Skip if already created
    //        session.execute("USE test_ks");
    //    }

    //    session.execute("INSERT INTO test (k, l, s, m) VALUES ('k', [3, 2, 1], { 3, 2, 1}, { 1349286846012 : 2 })");
    //    for (CQLRow row : session.execute("SELECT * FROM test")) {
    //        List<Integer> l = row.getList("l", Integer.class);
    //        Set<String> s = row.getSet("s", String.class);
    //        Map<Date, Integer> m = row.getMap("m", Date.class, Integer.class);

    //        System.out.println("l = " + l);
    //        System.out.println("s = " + s);
    //        System.out.println("m = " + m);
    //    }

    //    System.out.println("-------");

    //    BoundStatement stmt = session.prepare("INSERT INTO test (k, l, s, m) VALUES ('k2', ?, ?, ?)").newBoundStatement();
    //    stmt.setList(0, Arrays.asList(new Integer[]{ 5, 4, 3, 2, 1 }));
    //    stmt.setSet(1, new HashSet(Arrays.asList(new String[]{ "5", "4", "3", "2", "1" })));
    //    stmt.setMap(2, new HashMap<Date, Integer>(){{ put(new Date(1349286846012L), 4); }});
    //    session.executePrepared(stmt);

    //    for (CQLRow row : session.execute("SELECT * FROM test WHERE k = 'k2'")) {
    //        List<Integer> l = row.getList("l", Integer.class);
    //        Set<String> s = row.getSet("s", String.class);
    //        Map<Date, Integer> m = row.getMap("m", Date.class, Integer.class);

    //        System.out.println("l = " + l);
    //        System.out.println("s = " + s);
    //        System.out.println("m = " + m);
    //    }
    //}

    @Test
    public void MultiNodeContinuousExecuteTest() throws Exception {

        Cluster cluster = new Cluster.Builder().addContactPoints("127.0.0.1").build();
        Session session = cluster.connect();

        try {
            session.execute("CREATE KEYSPACE test_ks WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
            // We should deal with that sleep
            try { Thread.sleep(2000); } catch (Exception e) {}
            session.execute("USE test_ks");
            session.execute("CREATE TABLE test (k text PRIMARY KEY, i int, f float)");
        } catch (AlreadyExistsException e) {
            // Skip if already exists
            session.execute("USE test_ks");
        }

        //System.out.println("--- Schema ---\n" + cluster.getMetadata());

        for (int i = 0; i < 10000; ++i) {
            System.out.println(">> " + i);
            session.execute(String.format("INSERT INTO test (k, i, f) VALUES ('k%d', %d, %d.2)", i, i, i));
            Thread.currentThread().sleep(1000);
        }
    }

    //@Test
    //public void SchemaTest() throws Exception {

    //    Cluster cluster = new Cluster.Builder().addContactPoints("127.0.0.1").build();
    //    Session session = cluster.connect();

    //    try {
    //        session.execute("CREATE KEYSPACE test_ks WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
    //        // We should deal with that sleep
    //        try { Thread.sleep(2000); } catch (Exception e) {}
    //        session.execute("USE test_ks");
    //        session.execute("CREATE TABLE test (k text PRIMARY KEY, i int, f float)");
    //    } catch (AlreadyExistsException e) {
    //        // Skip if already exists
    //        session.execute("USE test_ks");
    //    }

    //    for (int i = 0; i < 10000; ++i) {
    //        System.out.println("--- Schema " + i + " ---");
    //        System.out.println(cluster.getMetadata().getKeyspace("test_ks").exportAsString());
    //        Thread.currentThread().sleep(4000);
    //    }
    //}
}
