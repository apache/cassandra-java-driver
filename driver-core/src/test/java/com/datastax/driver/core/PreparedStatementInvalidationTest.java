package com.datastax.driver.core;

import org.junit.Test;

public class PreparedStatementInvalidationTest {
    @Test
    public void testPreparedStatements()
    {
        Cluster cluster;
        Session session;
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();

        Cluster cluster2;
        Session session2;
        cluster2 = Cluster.builder().addContactPoint("127.0.0.1").build();
        session2 = cluster2.connect();

        session.execute("DROP KEYSPACE IF EXISTS \"keyspace1\"");

        session.execute("CREATE KEYSPACE \"keyspace1\" WITH replication = {\n" +
                "  'class': 'SimpleStrategy',\n" +
                "  'replication_factor': '3'\n" +
                "};");
        session.execute("use keyspace1");
        session2.execute("use keyspace1");
        session.execute("CREATE TABLE simpletable (a int PRIMARY KEY, b int, c int);");
        session.execute("INSERT INTO simpletable (a,b,c) VALUES (1,2,3)");
        session.execute("INSERT INTO simpletable (a,b,c) VALUES (4,5,6)");

        PreparedStatement prepared1 = session.prepare("SELECT * FROM simpletable");
        PreparedStatement prepared2 = session2.prepare("SELECT * FROM simpletable");
        System.out.println("RESULT :" + session.execute(prepared1.bind()).all());
        System.out.println("RESULT :" + session2.execute(prepared2.bind()).all());

        session.execute("ALTER TABLE simpletable ADD d int");
        session.execute("INSERT INTO simpletable (a,b,c, d) VALUES (4,5,6, 7)");
        System.out.println("RESULT :" + session.execute(prepared1.bind()).all());
        System.out.println("RESULT :" + session2.execute(prepared2.bind()).all());

        session.execute("ALTER TABLE simpletable ADD e int");
        session.execute("INSERT INTO simpletable (a,b,c,d,e) VALUES (4,5,6,7,8)");
        System.out.println("RESULT :" + session.execute(prepared1.bind()).all());
        System.out.println("RESULT :" + session2.execute(prepared2.bind()).all());
    }

    @Test
    public void testPreparedStatementsSameSession()
    {
        Cluster cluster;
        Session session;
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();

        session.execute("DROP KEYSPACE IF EXISTS \"keyspace1\"");

        session.execute("CREATE KEYSPACE \"keyspace1\" WITH replication = {\n" +
                "  'class': 'SimpleStrategy',\n" +
                "  'replication_factor': '3'\n" +
                "};");
        session.execute("use keyspace1");
        session.execute("CREATE TABLE simpletable (a int PRIMARY KEY, b int, c int);");
        session.execute("INSERT INTO simpletable (a,b,c) VALUES (1,2,3)");
        session.execute("INSERT INTO simpletable (a,b,c) VALUES (4,5,6)");

        PreparedStatement prepared1 = session.prepare("SELECT * FROM simpletable");
        for (int i = 0; i < 10; i++) {
            System.out.println("RESULT :" + session.execute(prepared1.bind()).all());
        }

        session.execute("ALTER TABLE simpletable ADD d int");
        session.execute("INSERT INTO simpletable (a,b,c, d) VALUES (4,5,6, 7)");
        for (int i = 0; i < 10; i++) {
            System.out.println("RESULT :" + session.execute(prepared1.bind()).all());
        }

        session.execute("ALTER TABLE simpletable ADD e int");
        session.execute("INSERT INTO simpletable (a,b,c,d,e) VALUES (4,5,6,7,8)");
        for (int i = 0; i < 10; i++) {
            System.out.println("RESULT :" + session.execute(prepared1.bind()).all());
        }
    }

}
