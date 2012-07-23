package com.datastax.driver.core;

import org.junit.BeforeClass;
import org.junit.Test;
import static junit.framework.Assert.*;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class SessionTest {

    // I really think we should make sure the library doesn't complain about
    // log4j by default, but for now let's deal with it locally
    @BeforeClass
    public static void classSetUp() {
        Logger rootLogger = Logger.getRootLogger();
        if (!rootLogger.getAllAppenders().hasMoreElements()) {
            rootLogger.setLevel(Level.INFO);
            rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%-5p [%t]: %m%n")));
        }
    }

    @Test
    public void SimpleExecuteTest() throws Exception {

        Cluster cluster = new Cluster.Builder().addContactPoint("localhost").build();
        Session session = cluster.connect();

        ResultSet rs = session.execute("SELECT * FROM system.local");
        System.out.println(rs.columns().toString());
        for (CQLRow row : rs)
            System.out.println(row.toString());
    }
}
