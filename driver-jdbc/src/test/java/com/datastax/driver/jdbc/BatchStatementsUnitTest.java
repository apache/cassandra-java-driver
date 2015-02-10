package com.datastax.driver.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchStatementsUnitTest {
	
    private static final Logger LOG = LoggerFactory.getLogger(CollectionsUnitTest.class);


    private static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort() + ""));
    private static final String KEYSPACE = "testks";
    private static final String SYSTEM = "system";
    private static final String CQLV3 = "3.0.0";

    private static java.sql.Connection con = null;
    private static java.sql.Connection con2 = null;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Class.forName("com.datastax.driver.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s?debug=true&version=%s", HOST, PORT, SYSTEM, CQLV3);

        con = DriverManager.getConnection(URL);
        

        if (LOG.isDebugEnabled()) LOG.debug("URL         = '{}'", URL);

        Statement stmt = con.createStatement();

        // Use Keyspace
        String useKS = String.format("USE %s;", KEYSPACE);

        // Drop Keyspace
        String dropKS = String.format("DROP KEYSPACE %s;", KEYSPACE);

        try
        {
            stmt.execute(dropKS);
        }
        catch (Exception e)
        {/* Exception on DROP is OK */}

        // Create KeySpace
        String createKS = String.format("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy',  'replication_factor' : 1  };",KEYSPACE);
//        String createKS = String.format("CREATE KEYSPACE %s WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1;",KEYSPACE);
        if (LOG.isDebugEnabled()) LOG.debug("createKS    = '{}'", createKS);

        stmt = con.createStatement();
        stmt.execute("USE " + SYSTEM);
        stmt.execute(createKS);
        stmt.execute(useKS);


        // Create the target Table (CF)
        String createTable = "CREATE TABLE testcollection (" + " k int PRIMARY KEY," + " L list<bigint>," + " M map<double, boolean>," + " S set<text>" + ") ;";
        if (LOG.isDebugEnabled()) LOG.debug("createTable = '{}'", createTable);

        stmt.execute(createTable);
        stmt.close();
        con.close();

        // open it up again to see the new TABLE
        URL = String.format("jdbc:cassandra://%s:%d/%s?debug=true&version=%s", HOST, PORT, KEYSPACE, CQLV3);
        con = DriverManager.getConnection(URL);
        if (LOG.isDebugEnabled()) LOG.debug("URL         = '{}'", URL);

        Statement statement = con.createStatement();

        String insert = "INSERT INTO testcollection (k,L) VALUES( 1,[1, 3, 12345]);";
        statement.executeUpdate(insert);
        String update1 = "UPDATE testcollection SET S = {'red', 'white', 'blue'} WHERE k = 1;";
        String update2 = "UPDATE testcollection SET M = {2.0: true, 4.0: false, 6.0 : true} WHERE k = 1;";
        statement.executeUpdate(update1);
        statement.executeUpdate(update2);
        con2 = DriverManager.getConnection(URL);

        if (LOG.isDebugEnabled()) LOG.debug("Unit Test: 'CollectionsTest' initialization complete.\n\n");
    }

    /**
     * Close down the connection when complete
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (con != null) con.close();
        if (con2 != null) con2.close();
    }

    @Test
    public void testBatchSimpleStatement() throws Exception
    {
        System.out.println("Test: 'testBatchSimpleStatement'\n");

        Statement statement = con.createStatement();
        
        
        for(int i=0;i<10;i++){
        	System.out.println("--- Statement " + i + " ==> INSERT INTO testcollection (k,L) VALUES( " + i + ",[1, 3, 12345])");
        	statement.addBatch("INSERT INTO testcollection (k,L) VALUES( " + i + ",[1, 3, 12345])");
        }
        
        int[] counts = statement.executeBatch();
        
        assertEquals(10,counts.length);
        
        ResultSet result = statement.executeQuery("SELECT * FROM testcollection;");

        int nbRow = 0;
        ArrayList<Integer> ids = new ArrayList<Integer>(); 
        while(result.next()){
        	nbRow++;
        	ids.add(result.getInt("k"));
        }

        assertEquals(10, nbRow);
        Collections.sort(ids);
        int nb = 0;
        for(Integer id:ids){
        	assertEquals(nb,id.intValue());
        	nb++;
        }
        
        statement.close();

        
    }
    
    
    @Test
    public void testBatchPreparedStatement() throws Exception
    {
    	System.out.println("Test: 'testBatchPreparedStatement'\n");

        PreparedStatement statement = con.prepareStatement("INSERT INTO testcollection (k,L) VALUES(?,?)");
        
        
        for(int i=0;i<10;i++){
        	System.out.println("--- Generating prepared statement " + i);
        	statement.setInt(1, i);
        	statement.setString(2, "[1, 3, 12345]");
        	statement.addBatch();
        }
        
        
        int[] counts = statement.executeBatch();
        
        
        assertEquals(10,counts.length);
        
        ResultSet result = statement.executeQuery("SELECT * FROM testcollection;");

        int nbRow = 0;
        ArrayList<Integer> ids = new ArrayList<Integer>(); 
        while(result.next()){
        	nbRow++;
        	ids.add(result.getInt("k"));
        }

        assertEquals(10, nbRow);
        Collections.sort(ids);
        int nb = 0;
        for(Integer id:ids){
        	assertEquals(nb,id.intValue());
        	nb++;
        }
        
        statement.close();

        
    }
    
    
    @Test
    public void testAsyncSelectStatement() throws Exception
    {
    	System.out.println("Test: 'testAsyncSelectStatement'\n");

        PreparedStatement statement = con2.prepareStatement("INSERT INTO testcollection (k,L) VALUES(?,?)");
        
        
        for(int i=0;i<10;i++){
        	System.out.println("--- Generating prepared statement " + i);
        	statement.setInt(1, i);
        	statement.setString(2, "[1, 3, 12345]");
        	statement.addBatch();
        }
        
        
        int[] counts = statement.executeBatch();
        
        
        assertEquals(10,counts.length);
        
        StringBuilder query=new StringBuilder();
        for(int i=0;i<10;i++){
        	query.append("SELECT * FROM testcollection where k=" + i + ";");
        }
        
        Statement selectStatement = con2.createStatement();
        ResultSet result = selectStatement.executeQuery(query.toString());

        int nbRow = 0;
        ArrayList<Integer> ids = new ArrayList<Integer>(); 
        while(result.next()){
        	nbRow++;
        	ids.add(result.getInt("k"));
        }

        assertEquals(10, nbRow);
        Collections.sort(ids);
        int nb = 0;
        for(Integer id:ids){
        	assertEquals(nb,id.intValue());
        	nb++;
        }
        
        statement.close();

        
    }
    
    @Test
    public void testAsyncSelect1Statement() throws Exception
    {
    	System.out.println("Test: 'testAsyncSelect1Statement'\n");

        PreparedStatement statement = con.prepareStatement("INSERT INTO testcollection (k,L) VALUES(?,?)");
        
        
        for(int i=0;i<1;i++){
        	System.out.println("--- Generating prepared statement " + i);
        	statement.setInt(1, i);
        	statement.setString(2, "[1, 3, 12345]");
        	statement.addBatch();
        }
        
        
        int[] counts = statement.executeBatch();
        
        
        assertEquals(1,counts.length);
        
        StringBuilder query=new StringBuilder();
        for(int i=0;i<1;i++){
        	query.append("SELECT * FROM testcollection where k=" + i + ";");
        }
        
        Statement selectStatement = con.createStatement();
        ResultSet result = selectStatement.executeQuery(query.toString());

        int nbRow = 0;
        ArrayList<Integer> ids = new ArrayList<Integer>(); 
        while(result.next()){
        	nbRow++;
        	ids.add(result.getInt("k"));
        }

        assertEquals(1, nbRow);
        Collections.sort(ids);
        int nb = 0;
        for(Integer id:ids){
        	assertEquals(nb,id.intValue());
        	nb++;
        }
        
        statement.close();

        
    }


}
