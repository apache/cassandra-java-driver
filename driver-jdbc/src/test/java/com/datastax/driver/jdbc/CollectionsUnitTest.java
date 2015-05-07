/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datastax.driver.jdbc;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.AssertJUnit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.jdbc.CassandraResultSetExtras;

/**
 * Test CQL Collections Data Types
 * List
 * Map
 * Set
 * 
 */
public class CollectionsUnitTest
{
    private static final Logger LOG = LoggerFactory.getLogger(CollectionsUnitTest.class);


    private static String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort() + ""));
    private static final String KEYSPACE = "testks";
    private static final String SYSTEM = "system";
    private static final String CQLV3 = "3.0.0";

    private static java.sql.Connection con = null;

    private static CCMBridge ccmBridge = null;

    
    private static boolean suiteLaunch = true;
    

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    	/*System.setProperty("cassandra.version", "2.1.2");*/    	
    	    	
    	if(BuildCluster.HOST.equals(System.getProperty("host", ConnectionDetails.getHost()))){
    		BuildCluster.setUpBeforeSuite();
    		suiteLaunch=false;
    	}
    	HOST = CCMBridge.ipOfNode(1);        
    
        Class.forName("com.datastax.driver.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s?version=%s", HOST, PORT, SYSTEM, CQLV3);

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
        URL = String.format("jdbc:cassandra://%s:%d/%s?version=%s", HOST, PORT, KEYSPACE, CQLV3);
        con = DriverManager.getConnection(URL);
        if (LOG.isDebugEnabled()) LOG.debug("URL         = '{}'", URL);

        Statement statement = con.createStatement();

        String insert = "INSERT INTO testcollection (k,L) VALUES( 1,[1, 3, 12345]);";
        statement.executeUpdate(insert);
        String update1 = "UPDATE testcollection SET S = {'red', 'white', 'blue'} WHERE k = 1;";
        String update2 = "UPDATE testcollection SET M = {2.0: true, 4.0: false, 6.0 : true} WHERE k = 1;";
        statement.executeUpdate(update1);
        statement.executeUpdate(update2);


        if (LOG.isDebugEnabled()) LOG.debug("Unit Test: 'CollectionsTest' initialization complete.\n\n");
    }

    
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
    	if (con != null) con.close();
    	if(!suiteLaunch){
        	BuildCluster.tearDownAfterSuite();
        }
             
    }
    

    @Test
    public void testReadList() throws Exception
    {
        if (LOG.isDebugEnabled()) LOG.debug("Test: 'testReadList'\n");

        Statement statement = con.createStatement();
        
        String insert = "INSERT INTO testcollection (k,L) VALUES( 1,[1, 3, 12345]);";
        statement.executeUpdate(insert);
        
        ResultSet result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        AssertJUnit.assertEquals(1, result.getInt("k"));

        Object myObj = result.getObject("l");
        if (LOG.isDebugEnabled()) LOG.debug("l           = '{}'\n", myObj);
        List<Long> myList = (List<Long>) myObj;
        AssertJUnit.assertEquals(3, myList.size());
        AssertJUnit.assertTrue(12345L == myList.get(2));
        AssertJUnit.assertTrue(myObj instanceof ArrayList);

        myList = (List<Long>) extras(result).getList("l");
        statement.close();
        AssertJUnit.assertTrue(3L == myList.get(1));
    }

    @Test
    public void testUpdateList() throws Exception
    {
        if (LOG.isDebugEnabled()) LOG.debug("Test: 'testUpdateList'\n");
        
        Statement statement = con.createStatement();

        String update1 = "UPDATE testcollection SET L = L + [2,4,6] WHERE k = 1;";
        statement.executeUpdate(update1);

        ResultSet result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        AssertJUnit.assertEquals(1, result.getInt("k"));
        Object myObj = result.getObject("l");
        List<Long> myList = (List<Long>) myObj;
        AssertJUnit.assertEquals(6, myList.size());
        AssertJUnit.assertTrue(12345L == myList.get(2));
        
        if (LOG.isDebugEnabled()) LOG.debug("l           = '{}'", myObj);

        String update2 = "UPDATE testcollection SET L = [98,99,100] + L WHERE k = 1;";
        statement.executeUpdate(update2);
        result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();
        myObj = result.getObject("l");
        myList = (List<Long>) myObj;
        AssertJUnit.assertTrue(100L == myList.get(0));
        
        if (LOG.isDebugEnabled()) LOG.debug("l           = '{}'", myObj);

        String update3 = "UPDATE testcollection SET L[0] = 2000 WHERE k = 1;";
        statement.executeUpdate(update3);
        result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();
        myObj = result.getObject("l");
        myList = (List<Long>) myObj;
        
        if (LOG.isDebugEnabled()) LOG.debug("l           = '{}'", myObj);
        
//        String update4 = "UPDATE testcollection SET L = L +  ? WHERE k = 1;";
        String update4 = "UPDATE testcollection SET L =  ? WHERE k = 1;";
        
        PreparedStatement prepared = con.prepareStatement(update4);
        List<Long> myNewList = new ArrayList<Long>();
        myNewList.add(8888L);
        myNewList.add(9999L);
        prepared.setObject(1, myNewList, Types.OTHER);
        prepared.execute();

        result = prepared.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();
        myObj = result.getObject("l");
        myList = (List<Long>) myObj;
        
        if (LOG.isDebugEnabled()) LOG.debug("l (prepared)= '{}'\n", myObj);
    }

    @Test
    public void testReadSet() throws Exception
    {
        if (LOG.isDebugEnabled()) LOG.debug("Test: 'testReadSet'\n");

        Statement statement = con.createStatement();

        String update1 = "UPDATE testcollection SET S = {'red', 'white', 'blue'} WHERE k = 1;";        
        statement.executeUpdate(update1);
        
        
        ResultSet result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        AssertJUnit.assertEquals(1, result.getInt("k"));

        Object myObj = result.getObject("s");
        if (LOG.isDebugEnabled()) LOG.debug("s           = '{}'\n", myObj);
        Set<String> mySet = (Set<String>) myObj;
        AssertJUnit.assertEquals(3, mySet.size());
        AssertJUnit.assertTrue(mySet.contains("white"));
        AssertJUnit.assertTrue(myObj instanceof LinkedHashSet);
    }

    @Test
    public void testUpdateSet() throws Exception
    {
        if (LOG.isDebugEnabled()) LOG.debug("Test: 'testUpdateSet'\n");
        
        Statement statement = con.createStatement();

        // add some items to the set
        String update1 = "UPDATE testcollection SET S = S + {'green', 'white', 'orange'} WHERE k = 1;";
        statement.executeUpdate(update1);

        ResultSet result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        AssertJUnit.assertEquals(1, result.getInt("k"));
        Object myObj = result.getObject("s");
        Set<String> mySet = (Set<String>) myObj;
        AssertJUnit.assertEquals(5, mySet.size());
        AssertJUnit.assertTrue(mySet.contains("white"));

        if (LOG.isDebugEnabled()) LOG.debug("s           = '{}'", myObj);

        // remove an item from the set
        String update2 = "UPDATE testcollection SET S = S - {'red'} WHERE k = 1;";
        statement.executeUpdate(update2);

        result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        AssertJUnit.assertEquals(1, result.getInt("k"));

        myObj = result.getObject("s");
        mySet = (Set<String>) myObj;
        AssertJUnit.assertEquals(4, mySet.size());
        AssertJUnit.assertTrue(mySet.contains("white"));
        AssertJUnit.assertFalse(mySet.contains("red"));

        if (LOG.isDebugEnabled()) LOG.debug("s           = '{}'", myObj);
        
        String update4 = "UPDATE testcollection SET S =  ? WHERE k = 1;";
        
        PreparedStatement prepared = con.prepareStatement(update4);
        Set<String> myNewSet = new HashSet<String>();
        myNewSet.add("black");
        myNewSet.add("blue");
        prepared.setObject(1, myNewSet, Types.OTHER);
        prepared.execute();

        result = prepared.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();
        myObj = result.getObject("s");
        mySet = (Set<String>) myObj;
        
        if (LOG.isDebugEnabled()) LOG.debug("s (prepared)= '{}'\n", myObj);
    }

    @Test
    public void testReadMap() throws Exception
    {
        if (LOG.isDebugEnabled()) LOG.debug("Test: 'testReadMap'\n");

        Statement statement = con.createStatement();

        String update2 = "UPDATE testcollection SET M = {2.0: true, 4.0: false, 6.0 : true} WHERE k = 1;";
        statement.executeUpdate(update2);
        ResultSet result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        AssertJUnit.assertEquals(1, result.getInt("k"));

        Object myObj = result.getObject("m");
        if (LOG.isDebugEnabled()) LOG.debug("m           = '{}'\n", myObj);
        Map<Double,Boolean> myMap = (Map<Double,Boolean>) myObj;
        AssertJUnit.assertEquals(3, myMap.size());
        AssertJUnit.assertTrue(myMap.keySet().contains(2.0));
        AssertJUnit.assertTrue(myObj instanceof HashMap);
    }

    @Test
    public void testUpdateMap() throws Exception
    {
        if (LOG.isDebugEnabled()) LOG.debug("Test: 'testUpdateMap'\n");
        
        Statement statement = con.createStatement();

        // add some items to the set
        String update1 = "UPDATE testcollection SET M = M + {1.0: true, 3.0: false, 5.0: false} WHERE k = 1;";
        statement.executeUpdate(update1);

        ResultSet result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        AssertJUnit.assertEquals(1, result.getInt("k"));
        Object myObj = result.getObject("m");
        Map<Double,Boolean> myMap = (Map<Double,Boolean>) myObj;
        AssertJUnit.assertEquals(6, myMap.size());
        AssertJUnit.assertTrue(myMap.keySet().contains(5.0));

        if (LOG.isDebugEnabled()) LOG.debug("m           = '{}'", myObj);

        // remove an item from the map
        String update2 = "DELETE M[6.0] FROM testcollection WHERE k = 1;";
        statement.executeUpdate(update2);

        result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        AssertJUnit.assertEquals(1, result.getInt("k"));

        myObj = result.getObject("m");
        myMap = (Map<Double,Boolean>) myObj;
        AssertJUnit.assertEquals(5, myMap.size());
        AssertJUnit.assertTrue(myMap.keySet().contains(5.0));
        AssertJUnit.assertFalse(myMap.keySet().contains(6.0));

        if (LOG.isDebugEnabled()) LOG.debug("m           = '{}'", myObj);
        
        String update4 = "UPDATE testcollection SET M =  ? WHERE k = 1;";
        
        PreparedStatement prepared = con.prepareStatement(update4);
        Map<Double,Boolean> myNewMap = new LinkedHashMap<Double,Boolean> ();
        myNewMap.put(10.0, false);
        myNewMap.put(12.0, true);
        prepared.setObject(1, myNewMap, Types.OTHER);
        prepared.execute();

        result = prepared.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();
        myObj = result.getObject("m");
        myMap = (Map<Double,Boolean>) myObj;
        
        if (LOG.isDebugEnabled()) LOG.debug("m (prepared)= '{}'\n", myObj);
    }


    private CassandraResultSetExtras extras(ResultSet result) throws Exception
    {
        Class crse = Class.forName("com.datastax.driver.jdbc.CassandraResultSetExtras");
        return (CassandraResultSetExtras) result.unwrap(crse);
    }

}
