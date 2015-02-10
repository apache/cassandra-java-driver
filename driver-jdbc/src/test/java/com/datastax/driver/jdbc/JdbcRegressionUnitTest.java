/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package com.datastax.driver.jdbc;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Blob;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.jdbc.CassandraStatementExtras;

public class JdbcRegressionUnitTest
{
    private static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort()+""));
    private static final String KEYSPACE = "testks";
    private static final String TABLE = "regressiontest";
//    private static final String CQLV3 = "3.0.0";
    private static final String CONSISTENCY_QUORUM = "QUORUM";
      
    private static java.sql.Connection con = null;
    

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Class.forName("org.apache.cassandra2.cql.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,"system");
        System.out.println("Connection URL = '"+URL +"'");
        
        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();
        
        // Drop Keyspace
        String dropKS = String.format("DROP KEYSPACE \"%s\";",KEYSPACE);
        
        try { stmt.execute(dropKS);}
        catch (Exception e){/* Exception on DROP is OK */}

        // Create KeySpace
        String createKS = String.format("CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",KEYSPACE);
        System.out.println("createKS = '"+createKS+"'");
        stmt = con.createStatement();
        stmt.execute("USE system;");
        stmt.execute(createKS);
        
        // Use Keyspace
        String useKS = String.format("USE \"%s\";",KEYSPACE);
        stmt.execute(useKS);
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY "+TABLE+" (keyname text PRIMARY KEY,"
                        + " bValue boolean,"
                        + " iValue int"
                        + ");";
        stmt.execute(createCF);
        
        //create an index
        stmt.execute("CREATE INDEX ON "+TABLE+" (iValue)");
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
        System.out.println(con);

    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (con!=null) con.close();
    }


    @Test
    public void testIssue10() throws Exception
    {
        String insert = "INSERT INTO regressiontest (keyname,bValue,iValue) VALUES( 'key0',true, 2000);";
        Statement statement = con.createStatement();

        statement.executeUpdate(insert);
        statement.close();
        
        statement = con.createStatement();
        ResultSet result = statement.executeQuery("SELECT bValue,iValue FROM regressiontest WHERE keyname='key0';");
        result.next();
        
        boolean b = result.getBoolean(1);
        assertTrue(b);
        
        int i = result.getInt(2);
        assertEquals(2000, i);
   }

    @Test
    public void testIssue15() throws Exception
    {
//        con.close();
//
//        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s?version=%s",HOST,PORT,KEYSPACE,CQLV3));
//        System.out.println(con);
//        con.close();

//        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
//        System.out.println(con);
//        con.close();

    }
    
    @Test
    public void testIssue18() throws Exception
    {
       Statement statement = con.createStatement();

       String truncate = "TRUNCATE regressiontest;";
       statement.execute(truncate);
       
       String insert1 = "INSERT INTO regressiontest (keyname,bValue,iValue) VALUES( 'key0',true, 2000);";
       statement.executeUpdate(insert1);
       
       String insert2 = "INSERT INTO regressiontest (keyname,bValue) VALUES( 'key1',false);";
       statement.executeUpdate(insert2);
       
       
       
       String select = "SELECT * from regressiontest;";
       
       ResultSet result = statement.executeQuery(select);
       
       ResultSetMetaData metadata = result.getMetaData();
       
       int colCount = metadata.getColumnCount();
       
       System.out.println("Before doing a next()");
       System.out.printf("(%d) ",result.getRow());
       for (int i = 1; i <= colCount; i++)
       {
           System.out.print(showColumn(i,result)+ " "); 
       }
       System.out.println();
       
       
       System.out.println("Fetching each row with a next()");
       while (result.next())
       {
           metadata = result.getMetaData();
           colCount = metadata.getColumnCount();
           System.out.printf("(%d) ",result.getRow());
           for (int i = 1; i <= colCount; i++)
           {
               System.out.print(showColumn(i,result)+ " "); 
           }
           System.out.println();
       }
    }
    
    @Test
    public void testIssue33() throws Exception
    {
        Statement stmt = con.createStatement();
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY t33 (k int PRIMARY KEY," 
                        + "c text "
                        + ") ;";        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
        
        // paraphrase of the snippet from the ISSUE #33 provided test
        PreparedStatement statement = con.prepareStatement("update t33 set c=? where k=123");
        statement.setString(1, "mark");
        statement.executeUpdate();

        ResultSet result = statement.executeQuery("SELECT * FROM t33;");
        
        ResultSetMetaData metadata = result.getMetaData();        
        
        int colCount = metadata.getColumnCount();
        
        System.out.println("Test Issue #33");
        DatabaseMetaData md = con.getMetaData();
        System.out.println();        
        System.out.println("--------------");
        System.out.println("Driver Version :   " + md.getDriverVersion());
        System.out.println("DB Version     :   " + md.getDatabaseProductVersion());
        System.out.println("Catalog term   :   " + md.getCatalogTerm());
        System.out.println("Catalog        :   " + con.getCatalog());
        System.out.println("Schema term    :   " + md.getSchemaTerm());
        
        System.out.println("--------------");
        while (result.next())
        {
            metadata = result.getMetaData();
            colCount = metadata.getColumnCount();
            System.out.printf("(%d) ",result.getRow());
            for (int i = 1; i <= colCount; i++)
            {
                System.out.print(showColumn(i,result)+ " "); 
            }
            System.out.println();
        }
   }

/* 
 * 		DEACTIVATED BECAUSE ALREADY EXECUTED IN testIssue33 and often fails due to arbitrary order execution of tests in Junit
 *    @Test
    public void testIssue38() throws Exception
    {
        
        // test catching exception for beforeFirst() and afterLast()
        Statement stmt = con.createStatement();

        ResultSet result = stmt.executeQuery("SELECT * FROM t33;");
        
        try
        {
            result.beforeFirst();
        }
        catch (Exception e)
        {
            System.out.println();
            System.out.println("beforeFirst() test -> "+ e);
        }
        
    }
  */  
    @Test
    public void testIssue40() throws Exception
    {
        DatabaseMetaData md = con.getMetaData();
        System.out.println();
        System.out.println("Test Issue #40");
        System.out.println("--------------");

        // test various retrieval methods
        ResultSet result = md.getTables(con.getCatalog(), null, "%", new String[]
        { "TABLE" });
        assertTrue("Make sure we have found a table", result.next());
        result = md.getTables(null, KEYSPACE, TABLE, null);
        assertTrue("Make sure we have found the table asked for", result.next());
        result = md.getTables(null, KEYSPACE, TABLE, new String[]
        { "TABLE" });
        assertTrue("Make sure we have found the table asked for", result.next());
        result = md.getTables(con.getCatalog(), KEYSPACE, TABLE, new String[]
        { "TABLE" });
        assertTrue("Make sure we have found the table asked for", result.next());

        // check the table name
        String tn = result.getString("TABLE_NAME");
        assertEquals("Table name match", TABLE, tn);
        System.out.println("Found table via dmd    :   " + tn);

        // load the columns
        result = md.getColumns(con.getCatalog(), KEYSPACE, TABLE, null);
        assertTrue("Make sure we have found first column", result.next());
        assertEquals("Make sure table name match", TABLE, result.getString("TABLE_NAME"));
        String cn = result.getString("COLUMN_NAME");
        System.out.println("Found (default) PK column       :   " + cn);
        assertEquals("Column name check", "keyname", cn);
        assertEquals("Column type check", Types.VARCHAR, result.getInt("DATA_TYPE"));
        assertTrue("Make sure we have found second column", result.next());
        cn = result.getString("COLUMN_NAME");
        System.out.println("Found column       :   " + cn);
        assertEquals("Column name check", "bvalue", cn);
        assertEquals("Column type check", Types.BOOLEAN, result.getInt("DATA_TYPE"));
        assertTrue("Make sure we have found thirth column", result.next());
        cn = result.getString("COLUMN_NAME");
        System.out.println("Found column       :   " + cn);
        assertEquals("Column name check", "ivalue", cn);
        assertEquals("Column type check", Types.INTEGER, result.getInt("DATA_TYPE"));

        // make sure we filter
        result = md.getColumns(con.getCatalog(), KEYSPACE, TABLE, "bvalue");
        result.next();
        assertFalse("Make sure we have found requested column only", result.next());
    }    
    
    @Test
    public void testIssue59() throws Exception
    {
        Statement stmt = con.createStatement();
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY t59 (k int PRIMARY KEY," 
                        + "c text "
                        + ") ;";        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
 
        PreparedStatement statement = con.prepareStatement("update t59 set c=? where k=123", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setString(1, "hello");
        statement.executeUpdate();

        ResultSet result = statement.executeQuery("SELECT * FROM t59;");
        
        System.out.println(resultToDisplay(result,59,null));

    }
    
    @Test
    public void testIssue65() throws Exception
    {
        Statement stmt = con.createStatement();
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY t65 (key text PRIMARY KEY," 
                        + "int1 int, "
                        + "int2 int, "
                        + "intset  set<int> "
                        + ") ;";        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
        
        Statement statement = con.createStatement();
        String insert = "INSERT INTO t65 (key, int1,int2,intset) VALUES ('key1',1,100,{10,20,30,40});";
        statement.executeUpdate(insert);
        
        ResultSet result = statement.executeQuery("SELECT * FROM t65;");

        System.out.println(resultToDisplay(result,65, "with set = {10,20,30,40}"));
       
        String update = "UPDATE t65 SET intset=? WHERE key=?;";
 
        PreparedStatement pstatement = con.prepareStatement(update);
        Set<Integer> mySet = new HashSet<Integer> ();
        pstatement.setObject(1, mySet, Types.OTHER);
        pstatement.setString(2, "key1");
       
        pstatement.executeUpdate();

        result = statement.executeQuery("SELECT * FROM t65;");
        
        System.out.println(resultToDisplay(result,65," with set = <empty>"));

    }
    
    @Test
    public void testIssue71() throws Exception
    {
        Statement stmt = con.createStatement();
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY t71 (k int PRIMARY KEY," 
                        + "c text "
                        + ") ;";        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
       con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s?consistency=%s",HOST,PORT,KEYSPACE,CONSISTENCY_QUORUM));
      
       // at this point defaultConsistencyLevel should be set the QUORUM in the connection

       stmt = con.createStatement();
       
       ConsistencyLevel cl = statementExtras(stmt).getConsistencyLevel();
       assertTrue(ConsistencyLevel.QUORUM == cl );
       
       System.out.println();
       System.out.println("Test Issue #71");
       System.out.println("--------------");
       System.out.println("statement.consistencyLevel = "+ cl);
       


    }
    
    @Test
    public void testIssue74() throws Exception
    {
        Statement stmt = con.createStatement();
        java.util.Date now = new java.util.Date();

        
        // Create the target Column family
        //String createCF = "CREATE COLUMNFAMILY t74 (id BIGINT PRIMARY KEY, col1 TIMESTAMP)";        
        String createCF = "CREATE COLUMNFAMILY t74 (id BIGINT PRIMARY KEY, col1 TIMESTAMP)";
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
        
        Statement statement = con.createStatement();
        
        String insert = "INSERT INTO t74 (id, col1) VALUES (?, ?);";
        
        PreparedStatement pstatement = con.prepareStatement(insert);
        pstatement.setLong(1, 1L); 
        pstatement.setObject(2, new Timestamp(now.getTime()),Types.TIMESTAMP);
        pstatement.execute();

        ResultSet result = statement.executeQuery("SELECT * FROM t74;");
        
        assertTrue(result.next());
        assertEquals(1L, result.getLong(1));
        Timestamp stamp = result.getTimestamp(2);
        
        assertEquals(now, stamp);
        stamp = (Timestamp)result.getObject(2); // maybe exception here
        assertEquals(now, stamp);

        System.out.println(resultToDisplay(result,74, "current date"));
       
    }

    @Test
    public void testIssue75() throws Exception
    {
        System.out.println();
        System.out.println("Test Issue #75");
        System.out.println("--------------");

        Statement stmt = con.createStatement();

        String truncate = "TRUNCATE regressiontest;";
        stmt.execute(truncate);

        String select = "select ivalue from "+TABLE;        
        
        ResultSet result = stmt.executeQuery(select);
        assertFalse("Make sure we have no rows", result.next());
        ResultSetMetaData rsmd = result.getMetaData();
        assertTrue("Make sure we do get a result", rsmd.getColumnDisplaySize(1) != 0);
        assertNotNull("Make sure we do get a label",rsmd.getColumnLabel(1));
        System.out.println("Found a column in ResultsetMetaData even when there are no rows:   " + rsmd.getColumnLabel(1));
        stmt.close();
    }

    @Test
    public void testIssue76() throws Exception
    {
        DatabaseMetaData md = con.getMetaData();
        System.out.println();
        System.out.println("Test Issue #76");
        System.out.println("--------------");

        // test various retrieval methods
        ResultSet result = md.getIndexInfo(con.getCatalog(), KEYSPACE, TABLE, false, false);
        assertTrue("Make sure we have found an index", result.next());

        // check the column name from index
        String cn = result.getString("COLUMN_NAME");
        assertEquals("Column name match for index", "ivalue", cn);
        System.out.println("Found index via dmd on :   " + cn);
    }

    @Test
    public void testIssue77() throws Exception
    {
        DatabaseMetaData md = con.getMetaData();
        System.out.println();
        System.out.println("Test Issue #77");
        System.out.println("--------------");

        // test various retrieval methods
        ResultSet result = md.getPrimaryKeys(con.getCatalog(), KEYSPACE, TABLE);
        assertTrue("Make sure we have found an pk", result.next());

        // check the column name from index
        String cn = result.getString("COLUMN_NAME");
        assertEquals("Column name match for pk", "keyname", cn);
        System.out.println("Found pk via dmd :   " + cn);
    }

    @Test
    public void testIssue78() throws Exception
    {
        DatabaseMetaData md = con.getMetaData();

        // load the columns, with no catalog and schema
        ResultSet result = md.getColumns(null, "%", TABLE, "ivalue");
        assertTrue("Make sure we have found an column", result.next());
    }

    @Test
    public void testIssue80() throws Exception
    {
        
    	System.out.println();
        System.out.println("Test Issue #80");
        System.out.println("--------------");
        
    	Statement stmt = con.createStatement();
        java.util.Date now = new java.util.Date();
        
        
        // Create the target Column family with each basic data type available on Cassandra
                
        String createCF = "CREATE COLUMNFAMILY t80 (bigint_col bigint PRIMARY KEY, ascii_col ascii , blob_col blob, boolean_col boolean, decimal_col decimal, double_col double, "+
        										" float_col float, inet_col inet, int_col int, text_col text, timestamp_col timestamp, uuid_col uuid," + 
        										"timeuuid_col timeuuid, varchar_col varchar, varint_col varint,string_set_col set<text>,string_list_col list<text>, string_map_col map<text,text>);";
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s?debug=true",HOST,PORT,KEYSPACE));
        System.out.println("con.getMetaData().getDatabaseProductName() = " + con.getMetaData().getDatabaseProductName());
        System.out.println("con.getMetaData().getDriverName() = " + con.getMetaData().getDriverName());
        Statement statement = con.createStatement();
        /*
         * INSERT INTO test.t80(bigint_col , ascii_col , blob_col , boolean_col , decimal_col , double_col , 
        										float_col , inet_col , int_col , text_col , timestamp_col , uuid_col , 
        										timeuuid_col , varchar_col , varint_col )
        			values(1, 'test', TextAsBlob('test'), true, 5.1, 5.123142 , 
        										4.2134432 , '192.168.1.1', 1 , 'text' , '2015-01-01 10:10:10' , now() , 
        										now(), 'test' , 3435 );
         * 
         */
        
        
        String insert = "INSERT INTO t80(bigint_col , ascii_col , blob_col , boolean_col , decimal_col , double_col , "
        				+ "float_col , inet_col , int_col , text_col , timestamp_col , uuid_col , timeuuid_col , varchar_col , varint_col, string_set_col, string_list_col, string_map_col) "
        			    + " values(?, ?, ?, ?, ?, ? , ?, ? , ? , ?, ? , ? , now(), ? , ?, ?, ?, ? );";
        
        
        
        
		
        PreparedStatement pstatement = con.prepareStatement(insert);
        
        
        pstatement.setObject(1, 1L); // bigint
        pstatement.setObject(2, "test"); // ascii                             
        pstatement.setObject(3, new ByteArrayInputStream("test".getBytes("UTF-8"))); // blob
        pstatement.setObject(4, true); // boolean
        pstatement.setObject(5, new BigDecimal(5.1));  // decimal
        pstatement.setObject(6, (double)5.1);  // decimal
        pstatement.setObject(7, (float)5.1);  // inet
        InetAddress inet = InetAddress.getLocalHost();
        pstatement.setObject(8, inet);  // inet
        pstatement.setObject(9, (int)1);  // int
        pstatement.setObject(10, "test");  // text
        pstatement.setObject(11, new Timestamp(now.getTime()));  // text
        UUID uuid = UUID.randomUUID();
        pstatement.setObject(12, uuid );  // uuid
        pstatement.setObject(13, "test");  // varchar
        pstatement.setObject(14, 1);        
        HashSet<String> mySet = new HashSet<String>();
        mySet.add("test");
        mySet.add("test");
        pstatement.setObject(15, mySet);
        ArrayList<String> myList = new ArrayList<String>();
        myList.add("test");
        myList.add("test");
        pstatement.setObject(16, myList);
        HashMap<String,String> myMap = new HashMap<String,String>();
        myMap.put("1","test");
        myMap.put("2","test");
        pstatement.setObject(17, myMap);
        
        
        pstatement.execute();
        
        pstatement.setLong(1, 2L); // bigint
        pstatement.setString(2, "test"); // ascii
        pstatement.setObject(3, new ByteArrayInputStream("test".getBytes("UTF-8"))); // blob
        pstatement.setBoolean(4, true); // boolean
        pstatement.setBigDecimal(5, new BigDecimal(5.1));  // decimal
        pstatement.setDouble(6, (double)5.1);  // decimal
        pstatement.setFloat(7, (float)5.1);  // inet        
        pstatement.setObject(8, inet);  // inet
        pstatement.setInt(9, 1);  // int
        pstatement.setString(10, "test");  // text
        pstatement.setTimestamp(11, new Timestamp(now.getTime()));  // text
        pstatement.setObject(12, uuid );  // uuid
        pstatement.setString(13, "test");  // varchar
        pstatement.setInt(14, 1);  // varint */
        pstatement.setString(15, mySet.toString());
        pstatement.setString(16, myList.toString());
        pstatement.setString(17, myMap.toString());
        
        pstatement.execute();

        ResultSet result = statement.executeQuery("SELECT * FROM t80 where bigint_col=1;");
        
        assertTrue(result.next());
        assertEquals(1L, result.getLong("bigint_col"));
        assertEquals("test", result.getString("ascii_col"));
        byte[] array = new byte[result.getBinaryStream("blob_col").available()];
    	try {
    		result.getBinaryStream("blob_col").read(array);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        assertEquals("test", new String(array, "UTF-8"));
        assertEquals(true, result.getBoolean("boolean_col"));
        assertEquals(new BigDecimal(5.1), result.getBigDecimal("decimal_col"));
        assertEquals((double)5.1, result.getDouble("double_col"),0);
        assertEquals((float)5.1, result.getFloat("float_col"),0);
        assertEquals(InetAddress.getLocalHost(), (InetAddress)result.getObject("inet_col"));
        assertEquals(1, result.getInt("int_col"));
        assertEquals("test", result.getString("text_col"));        
        assertEquals(new Timestamp(now.getTime()),result.getTimestamp("timestamp_col"));
        // 12 - cannot test timeuuid as it is generated by the server
        assertEquals(uuid,(UUID)result.getObject("uuid_col"));
        assertEquals("test",result.getString("varchar_col"));
        assertEquals(1,result.getLong("varint_col"));
        Set<String> retSet = (Set<String>) result.getObject("string_set_col");
        assertTrue(retSet instanceof LinkedHashSet);
        assertEquals(1,retSet.size());
        List<String> retList = (List<String>) result.getObject("string_list_col");
        assertTrue(retList instanceof ArrayList);
        assertEquals(2,retList.size());
        Map<String,String> retMap = (Map<String,String>) result.getObject("string_map_col");
        assertTrue(retMap instanceof HashMap);
        assertEquals(2,retMap.keySet().size());
        
        
        result = statement.executeQuery("SELECT * FROM t80 where bigint_col=2;");
        
        
        assertTrue(result.next());
        assertEquals(2L, result.getLong("bigint_col"));
        assertEquals("test", result.getString("ascii_col"));
        array = new byte[result.getBinaryStream("blob_col").available()];
    	try {
    		result.getBinaryStream("blob_col").read(array);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        assertEquals("test", new String(array, "UTF-8"));
        assertEquals(true, result.getBoolean("boolean_col"));
        assertEquals(new BigDecimal(5.1), result.getBigDecimal("decimal_col"));
        assertEquals((double)5.1, result.getDouble("double_col"),0);
        assertEquals((float)5.1, result.getFloat("float_col"),0);
        assertEquals(InetAddress.getLocalHost(), (InetAddress)result.getObject("inet_col"));
        assertEquals(1, result.getInt("int_col"));
        assertEquals("test", result.getString("text_col"));        
        assertEquals(new Timestamp(now.getTime()),result.getTimestamp("timestamp_col"));
        // 12 - cannot test timeuuid as it is generated by the server
        assertEquals(uuid,(UUID)result.getObject("uuid_col"));
        assertEquals("test",result.getString("varchar_col"));
        assertEquals(1,result.getLong("varint_col"));
        retSet = (Set<String>) result.getObject("string_set_col");
        assertTrue(retSet instanceof LinkedHashSet);
        assertEquals(1,retSet.size()); 
        retList = (List<String>) result.getObject("string_list_col");
        assertTrue(retList instanceof ArrayList);
        assertEquals(2,retList.size()); 
        retMap = (Map<String,String>) result.getObject("string_map_col");
        System.out.println("HashMap ??? " + retMap);
        assertTrue(retMap instanceof HashMap);
        assertEquals(2,retMap.keySet().size());
        
        statement.close();
        pstatement.close();
        
        
        
       
    }

    
    @Test
    public void testIssue102() throws Exception
    {
        // null int or long should be... null !
    	System.out.println();
        System.out.println("Test Issue #102");
        System.out.println("--------------");
        
    	Statement stmt = con.createStatement();
        java.util.Date now = new java.util.Date();
        
        
        // Create the target Column family with each basic data type available on Cassandra
                
        String createCF = "CREATE COLUMNFAMILY t102 (bigint_col bigint PRIMARY KEY, null_int_col int , null_bigint_col bigint);";
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
        System.out.println("con.getMetaData().getDatabaseProductName() = " + con.getMetaData().getDatabaseProductName());
        System.out.println("con.getMetaData().getDriverName() = " + con.getMetaData().getDriverName());
        Statement statement = con.createStatement();
        /*
         * INSERT INTO test.t80(bigint_col , ascii_col , blob_col , boolean_col , decimal_col , double_col , 
        										float_col , inet_col , int_col , text_col , timestamp_col , uuid_col , 
        										timeuuid_col , varchar_col , varint_col )
        			values(1, 'test', TextAsBlob('test'), true, 5.1, 5.123142 , 
        										4.2134432 , '192.168.1.1', 1 , 'text' , '2015-01-01 10:10:10' , now() , 
        										now(), 'test' , 3435 );
         * 
         */
        
        
        String insert = "INSERT INTO t102(bigint_col) values(?);";
        
        
        
        
		
        PreparedStatement pstatement = con.prepareStatement(insert);
        
        
        pstatement.setObject(1, 1L); // bigint
                
        pstatement.execute();
                
        ResultSet result = statement.executeQuery("SELECT * FROM t102 where bigint_col=1;");
        
        assertTrue(result.next());
        assertEquals(1L, result.getLong("bigint_col"));
        System.out.println("null_bigint_col = " +  result.getLong("null_bigint_col"));
        assertEquals(0L,result.getLong("null_bigint_col"));
        assertTrue(result.wasNull());
        assertEquals(0,result.getInt("null_int_col"));
        assertTrue(result.wasNull());
        
        statement.close();
        pstatement.close();
        
        
        
       
    }

    @Test
    public void testGetLongGetDouble() throws Exception
    {
        // null int or long should be... null !
    	System.out.println();
        System.out.println("Test getLong on int/varint and getDouble on float #102");
        System.out.println("--------------");
        
    	Statement stmt = con.createStatement();
        java.util.Date now = new java.util.Date();
        
        
        // Create the target Column family with each basic data type available on Cassandra
                
        String createCF = "CREATE COLUMNFAMILY getLongGetDouble(bigint_col bigint PRIMARY KEY, int_col int, varint_col varint, float_col float);";
        
        stmt.execute(createCF);
        stmt.close();
        //con.close();

        // open it up again to see the new CF
        //con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
        Statement statement = con.createStatement();
        
        String insert = "INSERT INTO getLongGetDouble(bigint_col, int_col, varint_col, float_col) values(?,?,?,?);";
		
        PreparedStatement pstatement = con.prepareStatement(insert);
        
        pstatement.setObject(1, 1L); // bigint
        pstatement.setInt(2, 1); // bigint
        pstatement.setInt(3, 1); // bigint
        pstatement.setFloat(4, (float)1.1); // bigint
                
        pstatement.execute();
                
        ResultSet result = statement.executeQuery("SELECT * FROM getLongGetDouble where bigint_col=1;");
        
        assertTrue(result.next());
        assertEquals(1L, result.getLong("bigint_col"));        
        assertEquals(1L,result.getLong("int_col"));
        assertEquals(1L,result.getLong("varint_col"));
        assertEquals((double)1.1,result.getDouble("float_col"),0.1);
        
        statement.close();
        pstatement.close();
        
        
        
       
    }
    
    @Test
    public void isValid() throws Exception
    {
//    	assert con.isValid(3);
    }
    
    @Test(expected=SQLException.class)
    public void isValidSubZero() throws Exception
    {
    	con.isValid(-42);
    }
    
    @Test
    public void isNotValid() throws Exception
    {
//        PreparedStatement currentStatement = ((CassandraConnection) con).isAlive;
//        PreparedStatement mockedStatement = mock(PreparedStatement.class);
//        when(mockedStatement.executeQuery()).thenThrow(new SQLException("A mocked ERROR"));
//        ((CassandraConnection) con).isAlive = mockedStatement;
//        assert con.isValid(5) == false;
//        ((CassandraConnection) con).isAlive = currentStatement;
    }
    
    private final String  showColumn(int index, ResultSet result) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(index).append("]");
        sb.append(result.getObject(index));
        return sb.toString();
    }
    
    private final String resultToDisplay(ResultSet result, int issue, String note) throws Exception
    {
        StringBuilder sb = new StringBuilder("Test Issue #" + issue + " - "+ note + "\n");
       ResultSetMetaData metadata = result.getMetaData();
        
        int colCount = metadata.getColumnCount();
        
        sb.append("--------------").append("\n");
        while (result.next())
        {
            metadata = result.getMetaData();
            colCount = metadata.getColumnCount();
            sb.append(String.format("(%d) ",result.getRow()));
            for (int i = 1; i <= colCount; i++)
            {
                sb.append(showColumn(i,result)+ " "); 
            }
            sb.append("\n");
        }
        
        return sb.toString();        
    }
    
    private CassandraStatementExtras statementExtras(Statement statement) throws Exception
    {
        Class cse = Class.forName("com.datastax.driver.jdbc.CassandraStatementExtras");
        return (CassandraStatementExtras) statement.unwrap(cse);
    }

}
