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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.jdbc.CassandraStatement;
import com.datastax.driver.jdbc.MetadataResultSets;


public class MetadataResultSetsUnitTest
{
    private static String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort()+""));
    private static final String KEYSPACE1 = "testks1";
    private static final String KEYSPACE2 = "testks2";
    private static final String DROP_KS = "DROP KEYSPACE \"%s\";";
    private static final String CREATE_KS = "CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";
      
    private static java.sql.Connection con = null;
    

    private static CCMBridge ccmBridge = null;
    

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    	/*System.setProperty("cassandra.version", "2.1.2");*/
    	HOST = CCMBridge.ipOfNode(1);
    	
        Class.forName("com.datastax.driver.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s?version=3.0.0",HOST,PORT,"system");
        System.out.println("Connection URL = '"+URL +"'");
        
        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();
        
        // Drop Keyspace
        String dropKS1 = String.format(DROP_KS,KEYSPACE1);
        String dropKS2 = String.format(DROP_KS,KEYSPACE2);
        
        try { stmt.execute(dropKS1); stmt.execute(dropKS2);}
        catch (Exception e){/* Exception on DROP is OK */}

        // Create KeySpace
        String createKS1 = String.format(CREATE_KS,KEYSPACE1);
        String createKS2 = String.format(CREATE_KS,KEYSPACE2);
        stmt = con.createStatement();
        stmt.execute("USE system;");
        stmt.execute(createKS1);
        stmt.execute(createKS2);
        
        // Use Keyspace
        String useKS1 = String.format("USE \"%s\";",KEYSPACE1);
        String useKS2 = String.format("USE \"%s\";",KEYSPACE2);
        stmt.execute(useKS1);
        
        // Create the target Column family
        String createCF1 = "CREATE COLUMNFAMILY test1 (keyname text PRIMARY KEY," 
                        + " t1bValue boolean,"
                        + " t1iValue int"
                        + ") WITH comment = 'first TABLE in the Keyspace'"
                        + ";";
        
        String createCF2 = "CREATE COLUMNFAMILY test2 (keyname text PRIMARY KEY," 
                        + " t2bValue boolean,"
                        + " t2iValue int"
                        + ") WITH comment = 'second TABLE in the Keyspace'"
                        + ";";
        
        
        stmt.execute(createCF1);
        stmt.execute(createCF2);
        stmt.execute(useKS2);
        stmt.execute(createCF1);
        stmt.execute(createCF2);

        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s?version=3.0.0",HOST,PORT,KEYSPACE1));

    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
    	if (con != null) con.close();
        
    }
    
    private final String showColumn(int index, ResultSet result) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(index).append("]");
        sb.append(result.getObject(index));
        return sb.toString();
    }

    private final String toString(ResultSet result) throws SQLException
    {
       StringBuilder sb = new StringBuilder();

       while (result.next())
       {
           ResultSetMetaData metadata = result.getMetaData();
           int colCount = metadata.getColumnCount();
           sb.append(String.format("(%d) ",result.getRow()));
           for (int i = 1; i <= colCount; i++)
           {
               sb.append(" " +showColumn(i,result)); 
           }
           sb.append("\n");
       }
       return sb.toString();
    }

	private final String getColumnNames(ResultSetMetaData metaData) throws SQLException
	{
       StringBuilder sb = new StringBuilder();
        int count = metaData.getColumnCount();
        for (int i = 1; i <= count; i++) {
            sb.append(metaData.getColumnName(i));
            if (i < count) sb.append(", ");
		}
        return sb.toString();
	}

    // TESTS ------------------------------------------------------------------
    
    @Test
    public void testTableType() throws SQLException
    {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.instance.makeTableTypes(statement);
        
        System.out.println("--- testTableType() ---");
        System.out.println(toString(result));       
        System.out.println();
    }

    @Test
    public void testCatalogs() throws SQLException
    {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.instance.makeCatalogs(statement);
        
        System.out.println("--- testCatalogs() ---");
        System.out.println(toString(result));       
        System.out.println();
    }

    @Test
    public void testSchemas() throws SQLException
    {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.instance.makeSchemas(statement, null);
        
        System.out.println("--- testSchemas() ---");
        System.out.println(getColumnNames(result.getMetaData()));
       
        System.out.println(toString(result));       
        System.out.println();
        
        result = MetadataResultSets.instance.makeSchemas(statement, KEYSPACE2);
        System.out.println(toString(result));       
        System.out.println();
    }
    
    @Test
    public void testTables() throws SQLException
    {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.instance.makeTables(statement, null, null);
        
        System.out.println("--- testTables() ---");
        System.out.println(getColumnNames(result.getMetaData()));
       
        System.out.println(toString(result));       
        System.out.println();
        
        result = MetadataResultSets.instance.makeTables(statement, KEYSPACE2, null);
        System.out.println(toString(result));       
        System.out.println();

        result = MetadataResultSets.instance.makeTables(statement, null, "test1");
        System.out.println(toString(result));       
        System.out.println();

        result = MetadataResultSets.instance.makeTables(statement, KEYSPACE2, "test1");
        System.out.println(toString(result));       
        System.out.println();
    }

    @Test
    public void testColumns() throws SQLException
    {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.instance.makeColumns(statement, KEYSPACE1, "test1" ,null);
        
        System.out.println("--- testColumns() ---");
        System.out.println(getColumnNames(result.getMetaData()));
       
        System.out.println(toString(result));       
        System.out.println();

        result = MetadataResultSets.instance.makeColumns(statement, KEYSPACE1, "test2" ,null);
        
        System.out.println("--- testColumns() ---");
        System.out.println(getColumnNames(result.getMetaData()));
       
        System.out.println(toString(result));       
        System.out.println();
    }
    
    
    @Test
    public void testCollectionsMetadata() throws Exception
    {
        
    	System.out.println();
        System.out.println("Collections metadata test");
        System.out.println("--------------");
        
    	Statement stmt = con.createStatement();
        java.util.Date now = new java.util.Date();
        
        
        // Create the target Column family with each basic data type available on Cassandra
       
        String createCF = "CREATE TABLE " + KEYSPACE1 + ".collections_metadata(part_key text PRIMARY KEY, set1 set<text>, description text, map2 map<text,int>, list3 list<text>);";
        
        stmt.execute(createCF);
        stmt.close();
                
        Statement statement = con.createStatement();
        String insert = "INSERT INTO " + KEYSPACE1 + ".collections_metadata(part_key, set1, description, map2, list3) VALUES ('part_key',{'val1','val2','val3'},'desc',{'val1':1,'val2':2},['val1','val2']);";
                
        ResultSet result = statement.executeQuery(insert);
        
        result = statement.executeQuery("select * from " + KEYSPACE1 + ".collections_metadata");
        
        
        assertTrue(result.next());
        assertEquals(5, result.getMetaData().getColumnCount());
        for(int i=1;i<=result.getMetaData().getColumnCount();i++){
        	System.out.println("getColumnName : " + result.getMetaData().getColumnName(i));
        	System.out.println("getCatalogName : " + result.getMetaData().getCatalogName(i));
        	System.out.println("getColumnClassName : " + result.getMetaData().getColumnClassName(i));
        	System.out.println("getColumnDisplaySize : " + result.getMetaData().getColumnDisplaySize(i));
        	System.out.println("getColumnLabel : " + result.getMetaData().getColumnLabel(i));        
        	System.out.println("getColumnType : " + result.getMetaData().getColumnType(i));
        	System.out.println("getColumnTypeName : " + result.getMetaData().getColumnTypeName(i));
        	System.out.println("getPrecision : " + result.getMetaData().getPrecision(i));
        	System.out.println("getScale : " + result.getMetaData().getScale(i));
        	System.out.println("getSchemaName : " + result.getMetaData().getSchemaName(i));
        	System.out.println("getTableName : " + result.getMetaData().getTableName(i));
        	System.out.println("==========================");
        }
        
        // columns are apparently ordered alphabetically and not according to the query order
        assertEquals("part_key", result.getMetaData().getColumnName(1));
        assertEquals("description", result.getMetaData().getColumnName(2));
        assertEquals("list3", result.getMetaData().getColumnName(3));
        assertEquals("map2", result.getMetaData().getColumnName(4));
        assertEquals("set1", result.getMetaData().getColumnName(5));
        
        assertEquals("part_key", result.getMetaData().getColumnLabel(1));
        assertEquals("description", result.getMetaData().getColumnLabel(2));
        assertEquals("list3", result.getMetaData().getColumnLabel(3));
        assertEquals("map2", result.getMetaData().getColumnLabel(4));
        assertEquals("set1", result.getMetaData().getColumnLabel(5));
        
        assertEquals("collections_metadata", result.getMetaData().getTableName(1));
        assertEquals("collections_metadata", result.getMetaData().getTableName(2));
        assertEquals("collections_metadata", result.getMetaData().getTableName(3));
        assertEquals("collections_metadata", result.getMetaData().getTableName(4));
        assertEquals("collections_metadata", result.getMetaData().getTableName(5));
        
        assertEquals("java.lang.String", result.getMetaData().getColumnClassName(1));
        assertEquals("java.lang.String", result.getMetaData().getColumnClassName(2));
        assertEquals("java.util.List", result.getMetaData().getColumnClassName(3));
        assertEquals("java.util.Map", result.getMetaData().getColumnClassName(4));
        assertEquals("java.util.Set", result.getMetaData().getColumnClassName(5));
        
        assertEquals(12, result.getMetaData().getColumnType(1));
        assertEquals(12, result.getMetaData().getColumnType(2));
        assertEquals(1111, result.getMetaData().getColumnType(3));
        assertEquals(1111, result.getMetaData().getColumnType(4));
        assertEquals(1111, result.getMetaData().getColumnType(5));
        
        
        
        statement.close();
        
        
        
        
       
    }

    
}
