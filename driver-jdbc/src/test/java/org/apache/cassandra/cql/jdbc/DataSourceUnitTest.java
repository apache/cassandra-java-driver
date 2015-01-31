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
package org.apache.cassandra.cql.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.cassandra.cql.ConnectionDetails;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataSourceUnitTest
{
    private static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort()+""));
    private static final String KEYSPACE = "testks";
    private static final String USER = "JohnDoe";
    private static final String PASSWORD = "secret";
    private static final String VERSION = "3.0.0";
    private static final String CONSISTENCY = "ONE";
    
    private static java.sql.Connection con = null;


    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,"system"));
        Statement stmt = con.createStatement();
        
        // Drop Keyspace
        String dropKS = String.format("DROP KEYSPACE \"%s\";",KEYSPACE);
        
        try { stmt.execute(dropKS);}
        catch (Exception e){/* Exception on DROP is OK */}
        
        // Create KeySpace
        String createKS = String.format("CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",KEYSPACE);
//        String createKS = String.format("CREATE KEYSPACE %s WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1;",KEYSPACE);
        stmt.execute(createKS);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (con!=null) con.close();
    }


    @Test
    public void testConstructor() throws Exception
    {
        CassandraDataSource cds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD,VERSION,CONSISTENCY);
        assertEquals(HOST,cds.getServerName());
        assertEquals(PORT,cds.getPortNumber());
        assertEquals(KEYSPACE,cds.getDatabaseName());
        assertEquals(USER,cds.getUser());
        assertEquals(PASSWORD,cds.getPassword());
        assertEquals(VERSION, cds.getVersion());
        
        DataSource ds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD,VERSION,CONSISTENCY);
        assertNotNull(ds);
        
        // null username and password
        java.sql.Connection cnx = ds.getConnection(null, null);
        assertFalse(cnx.isClosed());
        ds.setLoginTimeout(5);
        assertEquals(5, ds.getLoginTimeout());

        // no username and password
        cnx = ds.getConnection();
        assertFalse(cnx.isClosed());
        ds.setLoginTimeout(5);
        assertEquals(VERSION, ((CassandraConnection) cnx).getConnectionProps().get(Utils.TAG_CQL_VERSION));
        assertEquals(5, ds.getLoginTimeout());
    }

    
    @Test
    public void testIsWrapperFor() throws Exception
    {
        DataSource ds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD,VERSION,CONSISTENCY);
        
        boolean isIt = false;
                
        // it is a wrapper for DataSource
        isIt = ds.isWrapperFor(DataSource.class);        
        assertTrue(isIt);
        
        // it is not a wrapper for this test class
        isIt = ds.isWrapperFor(this.getClass());        
        assertFalse(isIt);
    }
 
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testUnwrap() throws Exception
    {
        DataSource ds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD,VERSION,CONSISTENCY);

        // it is a wrapper for DataSource
        DataSource newds = ds.unwrap(DataSource.class);        
        assertNotNull(newds);
        
        // it is not a wrapper for this test class
        newds = (DataSource) ds.unwrap(this.getClass());        
        assertNotNull(newds);
    }
}
