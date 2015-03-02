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

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.sql.DataSource;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.jdbc.CassandraDataSource;
import com.datastax.driver.jdbc.PooledCassandraDataSource;

public class PooledUnitTest
{
	private static String HOST = System.getProperty("host", ConnectionDetails.getHost());
	private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort() + ""));
	private static final String KEYSPACE = "testks";
	private static final String USER = "JohnDoe";
	private static final String PASSWORD = "secret";
	private static final String VERSION = "3.0.0";
    private static final String CONSISTENCY = "ONE";
    

	private static java.sql.Connection con = null;
    private static CCMBridge ccmBridge = null;

    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    	/*System.setProperty("cassandra.version", "2.1.2");*/
    	HOST = CCMBridge.ipOfNode(1);        

		Class.forName("com.datastax.driver.jdbc.CassandraDriver");
		con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, "system"));
		Statement stmt = con.createStatement();

		// Drop Keyspace
		String dropKS = String.format("DROP KEYSPACE \"%s\";", KEYSPACE);

		try
		{
			stmt.execute(dropKS);
		}
		catch (Exception e)
		{/* Exception on DROP is OK */
		}

		// Create KeySpace
		String createKS = String.format(
				"CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
				KEYSPACE);
		stmt.execute(createKS);

		// Create KeySpace
		String useKS = String.format("USE \"%s\";", KEYSPACE);
		stmt.execute(useKS);

		// Create the target Column family
		String createCF = "CREATE COLUMNFAMILY pooled_test (somekey text PRIMARY KEY," + "someInt int"
				+ ") ;";
		stmt.execute(createCF);

		String insertWorld = "UPDATE pooled_test SET someInt = 1 WHERE somekey = 'world'";
		stmt.execute(insertWorld);
	}

   @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
    	if (con != null) con.close();
    }

	@Test
	public void twoMillionConnections() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		for (int i = 0; i < 2000000; i++)
		{
			Connection connection = pooledCassandraDataSource.getConnection();
			connection.close();
		}
	}

	@Test
	public void twoMillionPreparedStatements() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();
		for (int i = 0; i < 10000; i++)
		{
			PreparedStatement preparedStatement = connection.prepareStatement("SELECT someInt FROM pooled_test WHERE somekey = ?");
			preparedStatement.close();
		}
		connection.close();
	}

	@Test
	public void preparedStatement() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		PreparedStatement statement = connection.prepareStatement("SELECT someInt FROM pooled_test WHERE somekey = ?");
		statement.setString(1, "world");

		ResultSet resultSet = statement.executeQuery();
		assert resultSet.next();
		assert resultSet.getInt(1) == 1;
		assert resultSet.next() == false;
		resultSet.close();

		statement.close();
		connection.close();
	}

	@Test
	public void preparedStatementClose() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		PreparedStatement statement = connection.prepareStatement("SELECT someInt FROM pooled_test WHERE somekey = ?");
		statement.setString(1, "world");

		ResultSet resultSet = statement.executeQuery();
		assert resultSet.next();
		assert resultSet.getInt(1) == 1;
		assert resultSet.next() == false;
		resultSet.close();

		connection.close();
		
		assert statement.isClosed();
	}

	@Test
	public void statement() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		Statement statement = connection.createStatement();

		ResultSet resultSet = statement.executeQuery("SELECT someInt FROM pooled_test WHERE somekey = 'world'");
		assert resultSet.next();
		assert resultSet.getInt(1) == 1;
		assert resultSet.next() == false;
		resultSet.close();

		statement.close();
		connection.close();
	}

	@Test
	public void statementClosed() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		Statement statement = connection.createStatement();

		ResultSet resultSet = statement.executeQuery("SELECT someInt FROM pooled_test WHERE somekey = 'world'");
		assert resultSet.next();
		assert resultSet.getInt(1) == 1;
		assert resultSet.next() == false;
		resultSet.close();

		connection.close();
		
		assert statement.isClosed();
	}
}
