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

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PooledCassandraConnection implements PooledConnection
{
	private static final Logger logger = LoggerFactory.getLogger(PooledCassandraConnection.class);
	
	private CassandraConnection physicalConnection;

	volatile Set<ConnectionEventListener> connectionEventListeners = new HashSet<ConnectionEventListener>();

	volatile Set<StatementEventListener> statementEventListeners = new HashSet<StatementEventListener>();

	private Map<String, Set<CassandraPreparedStatement>> freePreparedStatements = new HashMap<String, Set<CassandraPreparedStatement>>();

	private Map<String, Set<CassandraPreparedStatement>> usedPreparedStatements = new HashMap<String, Set<CassandraPreparedStatement>>();

	public PooledCassandraConnection(CassandraConnection physicalConnection)
	{
		this.physicalConnection = physicalConnection;
	}

	@Override
	public CassandraConnection getConnection()
	{
		return physicalConnection;
	}

	@Override
	public void close() throws SQLException
	{
		physicalConnection.close();
	}

	@Override
	public void addConnectionEventListener(ConnectionEventListener listener)
	{
		connectionEventListeners.add(listener);
	}

	@Override
	public void removeConnectionEventListener(ConnectionEventListener listener)
	{
		connectionEventListeners.remove(listener);
	}

	@Override
	public void addStatementEventListener(StatementEventListener listener)
	{
		statementEventListeners.add(listener);
	}

	@Override
	public void removeStatementEventListener(StatementEventListener listener)
	{
		statementEventListeners.remove(listener);
	}

	void connectionClosed()
	{
		ConnectionEvent event = new ConnectionEvent(this);
		for (ConnectionEventListener listener : connectionEventListeners)
		{
			listener.connectionClosed(event);
		}
	}

	void connectionErrorOccurred(SQLException sqlException)
	{
		ConnectionEvent event = new ConnectionEvent(this, sqlException);
		for (ConnectionEventListener listener : connectionEventListeners)
		{
			listener.connectionErrorOccurred(event);
		}
	}

	void statementClosed(CassandraPreparedStatement preparedStatement)
	{
		StatementEvent event = new StatementEvent(this, preparedStatement);
		for (StatementEventListener listener : statementEventListeners)
		{
			listener.statementClosed(event);
		}

		String cql = preparedStatement.getCql();
		Set<CassandraPreparedStatement> freeStatements = freePreparedStatements.get(cql);
		Set<CassandraPreparedStatement> usedStatements = usedPreparedStatements.get(cql);

		usedStatements.remove(preparedStatement);
		
		preparedStatement.resetResults();
		try
		{
			preparedStatement.clearParameters();
			freeStatements.add(preparedStatement);
		}
		catch (SQLException e)
		{
			logger.error(e.getMessage());
		}

	}

	void statementErrorOccurred(CassandraPreparedStatement preparedStatement, SQLException sqlException)
	{
		StatementEvent event = new StatementEvent(this, preparedStatement, sqlException);
		for (StatementEventListener listener : statementEventListeners)
		{
			listener.statementErrorOccurred(event);
		}
		
		String cql = preparedStatement.getCql();
		Set<CassandraPreparedStatement> usedStatements = usedPreparedStatements.get(cql);
		
		if (!(event.getSQLException() instanceof SQLRecoverableException))
		{
			preparedStatement.close();
			usedStatements.remove(preparedStatement);
		}
	}

	public synchronized ManagedPreparedStatement prepareStatement(ManagedConnection managedConnection, String cql) throws SQLException
	{
		if (!freePreparedStatements.containsKey(cql)) {
			freePreparedStatements.put(cql, new HashSet<CassandraPreparedStatement>());
			usedPreparedStatements.put(cql, new HashSet<CassandraPreparedStatement>());
		}
		
		Set<CassandraPreparedStatement> freeStatements = freePreparedStatements.get(cql);
		Set<CassandraPreparedStatement> usedStatements = usedPreparedStatements.get(cql);

		CassandraPreparedStatement managedPreparedStatement;
		if (freeStatements.isEmpty())
		{
			managedPreparedStatement = physicalConnection.prepareStatement(cql);
		}
		else
		{
			managedPreparedStatement = freeStatements.iterator().next();
			freeStatements.remove(managedPreparedStatement);
		}
		usedStatements.add(managedPreparedStatement);
		
		return new ManagedPreparedStatement(this, managedConnection, managedPreparedStatement);
	}

}
