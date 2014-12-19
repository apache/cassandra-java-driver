package org.apache.cassandra.cql.jdbc;

import static org.apache.cassandra.cql.jdbc.Utils.WAS_CLOSED_CON;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

class ManagedPreparedStatement extends AbstractStatement implements PreparedStatement
{
	private PooledCassandraConnection pooledCassandraConnection;

	private ManagedConnection managedConnection;

	private CassandraPreparedStatement preparedStatement;

	private boolean poolable;

	ManagedPreparedStatement(PooledCassandraConnection pooledCassandraConnection, ManagedConnection managedConnection,
			CassandraPreparedStatement preparedStatement)
	{
		this.pooledCassandraConnection = pooledCassandraConnection;
		this.managedConnection = managedConnection;
		this.preparedStatement = preparedStatement;
	}

	private void checkNotClosed() throws SQLNonTransientConnectionException
	{
		if (isClosed())
		{
			throw new SQLNonTransientConnectionException(WAS_CLOSED_CON);
		}
	}

	@Override
	public ManagedConnection getConnection() throws SQLException
	{
		return managedConnection;
	}

	@Override
	public boolean isClosed()
	{
		return preparedStatement == null || preparedStatement.isClosed();
	}

	@Override
	public void close() throws SQLNonTransientConnectionException
	{
		checkNotClosed();
		pooledCassandraConnection.statementClosed(preparedStatement);
		preparedStatement = null;
	}

	@Override
	public boolean isPoolable()
	{
		return poolable;
	}

	@Override
	public void setPoolable(boolean poolable)
	{
		this.poolable = poolable;
	}

	@Override
	public int hashCode()
	{
		return preparedStatement.getCql().hashCode();
	}

	// all following methods have the form
	// checkNotClosed-try-return/void-catch-notify-throw

	@Override
	public void addBatch(String arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.addBatch(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void clearBatch() throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.clearBatch();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.clearWarnings();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public boolean execute(String arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.execute(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public boolean execute(String arg0, int arg1) throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.execute(arg0, arg1);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int[] executeBatch() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.executeBatch();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public ResultSet executeQuery(String arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.executeQuery(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int executeUpdate(String arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.executeUpdate(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int executeUpdate(String arg0, int arg1) throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.executeUpdate(arg0, arg1);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int getFetchDirection() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getFetchDirection();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int getFetchSize() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getFetchSize();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int getMaxFieldSize() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getMaxFieldSize();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int getMaxRows() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getMaxRows();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public boolean getMoreResults() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getMoreResults();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public boolean getMoreResults(int arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getMoreResults(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int getQueryTimeout() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getQueryTimeout();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public ResultSet getResultSet() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getResultSet();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int getResultSetConcurrency() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getResultSetConcurrency();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int getResultSetHoldability() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getResultSetHoldability();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int getResultSetType() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getResultSetType();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int getUpdateCount() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getUpdateCount();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getWarnings();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setEscapeProcessing(boolean arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setEscapeProcessing(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setFetchDirection(int arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setFetchDirection(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setFetchSize(int arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setFetchSize(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setMaxFieldSize(int arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setMaxFieldSize(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setMaxRows(int arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setMaxRows(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setQueryTimeout(int arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setQueryTimeout(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.isWrapperFor(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.unwrap(arg0);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void addBatch() throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.addBatch();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void clearParameters() throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.clearParameters();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public boolean execute() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.execute();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public ResultSet executeQuery() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.executeQuery();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public int executeUpdate() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.executeUpdate();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getMetaData();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException
	{
		checkNotClosed();
		try
		{
			return preparedStatement.getParameterMetaData();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setBigDecimal(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setBoolean(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setByte(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setBytes(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setDate(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setDate(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setDouble(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setFloat(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setInt(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setLong(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setNString(parameterIndex, value);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setNull(parameterIndex, sqlType);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setNull(parameterIndex, sqlType, typeName);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setObject(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setObject(parameterIndex, x, targetSqlType);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setRowId(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setShort(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setString(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setTime(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setTime(parameterIndex, x, cal);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setTimestamp(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setTimestamp(parameterIndex, x, cal);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException
	{
		checkNotClosed();
		try
		{
			preparedStatement.setURL(parameterIndex, x);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.statementErrorOccurred(preparedStatement, sqlException);
			throw sqlException;
		}
	}
}
