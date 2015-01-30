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

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;








import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.Session;



import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import static org.apache.cassandra.cql.jdbc.Utils.*;
import static org.apache.cassandra.cql.jdbc.CassandraResultSet.*;


/**
 * Implementation class for {@link Connection}.
 */
class CassandraConnection extends AbstractConnection implements Connection
{

    private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);
    public static Integer roundRobinIndex;
    static final String IS_VALID_CQLQUERY_2_0_0 = "SELECT COUNT(1) FROM system.Versions WHERE component = 'cql';";
    static final String IS_VALID_CQLQUERY_3_0_0 = "SELECT COUNT(1) FROM system.\"Versions\" WHERE component = 'cql';";
    
    public static final int DB_MAJOR_VERSION = 1;
    public static final int DB_MINOR_VERSION = 2;
    public static final String DB_PRODUCT_NAME = "Cassandra";
    public static final String DEFAULT_CQL_VERSION = "3.0.0";

    public static Compression defaultCompression = Compression.LZ4;

    private final boolean autoCommit = true;

    private final int transactionIsolation = Connection.TRANSACTION_NONE;

    /**
     * Connection Properties
     */
    private Properties connectionProps;

    /**
     * Client Info Properties (currently unused)
     */
    private Properties clientInfo = new Properties();

    /**
     * Set of all Statements that have been created by this connection
     */
    private Set<Statement> statements = new ConcurrentSkipListSet<Statement>();

    
    private Cluster cCluster;
    private Session cSession;

    protected long timeOfLastFailure = 0;
    protected int numFailures = 0;
    protected String username = null;
    protected String url = null;
    protected String cluster;//current catalog
    protected String currentKeyspace;//current schema
    protected TreeSet<String> hostListPrimary;
    protected TreeSet<String> hostListBackup;
    int majorCqlVersion;
    private Metadata metadata;


    
    PreparedStatement isAlive = null;
    
    private String currentCqlVersion;
    
    ConsistencyLevel defaultConsistencyLevel;

    /**
     * Instantiates a new CassandraConnection.
     */
    public CassandraConnection(Properties props) throws SQLException
    {    	
    	hostListPrimary = new TreeSet<String>();
    	hostListBackup = new TreeSet<String>();
        connectionProps = (Properties)props.clone();
        clientInfo = new Properties();
        url = PROTOCOL + createSubName(props);
        try
        {
        	
            String host = props.getProperty(TAG_SERVER_NAME);           
            int port = Integer.parseInt(props.getProperty(TAG_PORT_NUMBER));            
            currentKeyspace = props.getProperty(TAG_DATABASE_NAME);
            username = props.getProperty(TAG_USER,"");
            String password = props.getProperty(TAG_PASSWORD,"");
            String version = props.getProperty(TAG_CQL_VERSION,DEFAULT_CQL_VERSION);
            String primaryDc = props.getProperty(TAG_PRIMARY_DC,"");
            String backupDc = props.getProperty(TAG_BACKUP_DC,"");
            String loadBalancingPolicy = props.getProperty(TAG_LOADBALANCING_POLICY,"");
            
            connectionProps.setProperty(TAG_ACTIVE_CQL_VERSION, version);
            majorCqlVersion = getMajor(version);
            defaultConsistencyLevel = ConsistencyLevel.valueOf(props.getProperty(TAG_CONSISTENCY_LEVEL,ConsistencyLevel.ONE.name()));
            boolean connected=false;            
            int retries=0;            
            
            Builder builder = Cluster.builder();
            builder.addContactPoints(host.split("--")).withPort(port);
            
            // Set credentials when applicable
            if(username.length()>0){
            	builder.withCredentials(username, password);
            }
                        
            
            // Set load balancing policy as requested in the url. Policies can be nested using dashes as separator 
            // for example : TokenAwarePolicy-DCAwareRoundRobinPolicy gives builder.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()));           
            if(loadBalancingPolicy.length()>0){
            	if(loadBalancingPolicy.startsWith("TokenAwarePolicy")){
            		if(loadBalancingPolicy.endsWith("DCAwareRoundRobinPolicy")){
            			if(primaryDc.length()>0){
            				builder.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(primaryDc)));
            			}else{
            				builder.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()));
            			}
            		}else if(loadBalancingPolicy.endsWith("RoundRobinPolicy")){
            			builder.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            		}else if(loadBalancingPolicy.endsWith("LatencyAwarePolicy")){
            			// TODO: add all necessary parameters to use LatencyAwarePolicy
            			builder.withLoadBalancingPolicy(new TokenAwarePolicy(Policies.defaultLoadBalancingPolicy()));
            		}else{
            			builder.withLoadBalancingPolicy(new TokenAwarePolicy(Policies.defaultLoadBalancingPolicy()));
            		}
            	}else if(loadBalancingPolicy.startsWith("DCAwareRoundRobinPolicy")){            		
            			if(primaryDc.length()>0){
            				builder.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy(primaryDc));
            			}else{
            				builder.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy());
            			}            		
            	}else if(loadBalancingPolicy.startsWith("RoundRobinPolicy")){            		            			
            				builder.withLoadBalancingPolicy(new RoundRobinPolicy());
            	}else if(loadBalancingPolicy.startsWith("LatencyAwarePolicy")){
            		// TODO: add all necessary parameters to use LatencyAwarePolicy
            		//builder.withLoadBalancingPolicy(new LatencyAwarePolicy(Policies.defaultLoadBalancingPolicy(), timeOfLastFailure, timeOfLastFailure, timeOfLastFailure, timeOfLastFailure, retries));
        			builder.withLoadBalancingPolicy(Policies.defaultLoadBalancingPolicy());
            	}else{
            		builder.withLoadBalancingPolicy(Policies.defaultLoadBalancingPolicy());
            	}
            }
            
            cCluster = builder.build();
	    	
	    	metadata = cCluster.getMetadata();
			   System.out.printf("Connected to cluster: %s\n", 
			         metadata.getClusterName());
			   for ( Host aHost : metadata.getAllHosts() ) {
			      System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
			    		 aHost.getDatacenter(), aHost.getAddress(), aHost.getRack());
			   }
			   
			cSession = cCluster.connect(currentKeyspace);
			
                                                                  
        }        
        catch (Exception e)
        {
            throw new SQLNonTransientConnectionException(e);
        }       
    }
    
    // get the Major portion of a string like : Major.minor.patch where 2 is the default
    private final int getMajor(String version)
    {
        int major = 0;
        String[] parts = version.split("\\.");
        try
        {
            major = Integer.valueOf(parts[0]);
        }
        catch (Exception e)
        {
            major = 2;
        }
        return major;
    }
    
    private final void checkNotClosed() throws SQLException
    {
        if (isClosed()) throw new SQLNonTransientConnectionException(WAS_CLOSED_CON);
    }

    public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    /**
     * On close of connection.
     */
    public synchronized void close() throws SQLException
    {
        // close all statements associated with this connection upon close
        for (Statement statement : statements)
            statement.close();
        statements.clear();
        
        if (isConnected())
        {
            // then disconnect from the transport                
            disconnect();
        }
    }

    public void commit() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public java.sql.Statement createStatement() throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this);
        statements.add(statement);
        return statement;
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency);
        statements.add(statement);
        return statement;
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency, resultSetHoldability);
        statements.add(statement);
        return statement;
    }

    public boolean getAutoCommit() throws SQLException
    {
        checkNotClosed();
        return autoCommit;
    }

    public Properties getConnectionProps()
    {
        return connectionProps;
    }

    public String getCatalog() throws SQLException
    {
        checkNotClosed();
        return metadata.getClusterName();
    }

    public void setSchema(String schema) throws SQLException
    {
        checkNotClosed();
        currentKeyspace = schema;
    }

    public String getSchema() throws SQLException
    {
        checkNotClosed();
        return currentKeyspace;
    }

    public Properties getClientInfo() throws SQLException
    {
        checkNotClosed();
        return clientInfo;
    }

    public String getClientInfo(String label) throws SQLException
    {
        checkNotClosed();
        return clientInfo.getProperty(label);
    }

    public int getHoldability() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are really no commits in Cassandra so no boundary...
        return DEFAULT_HOLDABILITY;
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        checkNotClosed();
        return new CassandraDatabaseMetaData(this);
    }

    public int getTransactionIsolation() throws SQLException
    {
        checkNotClosed();
        return transactionIsolation;
    }

    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }

    public synchronized boolean isClosed() throws SQLException
    {

        return !isConnected();
    }

    public boolean isReadOnly() throws SQLException
    {
        checkNotClosed();
        return false;
    }

    public boolean isValid(int timeout) throws SQLTimeoutException
    {
        if (timeout < 0) throw new SQLTimeoutException(BAD_TIMEOUT);

        // set timeout
/*
        try
        {
        	if (isClosed()) {
        		return false;
        	}
        	
            if (isAlive == null)
            {
                isAlive = prepareStatement(currentCqlVersion == "2.0.0" ? IS_VALID_CQLQUERY_2_0_0 : IS_VALID_CQLQUERY_3_0_0);
            }
            // the result is not important
            isAlive.executeQuery().close();
        }
        catch (SQLException e)
        {
        	return false;
        }
        finally {
            // reset timeout
            socket.setTimeout(0);
        }
*/
        return true;
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException
    {
        return false;
    }

    public String nativeSQL(String sql) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no distinction between grammars in this implementation...
        // so we are just return the input argument
        return sql;
    }

    public CassandraPreparedStatement prepareStatement(String cql) throws SQLException
    {
        return prepareStatement(cql,DEFAULT_TYPE,DEFAULT_CONCURRENCY,DEFAULT_HOLDABILITY);
    }

    public CassandraPreparedStatement prepareStatement(String cql, int rsType) throws SQLException
    {
        return prepareStatement(cql,rsType,DEFAULT_CONCURRENCY,DEFAULT_HOLDABILITY);
    }

    public CassandraPreparedStatement prepareStatement(String cql, int rsType, int rsConcurrency) throws SQLException
    {
        return prepareStatement(cql,rsType,rsConcurrency,DEFAULT_HOLDABILITY);
    }

    public CassandraPreparedStatement prepareStatement(String cql, int rsType, int rsConcurrency, int rsHoldability) throws SQLException
    {
        checkNotClosed();
        CassandraPreparedStatement statement = new CassandraPreparedStatement(this, cql, rsType,rsConcurrency,rsHoldability);
        statements.add(statement);
        return statement;
    }

    public void rollback() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        checkNotClosed();
        if (!autoCommit) throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public void setCatalog(String arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no catalog name to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setClientInfo(Properties props) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        if (props != null) clientInfo = props;
    }

    public void setClientInfo(String key, String value) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        clientInfo.setProperty(key, value);
    }

    public void setHoldability(int arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no holdability to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setReadOnly(boolean arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is all connections are read/write in the Cassandra implementation...
        // so we are "silently ignoring" the request
    }

    public void setTransactionIsolation(int level) throws SQLException
    {
        checkNotClosed();
        if (level != Connection.TRANSACTION_NONE) throw new SQLFeatureNotSupportedException(NO_TRANSACTIONS);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    /**
     * Remove a Statement from the Open Statements List
     */
    protected boolean removeStatement(Statement statement)
    {
        return statements.remove(statement);
    }
    
    /**
     * Shutdown the remote connection
     */
    protected void disconnect()
    {
        cSession.close();
        cCluster.close();
    }

    /**
     * Connection state.
     */
    protected boolean isConnected()
    {
        return !cSession.isClosed();
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("CassandraConnection [connectionProps=");
        builder.append(connectionProps);
        builder.append("]");
        return builder.toString();
    }
  
    
    public Session getSession(){
    	return this.cSession;
    }
    
    public Metadata getClusterMetadata(){
    	return this.cCluster.getMetadata();
    }
    

}
