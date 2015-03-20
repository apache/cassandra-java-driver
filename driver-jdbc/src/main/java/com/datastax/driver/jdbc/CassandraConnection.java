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

import java.sql.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;

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



import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import static com.datastax.driver.jdbc.CassandraResultSet.*;
import static com.datastax.driver.jdbc.Utils.*;


/**
 * Implementation class for {@link Connection}.
 */
public class CassandraConnection extends AbstractConnection implements Connection
{

    private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);
    public static Integer roundRobinIndex;
    static final String IS_VALID_CQLQUERY_2_0_0 = "SELECT COUNT(1) FROM system.Versions WHERE component = 'cql';";
    static final String IS_VALID_CQLQUERY_3_0_0 = "SELECT COUNT(1) FROM system.\"Versions\" WHERE component = 'cql';";
    
    public static int DB_MAJOR_VERSION = 1;
    public static int DB_MINOR_VERSION = 2;
    public static int DB_REVISION = 2;
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
    public boolean debugMode;

    public final LoadingCache<String, Session> sessions = CacheBuilder.newBuilder()
            .weakValues() // une entrée sera évincée quand plus personne n'a de référence sur le Cluster
            .removalListener(new RemovalListener<String, Session>() {
                @Override
                public void onRemoval(RemovalNotification<String, Session> notification) {
                	System.out.println("Suppression connexion : " + notification.getKey());
                    Session session = notification.getValue();
                    Cluster c = session.getCluster();                    
                    session.close();
                    c.close();
                    
                }
            })
            .build(new CacheLoader<String, Session>() {
                       @Override
                       public Session load(String url) throws Exception {
                    	   System.out.println("Creation connexion : " + url);
                    	   Properties finalProps = Utils.parseURL(url.replace("\"", "'"));
                           Session session = createSession(finalProps); 
                           return session;
                       }

                   });

    
    PreparedStatement isAlive = null;
    
    //private String currentCqlVersion;
    
    public ConsistencyLevel defaultConsistencyLevel;

    /**
     * Instantiates a new CassandraConnection.
     */
    //public CassandraConnection(Properties props) throws SQLException
    public CassandraConnection(String url) throws SQLException
    {    	
    	 try {    		     		 
			cSession = sessions.get(url);
			cCluster = cSession.getCluster();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			throw new SQLNonTransientConnectionException(e);
		}
    /*	debugMode = false;
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
            String loadBalancingPolicy = props.getProperty(TAG_LOADBALANCING_POLICY,"");
            String retryPolicy = props.getProperty(TAG_RETRY_POLICY,"");
            String reconnectPolicy = props.getProperty(TAG_RECONNECT_POLICY,"");
            debugMode = props.getProperty(TAG_DEBUG,"").equals("true");
                        
            connectionProps.setProperty(TAG_ACTIVE_CQL_VERSION, version);
            majorCqlVersion = getMajor(version);
            defaultConsistencyLevel = ConsistencyLevel.valueOf(props.getProperty(TAG_CONSISTENCY_LEVEL,ConsistencyLevel.ONE.name()));
                        
            
            Builder builder = Cluster.builder();
            builder.addContactPoints(host.split("--")).withPort(port);
            builder.withSocketOptions(new SocketOptions().setKeepAlive(true));
            // Set credentials when applicable
            if(username.length()>0){
            	builder.withCredentials(username, password);
            }
            
                        
            if(loadBalancingPolicy.length()>0){
            	// if load balancing policy has been given in the JDBC URL, parse it and add it to the cluster builder 
            	try{
            		builder.withLoadBalancingPolicy(Utils.parseLbPolicy(loadBalancingPolicy));
            	}catch(Exception e){
            		if(debugMode){
            			throw new Exception(e);
            		}            		
            		logger.warn("Error occured while parsing load balancing policy :" + e.getMessage() + " / Forcing to TokenAwarePolicy...");
            		builder.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            	}
            }
            
            if(retryPolicy.length()>0){
            	// if retry policy has been given in the JDBC URL, parse it and add it to the cluster builder 
            	try{
            		builder.withRetryPolicy(Utils.parseRetryPolicy(retryPolicy));
            	}catch(Exception e){
            		if(debugMode){
            			throw new Exception(e);
            		}
            		logger.warn("Error occured while parsing retry policy :" + e.getMessage() + " / skipping...");
            	}
            }
            
            if(reconnectPolicy.length()>0){
            	// if reconnection policy has been given in the JDBC URL, parse it and add it to the cluster builder 
            	try{
            		builder.withReconnectionPolicy(Utils.parseReconnectionPolicy(reconnectPolicy));
            	}catch(Exception e){
            		if(debugMode){
            			throw new Exception(e);
            		}
            		logger.warn("Error occured while parsing reconnection policy :" + e.getMessage() + " / skipping...");            		
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
			Host[] hosts = cCluster.getMetadata().getAllHosts().toArray(new Host[cCluster.getMetadata().getAllHosts().size()]);
			CassandraConnection.DB_MAJOR_VERSION = hosts[0].getCassandraVersion().getMajor();
			CassandraConnection.DB_MINOR_VERSION = hosts[0].getCassandraVersion().getMinor();
			CassandraConnection.DB_REVISION = hosts[0].getCassandraVersion().getPatch();
			
			
                                                                  
        }        
        catch (Exception e)
        {
        	try{
        		cCluster.close();
        	}catch(Exception e1){
        		
        	}
            throw new SQLNonTransientConnectionException(e);            
        }*/       
    	
        
    }
    
    private Session createSession(Properties props) throws SQLException{    	
    	debugMode = false;
    	hostListPrimary = new TreeSet<String>();
    	hostListBackup = new TreeSet<String>();
        connectionProps = (Properties)props.clone();
        clientInfo = new Properties();
        url = PROTOCOL + createSubName(props);
        Cluster cluster;
        Session session;
        try
        {
        	
            String host = props.getProperty(TAG_SERVER_NAME);           
            int port = Integer.parseInt(props.getProperty(TAG_PORT_NUMBER));            
            currentKeyspace = props.getProperty(TAG_DATABASE_NAME);
            username = props.getProperty(TAG_USER,"");
            String password = props.getProperty(TAG_PASSWORD,"");
            String version = props.getProperty(TAG_CQL_VERSION,DEFAULT_CQL_VERSION);
            String loadBalancingPolicy = props.getProperty(TAG_LOADBALANCING_POLICY,"");
            String retryPolicy = props.getProperty(TAG_RETRY_POLICY,"");
            String reconnectPolicy = props.getProperty(TAG_RECONNECT_POLICY,"");
            debugMode = props.getProperty(TAG_DEBUG,"").equals("true");
                        
            connectionProps.setProperty(TAG_ACTIVE_CQL_VERSION, version);
            majorCqlVersion = getMajor(version);
            defaultConsistencyLevel = ConsistencyLevel.valueOf(props.getProperty(TAG_CONSISTENCY_LEVEL,ConsistencyLevel.ONE.name()));
                        
            
            Builder builder = Cluster.builder();
            builder.addContactPoints(host.split("--")).withPort(port);
            builder.withSocketOptions(new SocketOptions().setKeepAlive(true));
            // Set credentials when applicable
            if(username.length()>0){
            	builder.withCredentials(username, password);
            }
            
                        
            if(loadBalancingPolicy.length()>0){
            	// if load balancing policy has been given in the JDBC URL, parse it and add it to the cluster builder 
            	try{
            		builder.withLoadBalancingPolicy(Utils.parseLbPolicy(loadBalancingPolicy));
            	}catch(Exception e){
            		if(debugMode){
            			throw new Exception(e);
            		}            		
            		logger.warn("Error occured while parsing load balancing policy :" + e.getMessage() + " / Forcing to TokenAwarePolicy...");
            		builder.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            	}
            }
            
            if(retryPolicy.length()>0){
            	// if retry policy has been given in the JDBC URL, parse it and add it to the cluster builder 
            	try{
            		builder.withRetryPolicy(Utils.parseRetryPolicy(retryPolicy));
            	}catch(Exception e){
            		if(debugMode){
            			throw new Exception(e);
            		}
            		logger.warn("Error occured while parsing retry policy :" + e.getMessage() + " / skipping...");
            	}
            }
            
            if(reconnectPolicy.length()>0){
            	// if reconnection policy has been given in the JDBC URL, parse it and add it to the cluster builder 
            	try{
            		builder.withReconnectionPolicy(Utils.parseReconnectionPolicy(reconnectPolicy));
            	}catch(Exception e){
            		if(debugMode){
            			throw new Exception(e);
            		}
            		logger.warn("Error occured while parsing reconnection policy :" + e.getMessage() + " / skipping...");            		
            	}
            }
           
            cluster = builder.build();
	    	
	    	metadata = cluster.getMetadata();
			   System.out.printf("Connected to cluster: %s\n", 
			         metadata.getClusterName());
			   for ( Host aHost : metadata.getAllHosts() ) {
			      System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
			    		 aHost.getDatacenter(), aHost.getAddress(), aHost.getRack());
			   }
			   
			session = cluster.connect(currentKeyspace);
			Host[] hosts = cluster.getMetadata().getAllHosts().toArray(new Host[cluster.getMetadata().getAllHosts().size()]);
			CassandraConnection.DB_MAJOR_VERSION = hosts[0].getCassandraVersion().getMajor();
			CassandraConnection.DB_MINOR_VERSION = hosts[0].getCassandraVersion().getMinor();
			CassandraConnection.DB_REVISION = hosts[0].getCassandraVersion().getPatch();
			
			return session;
                                                                  
        }        
        catch (Exception e)
        {        	
            throw new SQLNonTransientConnectionException(e);            
        }
    }
    
    // get the Major portion of a string like : Major.minor.patch where 2 is the default
    @SuppressWarnings("boxing")
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
    	System.out.println("Closing Cassandra connection...");
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
        //throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
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
        //if (!autoCommit) throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
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
        cSession = null;
        cCluster = null;
        System.out.println("Cassandra connection closed");
    }

    /**
     * Connection state.
     */
    protected boolean isConnected()
    {
    	try{
    		return !cSession.isClosed() && !cCluster.isClosed();
    	}catch(Exception e){
    		return false;
    	}
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
    
    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
    	HashMap<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
    	System.out.println("current KS : " + currentKeyspace);
    	Collection<UserType> types = this.cCluster.getMetadata().getKeyspace(currentKeyspace).getUserTypes();
    	for(UserType type:types){    		    		
    		typeMap.put(type.getTypeName(), type.getClass());
    	}

    	return typeMap;
    }
    

}
