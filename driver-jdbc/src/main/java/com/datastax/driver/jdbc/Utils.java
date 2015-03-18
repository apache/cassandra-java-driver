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

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;

import org.mockito.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.policies.LatencyAwarePolicy.Builder;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A set of static utility methods used by the JDBC Suite, and various default values and error message strings
 * that can be shared across classes.
 */
public class Utils
{
    private static final Pattern KEYSPACE_PATTERN = Pattern.compile("USE (\\w+);?", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern SELECT_PATTERN = Pattern.compile("(?:SELECT|DELETE)\\s+.+\\s+FROM\\s+(\\w+).*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern UPDATE_PATTERN = Pattern.compile("UPDATE\\s+(\\w+)\\s+.*", Pattern.CASE_INSENSITIVE);

    public static final String PROTOCOL = "jdbc:cassandra:";
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9042;
    public static final com.datastax.driver.core.ConsistencyLevel DEFAULT_CONSISTENCY = com.datastax.driver.core.ConsistencyLevel.ONE;
    

    public static final String KEY_VERSION = "version";
    public static final String KEY_CONSISTENCY = "consistency";
    public static final String KEY_PRIMARY_DC = "primarydc";
    public static final String KEY_BACKUP_DC = "backupdc";
    public static final String KEY_CONNECTION_RETRIES = "retries";
    public static final String KEY_LOADBALANCING_POLICY = "loadbalancing";
    public static final String KEY_RETRY_POLICY = "retry";
    public static final String KEY_RECONNECT_POLICY = "reconnection";
    public static final String KEY_DEBUG = "debug";
    //public static final String KEY_PRIMARY_DC = "primarydc";
    
    public static final String TAG_DESCRIPTION = "description";
    public static final String TAG_USER = "user";
    public static final String TAG_PASSWORD = "password";
    public static final String TAG_DATABASE_NAME = "databaseName";
    public static final String TAG_SERVER_NAME = "serverName";
    public static final String TAG_PORT_NUMBER = "portNumber";
    public static final String TAG_ACTIVE_CQL_VERSION = "activeCqlVersion";
    public static final String TAG_CQL_VERSION = "cqlVersion";
    public static final String TAG_BUILD_VERSION = "buildVersion";
    public static final String TAG_THRIFT_VERSION = "thriftVersion";
    public static final String TAG_CONSISTENCY_LEVEL = "consistencyLevel";
    public static final String TAG_LOADBALANCING_POLICY = "loadBalancing";
    public static final String TAG_RETRY_POLICY = "retry";
    public static final String TAG_RECONNECT_POLICY = "reconnection";
    public static final String TAG_DEBUG = "debug";
    
    public static final String TAG_PRIMARY_DC = "primaryDatacenter";
    public static final String TAG_BACKUP_DC = "backupDatacenter";
    public static final String TAG_CONNECTION_RETRIES = "retries";

    protected static final String WAS_CLOSED_CON = "method was called on a closed Connection";
    protected static final String WAS_CLOSED_STMT = "method was called on a closed Statement";
    protected static final String WAS_CLOSED_RSLT = "method was called on a closed ResultSet";
    protected static final String NO_INTERFACE = "no object was found that matched the provided interface: %s";
    protected static final String NO_TRANSACTIONS = "the Cassandra implementation does not support transactions";
    protected static final String NO_SERVER = "no Cassandra server is available";
    protected static final String ALWAYS_AUTOCOMMIT = "the Cassandra implementation is always in auto-commit mode";
    protected static final String BAD_TIMEOUT = "the timeout value was less than zero";
    protected static final String SCHEMA_MISMATCH = "schema does not match across nodes, (try again later)";
    public static final String NOT_SUPPORTED = "the Cassandra implementation does not support this method";
    protected static final String NO_GEN_KEYS = "the Cassandra implementation does not currently support returning generated  keys";
    protected static final String NO_BATCH = "the Cassandra implementation does not currently support this batch in Statement";
    protected static final String NO_MULTIPLE = "the Cassandra implementation does not currently support multiple open Result Sets";
    protected static final String NO_VALIDATOR = "Could not find key validator for: %s.%s";
    protected static final String NO_COMPARATOR = "Could not find key comparator for: %s.%s";
    protected static final String NO_RESULTSET = "No ResultSet returned from the CQL statement passed in an 'executeQuery()' method";
    protected static final String NO_UPDATE_COUNT = "No Update Count was returned from the CQL statement passed in an 'executeUpdate()' method";
    protected static final String NO_CF = "no column family reference could be extracted from the provided CQL statement";
    protected static final String BAD_KEEP_RSET = "the argument for keeping the current result set : %s is not a valid value";
    protected static final String BAD_TYPE_RSET = "the argument for result set type : %s is not a valid value";
    protected static final String BAD_CONCUR_RSET = "the argument for result set concurrency : %s is not a valid value";
    protected static final String BAD_HOLD_RSET = "the argument for result set holdability : %s is not a valid value";
    protected static final String BAD_FETCH_DIR = "fetch direction value of : %s is illegal";
    protected static final String BAD_AUTO_GEN = "auto key generation value of : %s is illegal";
    protected static final String BAD_FETCH_SIZE = "fetch size of : %s rows may not be negative";
    protected static final String MUST_BE_POSITIVE = "index must be a positive number less or equal the count of returned columns: %s";
    protected static final String VALID_LABELS = "name provided was not in the list of valid column labels: %s";
    protected static final String NOT_TRANSLATABLE = "column was stored in %s format which is not translatable to %s";
    protected static final String NOT_BOOLEAN = "string value was neither 'true' nor 'false' :  %s";
    protected static final String HOST_IN_URL = "Connection url must specify a host, e.g., jdbc:cassandra://localhost:9042/Keyspace1";
    protected static final String HOST_REQUIRED = "a 'host' name is required to build a Connection";
    protected static final String BAD_KEYSPACE = "Keyspace names must be composed of alphanumerics and underscores (parsed: '%s')";
    protected static final String URI_IS_SIMPLE = "Connection url may only include host, port, and keyspace, consistency and version option, e.g., jdbc:cassandra://localhost:9042/Keyspace1?version=3.0.0&consistency=ONE";
    protected static final String NOT_OPTION = "Connection url only supports the 'version' and 'consistency' options";
    protected static final String FORWARD_ONLY = "Can not position cursor with a type of TYPE_FORWARD_ONLY";

    protected static final Logger logger = LoggerFactory.getLogger(Utils.class);

    /**
     * Use the Compression object method to deflate the query string
     *
     * @param queryStr An un-compressed CQL query string
     * @param compression The compression object
     * @return A compressed string
    
    
    public static ByteBuffer compressQuery(String queryStr, Compression compression)
    {
        byte[] data = queryStr.getBytes(Charsets.UTF_8);
        Deflater compressor = new Deflater();
        compressor.setInput(data);
        compressor.finish();

        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];

        try
        {
            while (!compressor.finished())
            {
                int size = compressor.deflate(buffer);
                byteArray.write(buffer, 0, size);
            }
        }
        finally
        {
            compressor.end(); //clean up after the Deflater
        }

        logger.trace("Compressed query statement {} bytes in length to {} bytes", data.length, byteArray.size());

        return ByteBuffer.wrap(byteArray.toByteArray());
    }
 */
    /**
     * Parse a URL for the Cassandra JDBC Driver
     * <p/>
     * The URL must start with the Protocol: "jdbc:cassandra:"
     * The URI part(the "Subname") must contain a host and an optional port and optional keyspace name
     * ie. "//localhost:9160/Test1"
     *
     * @param url The full JDBC URL to be parsed
     * @return A list of properties that were parsed from the Subname
     * @throws SQLException
     */
    public static final Properties parseURL(String url) throws SQLException
    {
        Properties props = new Properties();

        if (!(url == null))
        {
            props.setProperty(TAG_PORT_NUMBER, "" + DEFAULT_PORT);
            String rawUri = url.substring(PROTOCOL.length());
            URI uri = null;
            try
            {
                uri = new URI(rawUri);
            }
            catch (URISyntaxException e)
            {
                throw new SQLSyntaxErrorException(e);
            }

            String host = uri.getHost();
            if (host == null) throw new SQLNonTransientConnectionException(HOST_IN_URL);
            props.setProperty(TAG_SERVER_NAME, host);

            int port = uri.getPort() >= 0 ? uri.getPort() : DEFAULT_PORT;
            props.setProperty(TAG_PORT_NUMBER, "" + port);

            String keyspace = uri.getPath();
            if ((keyspace != null) && (!keyspace.isEmpty()))
            {
                if (keyspace.startsWith("/")) keyspace = keyspace.substring(1);
                if (!keyspace.matches("[a-zA-Z]\\w+"))
                    throw new SQLNonTransientConnectionException(String.format(BAD_KEYSPACE, keyspace));
                props.setProperty(TAG_DATABASE_NAME, keyspace);
            }

            if (uri.getUserInfo() != null)
                throw new SQLNonTransientConnectionException(URI_IS_SIMPLE);
            
            String query = uri.getQuery();
            if ((query != null) && (!query.isEmpty()))
            {
                Map<String,String> params = parseQueryPart(query);
                if (params.containsKey(KEY_VERSION) )
                {
                    props.setProperty(TAG_CQL_VERSION,params.get(KEY_VERSION));
                }                
                if (params.containsKey(KEY_DEBUG) )
                {
                    props.setProperty(TAG_DEBUG,params.get(KEY_DEBUG));
                }
                if (params.containsKey(KEY_CONSISTENCY) )
                {
                    props.setProperty(TAG_CONSISTENCY_LEVEL,params.get(KEY_CONSISTENCY));
                }
                if (params.containsKey(KEY_PRIMARY_DC) )
                {
                    props.setProperty(TAG_PRIMARY_DC,params.get(KEY_PRIMARY_DC));
                }
                if (params.containsKey(KEY_BACKUP_DC) )
                {
                    props.setProperty(TAG_BACKUP_DC,params.get(KEY_BACKUP_DC));
                }
                if (params.containsKey(KEY_CONNECTION_RETRIES) )
                {
                    props.setProperty(TAG_CONNECTION_RETRIES,params.get(KEY_CONNECTION_RETRIES));
                }
                if (params.containsKey(KEY_LOADBALANCING_POLICY)){
                	props.setProperty(TAG_LOADBALANCING_POLICY, params.get(KEY_LOADBALANCING_POLICY));
                }
                if (params.containsKey(KEY_RETRY_POLICY)){
                	props.setProperty(TAG_RETRY_POLICY, params.get(KEY_RETRY_POLICY));
                }
                if (params.containsKey(KEY_RECONNECT_POLICY)){
                	props.setProperty(TAG_RECONNECT_POLICY, params.get(KEY_RECONNECT_POLICY));
                }
                

            }
        }

        if (logger.isTraceEnabled()) logger.trace("URL : '{}' parses to: {}", url, props);

        return props;
    }

    /**
     * Create a "Subname" portion of a JDBC URL from properties.
     * 
     * @param props A Properties file containing all the properties to be considered.
     * @return A constructed "Subname" portion of a JDBC URL in the form of a CLI (ie: //myhost:9160/Test1?version=3.0.0 )
     * @throws SQLException
     */
    public static final String createSubName(Properties props)throws SQLException
    {
        // make keyspace always start with a "/" for URI
        String keyspace = props.getProperty(TAG_DATABASE_NAME);
     
        // if keyspace is null then do not bother ...
        if (keyspace != null) 
            if (!keyspace.startsWith("/")) keyspace = "/"  + keyspace;
        
        String host = props.getProperty(TAG_SERVER_NAME);
        if (host==null)throw new SQLNonTransientConnectionException(HOST_REQUIRED);
                
        // construct a valid URI from parts... 
        URI uri;
        try
        {
            uri = new URI(
                null,
                null,
                host,
                props.getProperty(TAG_PORT_NUMBER)==null ? DEFAULT_PORT : Integer.parseInt(props.getProperty(TAG_PORT_NUMBER)),
                keyspace,
                makeQueryString(props),
                null);
        }
        catch (Exception e)
        {
            throw new SQLNonTransientConnectionException(e);
        }
        
        if (logger.isTraceEnabled()) logger.trace("Subname : '{}' created from : {}",uri.toString(), props);
        
        return uri.toString();
    }
    
    /**
     * Determine the current keyspace by inspecting the CQL string to see if a USE statement is provided; which would change the keyspace.
     *
     * @param cql     A CQL query string
     * @param current The current keyspace stored as state in the connection
     * @return the provided keyspace name or the keyspace from the contents of the CQL string
     */
    public static String determineCurrentKeyspace(String cql, String current)
    {
        String ks = current;
        Matcher isKeyspace = KEYSPACE_PATTERN.matcher(cql);
        if (isKeyspace.matches()) ks = isKeyspace.group(1);
        return ks;
    }

    /**
     * Determine the current column family by inspecting the CQL to find a CF reference.
     *
     * @param cql A CQL query string
     * @return The column family name from the contents of the CQL string or null in none was found
     */
    public static String determineCurrentColumnFamily(String cql)
    {
        String cf = null;
        Matcher isSelect = SELECT_PATTERN.matcher(cql);
        if (isSelect.matches()) cf = isSelect.group(1);
        Matcher isUpdate = UPDATE_PATTERN.matcher(cql);
        if (isUpdate.matches()) cf = isUpdate.group(1);
        return cf;
    }
    
    // Utility method
    /**
     * Utility method to pack bytes into a byte buffer from a list of ByteBuffers 
     * 
     * @param buffers A list of ByteBuffers representing the elements to pack
     * @param elements The count of the elements
     * @param size The size in bytes of the result buffer
     * @return The packed ByteBuffer
     */
    protected static ByteBuffer pack(List<ByteBuffer> buffers, int elements, int size)
    {
        ByteBuffer result = ByteBuffer.allocate(2 + size);
        result.putShort((short)elements);
        for (ByteBuffer bb : buffers)
        {
            result.putShort((short)bb.remaining());
            result.put(bb.duplicate());
        }
        return (ByteBuffer)result.flip();
    }
    
    protected static String makeQueryString(Properties props)
    {
        StringBuilder sb = new StringBuilder();
        String version = (props.getProperty(TAG_CQL_VERSION));
        String consistency = (props.getProperty(TAG_CONSISTENCY_LEVEL));
        if (consistency!=null) sb.append(KEY_CONSISTENCY).append("=").append(consistency);
        if (version!=null)
        {
            if (sb.length() != 0) sb.append("&");
            sb.append(KEY_VERSION).append("=").append(version);
        }
        
        return (sb.length()==0) ? null : sb.toString().trim();
    }
    
    protected static Map<String,String> parseQueryPart(String query) throws SQLException
    {
        Map<String,String> params = new HashMap<String,String>();
        for (String param : query.split("&"))
        {
            try
            {
                String pair[] = param.split("=");
                String key = URLDecoder.decode(pair[0], "UTF-8").toLowerCase();
                String value = "";
                if (pair.length > 1) value = URLDecoder.decode(pair[1], "UTF-8"); 
                params.put(key, value);
            }
            catch (UnsupportedEncodingException e)
            {
                throw new SQLSyntaxErrorException(e);
            }
        }
        return params;
    }
    
    public static LinkedHashSet<?> parseSet(String itemType, String value){
    	
    	if(itemType.equals("varchar") || itemType.equals("text") || itemType.equals("ascii")){
    		LinkedHashSet<String> zeSet = new LinkedHashSet<String>();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			for(String val:values){
				zeSet.add(val);				
			}
			return zeSet;
			
		}else if(itemType.equals("bigint")){
			LinkedHashSet<Long> zeSet = Sets.newLinkedHashSet();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
					zeSet.add(Long.parseLong(val.trim()));        				
			}        		
			return zeSet;
		}else if(itemType.equals("varint")){
			LinkedHashSet<BigInteger> zeSet = Sets.newLinkedHashSet();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
					zeSet.add(BigInteger.valueOf(Long.parseLong(val.trim())));        				
			}        		
			return zeSet;
		}else if(itemType.equals("decimal")){
			LinkedHashSet<BigDecimal> zeSet = Sets.newLinkedHashSet();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
					zeSet.add(BigDecimal.valueOf(Double.parseDouble(val.trim())));        				
			}        		
			return zeSet;
		}else if(itemType.equals("double")){
			LinkedHashSet<Double> zeSet = Sets.newLinkedHashSet();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
					zeSet.add(Double.parseDouble(val.trim()));        				
			}        		
			return zeSet;
		}else if(itemType.equals("float")){
			LinkedHashSet<Float> zeSet = Sets.newLinkedHashSet();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
					zeSet.add(Float.parseFloat(val.trim()));        				
			}        		
			return zeSet;
		}else if(itemType.equals("boolean")){
			LinkedHashSet<Boolean> zeSet = Sets.newLinkedHashSet();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
					zeSet.add(Boolean.parseBoolean(val.trim()));        				
			}        		
			return zeSet;
		}else if(itemType.equals("int")){
			LinkedHashSet<Integer> zeSet = Sets.newLinkedHashSet();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
					zeSet.add(Integer.parseInt(val.trim()));        				
			}        		
			return zeSet;
		}else if(itemType.equals("uuid")){
			LinkedHashSet<UUID> zeSet = Sets.newLinkedHashSet();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
					zeSet.add(UUID.fromString(val.trim()));        				
			}        		
			return zeSet;
		}else if(itemType.equals("timeuuid")){
			LinkedHashSet<UUID> zeSet = Sets.newLinkedHashSet();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
					zeSet.add(UUID.fromString(val.trim()));        				
			}        		
			return zeSet;
		}
    	return null;
    	
    }
    
    public static ArrayList<?> parseList(String itemType, String value){
    	
    	if(itemType.equals("varchar") || itemType.equals("text") || itemType.equals("ascii")){
    		ArrayList<String> zeList = Lists.newArrayList();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			int i=0;
			for(String val:values){
				if(i>0 && val.startsWith(" ")){
					zeList.add(val.substring(1));
				}else{
					zeList.add(val);
				}
				i++;
			}
			return zeList;
			
		}else if(itemType.equals("bigint")){
			ArrayList<Long> zeList = Lists.newArrayList();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
				zeList.add(Long.parseLong(val.trim()));        				
			}        		
			return zeList;
		}else if(itemType.equals("varint")){
			ArrayList<BigInteger> zeList = Lists.newArrayList();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
				zeList.add(BigInteger.valueOf(Long.parseLong(val.trim())));        				
			}        		
			return zeList;
		}else if(itemType.equals("decimal")){
			ArrayList<BigDecimal> zeList = Lists.newArrayList();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
				zeList.add(BigDecimal.valueOf(Double.parseDouble(val.trim())));        				
			}        		
			return zeList;
		}else if(itemType.equals("double")){
			ArrayList<Double> zeList = Lists.newArrayList();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
				zeList.add(Double.parseDouble(val.trim()));        				
			}        		
			return zeList;
		}else if(itemType.equals("float")){
			ArrayList<Float> zeList = Lists.newArrayList();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
				zeList.add(Float.parseFloat(val.trim()));        				
			}        		
			return zeList;
		}else if(itemType.equals("boolean")){
			ArrayList<Boolean> zeList = Lists.newArrayList();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
				zeList.add(Boolean.parseBoolean(val.trim()));        				
			}        		
			return zeList;
		}else if(itemType.equals("int")){
			ArrayList<Integer> zeList = Lists.newArrayList();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
				zeList.add(Integer.parseInt(val.trim()));        				
			}        		
			return zeList;
		}else if(itemType.equals("uuid")){
			ArrayList<UUID> zeList = Lists.newArrayList();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
				zeList.add(UUID.fromString(val.trim()));        				
			}        		
			return zeList;
		}else if(itemType.equals("timeuuid")){
			ArrayList<UUID> zeList = Lists.newArrayList();
			String[] values = value.replace("[", "").replace("]", "").split(", ");
			
			for(String val:values){        				
				zeList.add(UUID.fromString(val.trim()));        				
			}        		
			return zeList;
		}
    	return null;
    	
    }
    
    @SuppressWarnings({ "boxing", "unchecked", "rawtypes" })
	public static HashMap<?,?> parseMap(String kType, String vType, String value){
    	//Class<?> kTypeClass = TypesMap.getTypeForComparator(kType).getClass(); 
    	
    	//Type t = Type.getObjectType(kType);
    	
    	Map zeMap = new HashMap();
    	String[] values = value.replace("{", "").replace("}", "").split(", ");
    	List keys = Lists.newArrayList();
    	List vals = Lists.newArrayList();
    	
    	for(String val:values){
    		String[] keyVal = val.split("=");    		
    		if(kType.equals("bigint")){
    			keys.add(Long.parseLong(keyVal[0]));
    		}else if(kType.equals("varint")){    			
    			keys.add(BigInteger.valueOf(Long.parseLong(keyVal[0])));
			}else if(kType.equals("decimal")){
				keys.add(BigDecimal.valueOf(Double.parseDouble(keyVal[0])));			
			}else if(kType.equals("double")){				
				keys.add(Double.parseDouble(keyVal[0]));
			}else if(kType.equals("float")){
				keys.add(Float.parseFloat(keyVal[0]));			
			}else if(kType.equals("boolean")){
				keys.add(Boolean.parseBoolean(keyVal[0]));			
			}else if(kType.equals("int")){
				keys.add(Integer.parseInt(keyVal[0]));				
			}else if(kType.equals("uuid")){
				keys.add(UUID.fromString(keyVal[0]));				
			}else if(kType.equals("timeuuid")){
				keys.add(UUID.fromString(keyVal[0]));
			}else{
				keys.add(keyVal[0]);
			}
    		
    		if(vType.equals("bigint")){
    			vals.add(Long.parseLong(keyVal[1]));
    		}else if(vType.equals("varint")){    			
    			vals.add(BigInteger.valueOf(Long.parseLong(keyVal[1])));
			}else if(vType.equals("decimal")){
				vals.add(BigDecimal.valueOf(Double.parseDouble(keyVal[1])));			
			}else if(vType.equals("double")){				
				vals.add(Double.parseDouble(keyVal[1]));
			}else if(vType.equals("float")){
				vals.add(Float.parseFloat(keyVal[1]));			
			}else if(vType.equals("boolean")){
				vals.add(Boolean.parseBoolean(keyVal[1]));			
			}else if(vType.equals("int")){
				vals.add(Integer.parseInt(keyVal[1]));				
			}else if(vType.equals("uuid")){
				vals.add(UUID.fromString(keyVal[1]));				
			}else if(vType.equals("timeuuid")){
				vals.add(UUID.fromString(keyVal[1]));
			}else{
				vals.add(keyVal[1]);
			}
    		
    		zeMap.put(keys.get(keys.size()-1), vals.get(vals.size()-1));
    		
    	}
    	
    	
    	return (HashMap<?, ?>) zeMap;
    	
    }
    
    public static LoadBalancingPolicy parseLbPolicy(String loadBalancingPolicyString) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException{
    	String lb_regex = "([a-zA-Z]*Policy)(\\()(.*)(\\))";
    	Pattern lb_pattern = Pattern.compile(lb_regex);
    	Matcher lb_matcher = lb_pattern.matcher(loadBalancingPolicyString);
    	
    	if(lb_matcher.matches()){
	    	if(lb_matcher.groupCount()>0){
	    		// Primary LB policy has been specified
	    		String primaryLoadBalancingPolicy = lb_matcher.group(1);
	    		String loadBalancingPolicyParams = lb_matcher.group(3);
	    		return getLbPolicy(primaryLoadBalancingPolicy, loadBalancingPolicyParams);
	    	}
    	}
    	
		return null;
    }
    
    public static LoadBalancingPolicy getLbPolicy(String lbString, String parameters) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
    	LoadBalancingPolicy policy = null;
    	//LoadBalancingPolicy childPolicy = null;
    	if(!lbString.contains(".")){
			lbString="com.datastax.driver.core.policies."+ lbString;
		}
    	
    	if(parameters.length()>0){
    		// Child policy or parameters have been specified
    		System.out.println("parameters = " + parameters);
    		String paramsRegex = "([^,]+\\(.+?\\))|([^,]+)";    
    		//String lb_regex = "([a-zA-Z]*Policy)(\\()(.*)(\\))";
    		Pattern param_pattern = Pattern.compile(paramsRegex);
        	Matcher lb_matcher = param_pattern.matcher(parameters);
        	        	
    		ArrayList<Object> paramList = Lists.newArrayList();
    		ArrayList<Class> primaryParametersClasses = Lists.newArrayList();
        	int nb=0;
        	while(lb_matcher.find()){
	        	if(lb_matcher.groupCount()>0){	        		
	        			try{
		        			if(lb_matcher.group().contains("(") && !lb_matcher.group().trim().startsWith("(")){
		        				// We are dealing with child policies here
		        				primaryParametersClasses.add(LoadBalancingPolicy.class);
		        				// Parse and add child policy to the parameter list
		        				paramList.add(parseLbPolicy(lb_matcher.group()));
		        				nb++;
		        			}else{
		        				// We are dealing with parameters that are not policies here
		        				String param = lb_matcher.group();
		        				if(param.contains("'")){
		    	    				primaryParametersClasses.add(String.class);
		    	    				paramList.add(new String(param.trim().replace("'", "")));
		    	    			}else if(param.contains(".") || param.toLowerCase().contains("(double)") || param.toLowerCase().contains("(float)")){
		    	    				// gotta allow using float or double
		    	    				if(param.toLowerCase().contains("(double)")){
		    	    					primaryParametersClasses.add(double.class);
		    	    					paramList.add(Double.parseDouble(param.replace("(double)","").trim()));
		    	    				}else{		    	    					
			    	    				primaryParametersClasses.add(float.class);
			    	    				paramList.add(Float.parseFloat(param.replace("(float)","").trim()));			    	    			
		    	    				}
		    	    			}else{
		    	    				if(param.toLowerCase().contains("(long)")){
		    	    					primaryParametersClasses.add(long.class);
		    	    					paramList.add(Long.parseLong(param.toLowerCase().replace("(long)","").trim()));
		    	    				}else{
		    	    					primaryParametersClasses.add(int.class);
		    	    					paramList.add(Integer.parseInt(param.toLowerCase().replace("(int)","").trim()));
		    	    				}
		    	    			}
		        				nb++;
		        			}
	        			}catch(Exception e){
	        				e.printStackTrace();
	        			}
	        		}	        		
	        }
        	        	
        	
        	if(nb>0){
        		// Instantiate load balancing policy with parameters
        		if(lbString.toLowerCase().contains("latencyawarepolicy")){
        			//special sauce for the latency aware policy which uses a builder subclass to instantiate
        			Builder builder = LatencyAwarePolicy.builder((LoadBalancingPolicy) paramList.get(0));
        			
                    builder.withExclusionThreshold((Double)paramList.get(1));
        			builder.withScale((Long)paramList.get(2), TimeUnit.MILLISECONDS);
        			builder.withRetryPeriod((Long)paramList.get(3), TimeUnit.MILLISECONDS);
        			builder.withUpdateRate((Long)paramList.get(4), TimeUnit.MILLISECONDS);
        			builder.withMininumMeasurements((Integer)paramList.get(5));
        			
        			return builder.build();
        			
        		}else{
	        		Class<?> clazz = Class.forName(lbString);
	        		Constructor<?> constructor = clazz.getConstructor(primaryParametersClasses.toArray(new Class[primaryParametersClasses.size()]));
	        		
	        		return (LoadBalancingPolicy) constructor.newInstance(paramList.toArray(new Object[paramList.size()]));
        		}
        	}else{
        		// Only one policy has been specified, with no parameter or child policy         		
        		Class<?> clazz = Class.forName(lbString);			
        		policy =  (LoadBalancingPolicy) clazz.newInstance();
    		
        		return policy;
        	
        	}        	
    			    	
    	}else{
    		// Only one policy has been specified, with no parameter or child policy 
    		
    		Class<?> clazz = Class.forName(lbString);			
    		policy =  (LoadBalancingPolicy) clazz.newInstance();
		
    		return policy;
    	
    	}
    	//return null;
    }
    
    public static RetryPolicy parseRetryPolicy(String retryPolicyString) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException{
    
		if(!retryPolicyString.contains(".")){
			retryPolicyString="com.datastax.driver.core.policies."+ retryPolicyString;
			
			Class<?> clazz = Class.forName(retryPolicyString);
			
    		Field field = clazz.getDeclaredField("INSTANCE");
    		RetryPolicy policy = (RetryPolicy) field.get(null);
		
    		return policy;
		}
    	
		return null;
    }
    
    
    public static ReconnectionPolicy parseReconnectionPolicy(String reconnectionPolicyString) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException{
    	String lb_regex = "([a-zA-Z]*Policy)(\\()(.*)(\\))";
    	Pattern lb_pattern = Pattern.compile(lb_regex);
    	Matcher lb_matcher = lb_pattern.matcher(reconnectionPolicyString);
    	
    	if(lb_matcher.matches()){
	    	if(lb_matcher.groupCount()>0){
	    		// Primary LB policy has been specified
	    		String primaryReconnectionPolicy = lb_matcher.group(1);
	    		String reconnectionPolicyParams = lb_matcher.group(3);
	    		return getReconnectionPolicy(primaryReconnectionPolicy, reconnectionPolicyParams);
	    	}
    	}
    	
		return null;
    }
    
    public static ReconnectionPolicy getReconnectionPolicy(String rcString, String parameters) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
    	ReconnectionPolicy policy = null;
    	//ReconnectionPolicy childPolicy = null;
    	if(!rcString.contains(".")){
    		rcString="com.datastax.driver.core.policies."+ rcString;
		}
    	
    	if(parameters.length()>0){
    		// Child policy or parameters have been specified
    		System.out.println("parameters = " + parameters);
    		String paramsRegex = "([^,]+\\(.+?\\))|([^,]+)";    
    		//String lb_regex = "([a-zA-Z]*Policy)(\\()(.*)(\\))";
    		Pattern param_pattern = Pattern.compile(paramsRegex);
        	Matcher lb_matcher = param_pattern.matcher(parameters);
        	        	
    		ArrayList<Object> paramList = Lists.newArrayList();
    		ArrayList<Class> primaryParametersClasses = Lists.newArrayList();
        	int nb=0;
        	while(lb_matcher.find()){
	        	if(lb_matcher.groupCount()>0){	        		
	        			try{
		        			if(lb_matcher.group().contains("(") && !lb_matcher.group().trim().startsWith("(")){
		        				// We are dealing with child policies here
		        				primaryParametersClasses.add(LoadBalancingPolicy.class);
		        				// Parse and add child policy to the parameter list
		        				paramList.add(parseReconnectionPolicy(lb_matcher.group()));
		        				nb++;
		        			}else{
		        				// We are dealing with parameters that are not policies here
		        				String param = lb_matcher.group();
		        				if(param.contains("'")){
		    	    				primaryParametersClasses.add(String.class);
		    	    				paramList.add(new String(param.trim().replace("'", "")));
		    	    			}else if(param.contains(".") || param.toLowerCase().contains("(double)") || param.toLowerCase().contains("(float)")){
		    	    				// gotta allow using float or double
		    	    				if(param.toLowerCase().contains("(double)")){
		    	    					primaryParametersClasses.add(double.class);
		    	    					paramList.add(Double.parseDouble(param.replace("(double)","").trim()));
		    	    				}else{		    	    					
			    	    				primaryParametersClasses.add(float.class);
			    	    				paramList.add(Float.parseFloat(param.replace("(float)","").trim()));			    	    			
		    	    				}
		    	    			}else{
		    	    				if(param.toLowerCase().contains("(long)")){
		    	    					primaryParametersClasses.add(long.class);
		    	    					paramList.add(Long.parseLong(param.toLowerCase().replace("(long)","").trim()));
		    	    				}else{
		    	    					primaryParametersClasses.add(int.class);
		    	    					paramList.add(Integer.parseInt(param.toLowerCase().replace("(int)","").trim()));
		    	    				}
		    	    			}
		        				nb++;
		        			}
	        			}catch(Exception e){
	        				e.printStackTrace();
	        			}
	        		}	        		
	        }
        	        	
        	
        	if(nb>0){
        		// Instantiate load balancing policy with parameters
	        		Class<?> clazz = Class.forName(rcString);
	        		Constructor<?> constructor = clazz.getConstructor(primaryParametersClasses.toArray(new Class[primaryParametersClasses.size()]));
	        		
	        		return (ReconnectionPolicy) constructor.newInstance(paramList.toArray(new Object[paramList.size()]));

        	}
			// Only one policy has been specified, with no parameter or child policy         		
			Class<?> clazz = Class.forName(rcString);			
			policy =  (ReconnectionPolicy) clazz.newInstance();
 		
			return policy;        	
    			    	
    	}
		Class<?> clazz = Class.forName(rcString);			
		policy =  (ReconnectionPolicy) clazz.newInstance();

		return policy;
    }
    
}
