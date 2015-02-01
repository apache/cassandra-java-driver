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

import static com.datastax.driver.jdbc.Utils.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * <p>
 * The Supported Data types in CQL are as follows:
 * </p>
 * <table>
 * <tr>
 * <th>type</th>
 * <th>java type</th>
 * <th>description</th>
 * </tr>
 * <tr>
 * <td>ascii</td>
 * <td>String</td>
 * <td>ASCII character string</td>
 * </tr>
 * <tr>
 * <td>bigint</td>
 * <td>Long</td>
 * <td>64-bit signed long</td>
 * </tr>
 * <tr>
 * <td>blob</td>
 * <td>ByteBuffer</td>
 * <td>Arbitrary bytes (no validation)</td>
 * </tr>
 * <tr>
 * <td>boolean</td>
 * <td>Boolean</td>
 * <td>true or false</td>
 * </tr>
 * <tr>
 * <td>counter</td>
 * <td>Long</td>
 * <td>Counter column (64-bit long)</td>
 * </tr>
 * <tr>
 * <td>decimal</td>
 * <td>BigDecimal</td>
 * <td>Variable-precision decimal</td>
 * </tr>
 * <tr>
 * <td>double</td>
 * <td>Double</td>
 * <td>64-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>float</td>
 * <td>Float</td>
 * <td>32-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>int</td>
 * <td>Integer</td>
 * <td>32-bit signed int</td>
 * </tr>
 * <tr>
 * <td>text</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>timestamp</td>
 * <td>Date</td>
 * <td>A timestamp</td>
 * </tr>
 * <tr>
 * <td>uuid</td>
 * <td>UUID</td>
 * <td>Type 1 or type 4 UUID</td>
 * </tr>
 * <tr>
 * <td>varchar</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>varint</td>
 * <td>BigInteger</td>
 * <td>Arbitrary-precision integer</td>
 * </tr>
 * </table>
 * 
 */
class CassandraResultSet extends AbstractResultSet implements CassandraResultSetExtras
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraResultSet.class);

    public static final int DEFAULT_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    public static final int DEFAULT_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
    public static final int DEFAULT_HOLDABILITY = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    private Row currentRow;
    
    private ColumnDefinitions colDefinitions;
    //private com.datastax.driver.core.ResultSet datastaxRs;
    /**
     * The rows iterator.
     */
    private Iterator<Row> rowsIterator;


    int rowNumber = 0;
    // the current row key when iterating through results.
    private byte[] curRowKey = null;

    /**
     * The values.
     */
    // private List<Object> values = new ArrayList<Object>();

    /**
     * The index map.
     */
    // private Map<String, Integer> indexMap = new HashMap<String, Integer>();

    private final CResultSetMetaData meta;

    private final CassandraStatement statement;

    private int resultSetType;

    private int fetchDirection;

    private int fetchSize;

    private boolean wasNull;
    
    private com.datastax.driver.core.ResultSet driverResultSet;
    private ArrayList<com.datastax.driver.core.ResultSet> severalDriverResultSet;

    //private CqlMetadata schema;

    /**
     * no argument constructor.
     */
    CassandraResultSet()
    {
        statement = null;
        meta = new CResultSetMetaData();
    }

    /**
     * Instantiates a new cassandra result set from a com.datastax.driver.core.ResultSet.
     */
    CassandraResultSet(CassandraStatement statement, com.datastax.driver.core.ResultSet resultSet) throws SQLException
    {
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        this.driverResultSet = resultSet;
        //this.schema = resultSet.schema;

        // Initialize meta-data from schema
        populateMetaData();

        
        rowsIterator = resultSet.iterator();
        colDefinitions = resultSet.getColumnDefinitions();

        // Initialize to column values from the first row
        // re-Initialize meta-data to column values from the first row (if data exists)
        // NOTE: that the first call to next() will HARMLESSLY re-write these values for the columns
        // NOTE: the row cursor is not advanced and sits before the first row
        if (hasMoreRows())
        {
            populateColumns();            
            // reset the iterator back to the beginning.
            //rowsIterator = resultSet.iterator();
        }

        meta = new CResultSetMetaData();
    }
    
    /**
     * Instantiates a new cassandra result set from a CqlResult.
     */
    CassandraResultSet(CassandraStatement statement, ArrayList<com.datastax.driver.core.ResultSet> resultSets) throws SQLException
    {
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        
        // We have several result sets, but we will use only the first one for metadata needs
        this.driverResultSet = resultSets.get(0);
        //this.schema = resultSet.schema;

        // Initialize meta-data from schema
        populateMetaData();

        // Now we concatenate iterators of the different result sets into a single one and voilà !! ;) 
        rowsIterator = this.driverResultSet.iterator();
        for(int i=1;i<resultSets.size();i++){        	
        	rowsIterator = Iterators.concat(rowsIterator,resultSets.get(i).iterator());
        }
        colDefinitions = driverResultSet.getColumnDefinitions();

        // Initialize to column values from the first row
        // re-Initialize meta-data to column values from the first row (if data exists)
        // NOTE: that the first call to next() will HARMLESSLY re-write these values for the columns
        // NOTE: the row cursor is not advanced and sits before the first row
        if (hasMoreRows())
        {
            populateColumns();            
            // reset the iterator back to the beginning.
            //rowsIterator = resultSet.iterator();
        }

        meta = new CResultSetMetaData();
    }


    private final boolean hasMoreRows()
    {
        return (rowsIterator != null && (rowsIterator.hasNext() || (rowNumber==0 && currentRow!=null)));
    }

    private final void populateMetaData()
    {
    	
    	
    	/*
        values.clear();
        indexMap.clear();
        
        for (ByteBuffer name : this.schema.name_types.keySet())
        	{
        		TypedColumn c = createColumn(new Column(name));
        		String columnName = c.getNameString();            
        		values.add(c);            
        		indexMap.put(columnName, values.size()); // one greater than 0 based index of a list
        	}
        */
    }

    private final void populateColumns()
    {
    	
    	currentRow = rowsIterator.next();
     /*
    	// clear column value tables
        values.clear();
        indexMap.clear();

        CqlRow row = rowsIterator.next();
        curRowKey = row.getKey();
        List<Column> cols = row.getColumns();

        // loop through the columns
        for (Column col : cols)
        {
            TypedColumn c = createColumn(col);
            String columnName = c.getNameString();
            values.add(c);
            indexMap.put(columnName, values.size()); // one greater than 0 based index of a list
            
        }
        */
    }

    public boolean absolute(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void afterLast() throws SQLException
    {
        if (resultSetType == TYPE_FORWARD_ONLY) throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void beforeFirst() throws SQLException
    {
        if (resultSetType == TYPE_FORWARD_ONLY) throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    private final void checkIndex(int index) throws SQLException
    {
        // 1 <= index <= size()

    	if(currentRow!=null){
    		if(currentRow.getColumnDefinitions()!=null){
    			if (index < 1 || index > currentRow.getColumnDefinitions().asList().size()) throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, String.valueOf(index)) + " " + currentRow.getColumnDefinitions().asList().size());
    		}
    	}else if(driverResultSet!=null){
    		if(driverResultSet.getColumnDefinitions()!=null){
    			if (index < 1 || index > driverResultSet.getColumnDefinitions().asList().size()) throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, String.valueOf(index)) + " " + driverResultSet.getColumnDefinitions().asList().size());
    		}
    	}
    	
    }

    private final void checkName(String name) throws SQLException
    {
        //if (indexMap.get(name) == null) throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
    	if(currentRow!=null){
    		if (!currentRow.getColumnDefinitions().contains(name)) throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
    	}else if(driverResultSet!=null){
    		if(driverResultSet.getColumnDefinitions()!=null){
    			if (!driverResultSet.getColumnDefinitions().contains(name)) throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
    		}
    	
    	}
    }

    private final void checkNotClosed() throws SQLException
    {
        if (isClosed()) throw new SQLRecoverableException(WAS_CLOSED_RSLT);
    }

    public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    public void close() throws SQLException
    {
        /* indexMap = null;
        values = null; */
    	if(!isClosed()){
    		this.statement.close();
    	}
    }

    public int findColumn(String name) throws SQLException
    {
        checkNotClosed();
        checkName(name);
        return currentRow.getColumnDefinitions().getIndexOf(name);
    }

    public boolean first() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }


    public BigDecimal getBigDecimal(int index) throws SQLException
    {
        checkIndex(index);
        return currentRow.getDecimal(index - 1);
    }

    /** @deprecated */
    public BigDecimal getBigDecimal(int index, int scale) throws SQLException
    {
        checkIndex(index);
        return currentRow.getDecimal(index - 1).setScale(scale);
    }

    public BigDecimal getBigDecimal(String name) throws SQLException
    {
        checkName(name);
        return currentRow.getDecimal(name);
    }

    /** @deprecated */
    public BigDecimal getBigDecimal(String name, int scale) throws SQLException
    {
        checkName(name);
        return currentRow.getDecimal(name).setScale(scale);
    }

  

    public BigInteger getBigInteger(int index) throws SQLException
    {
        checkIndex(index);
        return currentRow.getVarint(index - 1);
    }

    public BigInteger getBigInteger(String name) throws SQLException
    {
        checkName(name);
        return currentRow.getVarint(name);
    }

    
    public boolean getBoolean(int index) throws SQLException
    {
        checkIndex(index);
        return currentRow.getBool(index-1);
    }

    public boolean getBoolean(String name) throws SQLException
    {
        checkName(name);
        return currentRow.getBool(name);
    }

    public byte getByte(int index) throws SQLException
    {
        checkIndex(index);
        return currentRow.getBytes(index - 1).get();
    }

    public byte getByte(String name) throws SQLException
    {
        checkName(name);
        return currentRow.getBytes(name).get();
    }


    public byte[] getBytes(int index) throws SQLException
    {
    	return currentRow.getBytes(index-1).array();
    }

    public byte[] getBytes(String name) throws SQLException
    {
        return currentRow.getBytes(name).array();
    }


/*    public TypedColumn getColumn(int index) throws SQLException
    {
        checkIndex(index);
        checkNotClosed();
        return values.get(index - 1);
    }

    public TypedColumn getColumn(String name) throws SQLException
    {
        checkName(name);
        checkNotClosed();
        return values.get(indexMap.get(name).intValue());
    }
*/
    public int getConcurrency() throws SQLException
    {
        checkNotClosed();
        return statement.getResultSetConcurrency();
    }

    public Date getDate(int index) throws SQLException
    {
        checkIndex(index);
        return new java.sql.Date(currentRow.getDate(index - 1).getTime());
    }

    public Date getDate(int index, Calendar calendar) throws SQLException
    {
        checkIndex(index);
        // silently ignore the Calendar argument; its a hint we do not need
        return getDate(index-1);
    }

    public Date getDate(String name) throws SQLException
    {
        checkName(name);
        return new java.sql.Date(currentRow.getDate(name).getTime());
    }

    public Date getDate(String name, Calendar calendar) throws SQLException
    {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
        return getDate(name);
    }


    public double getDouble(int index) throws SQLException
    {
        checkIndex(index);
        return currentRow.getDouble(index-1);
    }

    public double getDouble(String name) throws SQLException
    {
        checkName(name);
        return currentRow.getDouble(name);
    }

    
    public int getFetchDirection() throws SQLException
    {
        checkNotClosed();
        return fetchDirection;
    }

    public int getFetchSize() throws SQLException
    {
        checkNotClosed();
        return fetchSize;
    }

    public float getFloat(int index) throws SQLException
    {
        checkIndex(index);
        return currentRow.getFloat(index-1);
    }

    public float getFloat(String name) throws SQLException
    {
        checkName(name);
        return currentRow.getFloat(name);
    }


    public int getHoldability() throws SQLException
    {
        checkNotClosed();
        return statement.getResultSetHoldability();
    }

    public int getInt(int index) throws SQLException
    {
        checkIndex(index);
        return currentRow.getInt(index-1);
    }

    public int getInt(String name) throws SQLException
    {
        checkName(name);
        return currentRow.getInt(name);
    }


    public byte[] getKey() throws SQLException
    {
        return curRowKey;
    }

    public List<?> getList(int index) throws SQLException
    {
        checkIndex(index);
        if(currentRow.getColumnDefinitions().getType(index-1).isCollection()){
			try {
				return Lists.newArrayList(currentRow.getList(index - 1,Class.forName(currentRow.getColumnDefinitions().getType(index-1).getTypeArguments().get(0).asJavaClass().getCanonicalName())));
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			} // TODO: a remplacer par une vraie verification des types de collections
        }
        return currentRow.getList(index - 1,String.class);
    }

    public List<?> getList(String name) throws SQLException
    {
        checkName(name);
        // return currentRow.getList(name,String.class); // TODO: a remplacer par une vraie verification des types de collections
        if(currentRow.getColumnDefinitions().getType(name).isCollection()){
			try {
				return Lists.newArrayList(currentRow.getList(name,Class.forName(currentRow.getColumnDefinitions().getType(name).getTypeArguments().get(0).asJavaClass().getCanonicalName())));
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			} // TODO: a remplacer par une vraie verification des types de collections
        }
        return currentRow.getList(name,String.class);
    }



    public long getLong(int index) throws SQLException
    {
        checkIndex(index);
        try{
        	return currentRow.getLong(index - 1);
        }catch(InvalidTypeException e){
    		if(e.getMessage().contains("is of type varint")){
    			return currentRow.getVarint(index-1).longValue();
    		}
    		if(e.getMessage().contains("is of type int")){
    			return (long)currentRow.getInt(index-1);
    		}
    	}
        
        return currentRow.getLong(index - 1);
        
    }

    public long getLong(String name) throws SQLException
    {
        checkName(name);
        try{
        	return currentRow.getLong(name);
        }catch(InvalidTypeException e){
    		if(e.getMessage().contains("is of type varint")){
    			return currentRow.getVarint(name).longValue();
    		}
    		if(e.getMessage().contains("is of type int")){
    			return (long)currentRow.getInt(name);
    		}
    	}
        
        return currentRow.getLong(name);
    }

    
    public Map<?, ?> getMap(int index) throws SQLException
    {
        checkIndex(index);
        return currentRow.getMap(index-1,String.class,String.class); // TODO: a remplacer par une vraie verification des types de collections
    }

    public Map<?, ?> getMap(String name) throws SQLException
    {
        checkName(name);
        return currentRow.getMap(name,String.class,String.class); // TODO: a remplacer par une vraie verification des types de collections
    }


    public ResultSetMetaData getMetaData() throws SQLException
    {
        checkNotClosed();
        return meta;
    }

    public Object getObject(int index) throws SQLException
    {
        checkIndex(index);  
        List<DataType>  datatypes=null;
        
        if(currentRow.getColumnDefinitions().getType(index-1).isCollection()){
        	datatypes = currentRow.getColumnDefinitions().getType(index-1).getTypeArguments();
        	if(currentRow.getColumnDefinitions().getType(index-1).getName().toString().equals("set")){        	
        		return (Object) Sets.newLinkedHashSet(currentRow.getSet(index-1,TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
        	}
        	if(currentRow.getColumnDefinitions().getType(index-1).getName().toString().equals("list")){        		
        		return (Object) Lists.newArrayList(currentRow.getList(index-1,TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
        	}
        	if(currentRow.getColumnDefinitions().getType(index-1).getName().toString().equals("map")){        		
        		return (Object) Maps.newHashMap(currentRow.getMap(index-1,TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType(),TypesMap.getTypeForComparator(datatypes.get(1).toString()).getType()));
        	}
        	
        }else{
        	String typeName = currentRow.getColumnDefinitions().getType(index-1).getName().toString();        	
    		if (typeName.equals("varchar")){
    			return (Object) currentRow.getString(index-1);
    		}else if (typeName.equals("ascii")){
    			return (Object) currentRow.getString(index-1);
    		}else if (typeName.equals("integer")){
    			return (Object) currentRow.getInt(index-1);
    		}else if (typeName.equals("bigint")){        		
    			return (Object) currentRow.getLong(index-1);
    		}else if (typeName.equals("blob")){
    			return (Object) currentRow.getBytes(index-1);
    		}else if (typeName.equals("boolean")){
    			return (Object) currentRow.getBool(index-1);
    		}else if (typeName.equals("counter")){
    			return (Object) currentRow.getLong(index-1);
    		}else if (typeName.equals("decimal")){        			
    			return (Object) currentRow.getDecimal(index-1);
    		}else if (typeName.equals("double")){
    			return (Object) currentRow.getDouble(index-1);
    		}else if (typeName.equals("float")){
    			return (Object) currentRow.getFloat(index-1);
    		}else if (typeName.equals("inet")){
    			return (Object) currentRow.getInet(index-1);
    		}else if (typeName.equals("int")){
    			return (Object) currentRow.getInt(index-1);
    		}else if (typeName.equals("text")){
    			return (Object) currentRow.getString(index-1);
    		}else if (typeName.equals("timestamp")){        			
    	        return (Object) new Timestamp((currentRow.getDate(index-1)).getTime());
    		}else if (typeName.equals("uuid")){
    			return (Object) currentRow.getUUID(index-1);
    		}else if (typeName.equals("timeuuid")){
    			return (Object) currentRow.getUUID(index-1);
    		}else if (typeName.equals("varint")){
    	        return (Object) currentRow.getInt(index-1);
    		}
        			
        
        }
        	
        		
        
        return null; 
    }

    public Object getObject(String name) throws SQLException
    {    	
        checkName(name);
        List<DataType>  datatypes=null;
        
        if(currentRow.getColumnDefinitions().getType(name).isCollection()){
        	datatypes = currentRow.getColumnDefinitions().getType(name).getTypeArguments();
        	if(currentRow.getColumnDefinitions().getType(name).getName().toString().equals("set")){        	
        		return (Object) Sets.newLinkedHashSet(currentRow.getSet(name,TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
        	}
        	if(currentRow.getColumnDefinitions().getType(name).getName().toString().equals("list")){        		
        		
        		return (Object) Lists.newArrayList(currentRow.getList(name,TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
        	}
        	if(currentRow.getColumnDefinitions().getType(name).getName().toString().equals("map")){        		
        		return (Object) Maps.newHashMap(currentRow.getMap(name,TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType(),TypesMap.getTypeForComparator(datatypes.get(1).toString()).getType()));
        	}
        	
        }else{
        	String typeName = currentRow.getColumnDefinitions().getType(name).getName().toString();
        	
    		if (typeName.equals("varchar")){
    			return (Object) currentRow.getString(name);
    		}else if (typeName.equals("ascii")){
    			return (Object) currentRow.getString(name);
    		}else if (typeName.equals("integer")){
    			return (Object) currentRow.getInt(name);
    		}else if (typeName.equals("bigint")){        		
    			return (Object) currentRow.getLong(name);
    		}else if (typeName.equals("blob")){
    			return (Object) currentRow.getBytes(name);
    		}else if (typeName.equals("boolean")){
    			return (Object) currentRow.getBool(name);
    		}else if (typeName.equals("counter")){
    			return (Object) currentRow.getLong(name);
    		}else if (typeName.equals("decimal")){        			
    			return (Object) currentRow.getDecimal(name);
    		}else if (typeName.equals("double")){
    			return (Object) currentRow.getDouble(name);
    		}else if (typeName.equals("float")){
    			return (Object) currentRow.getFloat(name);
    		}else if (typeName.equals("inet")){
    			return (Object) currentRow.getInet(name);
    		}else if (typeName.equals("int")){
    			return (Object) currentRow.getInt(name);
    		}else if (typeName.equals("text")){
    			return (Object) currentRow.getString(name);
    		}else if (typeName.equals("timestamp")){        			
    	        return (Object) new Timestamp((currentRow.getDate(name)).getTime());
    		}else if (typeName.equals("uuid")){
    			return (Object) currentRow.getUUID(name);
    		}else if (typeName.equals("timeuuid")){
    			return (Object) currentRow.getUUID(name);
    		}else if (typeName.equals("varint")){
    	        return (Object) currentRow.getInt(name);
    		}
        	
        }
        	
        		
        
        return null; 
    }

    
    public int getRow() throws SQLException
    {
        checkNotClosed();
        return rowNumber;
    }

   

    public short getShort(int index) throws SQLException
    {
        checkIndex(index);
        return (short) currentRow.getInt(index - 1);
    }

    public Set<?> getSet(int index) throws SQLException
    {
        checkIndex(index);        
        try {
			return currentRow.getSet(index-1,Class.forName(currentRow.getColumnDefinitions().getType(index-1).getTypeArguments().get(0).asJavaClass().getCanonicalName()));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;
    }

    public Set<?> getSet(String name) throws SQLException
    {
        checkName(name);
        try {
			return currentRow.getSet(name,Class.forName(currentRow.getColumnDefinitions().getType(name).getTypeArguments().get(0).asJavaClass().getCanonicalName()));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;
    }


    public short getShort(String name) throws SQLException
    {
        checkName(name);
        return (short) currentRow.getInt(name);
    }


    public Statement getStatement() throws SQLException
    {
        checkNotClosed();
        return statement;
    }

    public String getString(int index) throws SQLException
    {
        checkIndex(index);
        try{
	        if(currentRow.getColumnDefinitions().getType(index-1).isCollection()){
	        	return getObject(index).toString();
	        }
	        return currentRow.getString(index-1);
        }catch(Exception e){
        	return getObject(index).toString();
        }
    }

    public String getString(String name) throws SQLException
    {
        checkName(name);
        try{
	        if(currentRow.getColumnDefinitions().getType(name).isCollection()){
	        	return getObject(name).toString();
	        }
	        return currentRow.getString(name);
        }catch(Exception e){
        	return getObject(name).toString();
        }
    }
    

    public Time getTime(int index) throws SQLException
    {
        checkIndex(index);
        java.util.Date date = currentRow.getDate(index-1);
        if(date==null)
        	return null;
        return new Time(currentRow.getDate(index - 1).getTime());
    }

    public Time getTime(int index, Calendar calendar) throws SQLException
    {
        checkIndex(index);
        // silently ignore the Calendar argument; its a hint we do not need
        java.util.Date date = currentRow.getDate(index-1);
        if(date==null)
        	return null;
        return getTime(index-1);
    }

    public Time getTime(String name) throws SQLException
    {
        checkName(name);
        java.util.Date date = currentRow.getDate(name);
        if(date==null)
        	return null;
        return new Time(currentRow.getDate(name).getTime());
    }

    public Time getTime(String name, Calendar calendar) throws SQLException
    {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(name);
    }
    
    public Timestamp getTimestamp(int index) throws SQLException
    {
        checkIndex(index);
        java.util.Date date = currentRow.getDate(index-1);
        if(date==null)
        	return null;
        return new Timestamp(currentRow.getDate(index - 1).getTime());
    }

    public Timestamp getTimestamp(int index, Calendar calendar) throws SQLException
    {
        checkIndex(index);
        // silently ignore the Calendar argument; its a hint we do not need
        java.util.Date date = currentRow.getDate(index-1);
        if(date==null)
        	return null;
        return getTimestamp(index-1);
    }

    public Timestamp getTimestamp(String name) throws SQLException
    {
        checkName(name);
        java.util.Date date = currentRow.getDate(name);
        if(date==null)
        	return null;
        return new Timestamp(currentRow.getDate(name).getTime());
    }

    public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException
    {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTimestamp(name);
    }


    public int getType() throws SQLException
    {
        checkNotClosed();
        return resultSetType;
    }

    // URL (awaiting some clarifications as to how it is stored in C* ... just a validated Sting in URL format?
    public URL getURL(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public URL getURL(String arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    // These Methods are planned to be implemented soon; but not right now...
    // Each set of methods has a more detailed set of issues that should be considered fully...


    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }


    public boolean isAfterLast() throws SQLException
    {
        checkNotClosed();
        return rowNumber == Integer.MAX_VALUE;
    }

    public boolean isBeforeFirst() throws SQLException
    {
        checkNotClosed();
        return rowNumber == 0;
    }

    public boolean isClosed() throws SQLException
    {
    	if(this.statement==null){
    		return true;
    	}else{
    		return this.statement.isClosed();
    	}
    	//return values == null;
    }

    public boolean isFirst() throws SQLException
    {
        checkNotClosed();
        return rowNumber == 1;
    }

    public boolean isLast() throws SQLException
    {
        checkNotClosed();
        return !rowsIterator.hasNext();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return CassandraResultSetExtras.class.isAssignableFrom(iface);
    }

    // Navigation between rows within the returned set of rows
    // Need to use a list iterator so next() needs completely re-thought

    public boolean last() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public synchronized boolean next() throws SQLException
    {
        if (hasMoreRows())
        {
            // populateColumns is called upon init to set up the metadata fields; so skip first call
            if (rowNumber != 0) populateColumns();
            //else rowsIterator.next();
// populateColumns();
            rowNumber++;
            return true;
        }
        else
        {
            rowNumber = Integer.MAX_VALUE;
            return false;
        }
    }

  

    public boolean previous() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean relative(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        checkNotClosed();

        if (direction == FETCH_FORWARD || direction == FETCH_REVERSE || direction == FETCH_UNKNOWN)
        {
            if ((getType() == TYPE_FORWARD_ONLY) && (direction != FETCH_FORWARD)) throw new SQLSyntaxErrorException("attempt to set an illegal direction : " + direction);
            fetchDirection = direction;
        }
        throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
    }

    public void setFetchSize(int size) throws SQLException
    {
        checkNotClosed();
        if (size < 0) throw new SQLException(String.format(BAD_FETCH_SIZE, size));
        fetchSize = size;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (iface.equals(CassandraResultSetExtras.class)) return (T) this;

        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    public boolean wasNull() throws SQLException
    {
        return wasNull;
    }
    
    

    /**
     * RSMD implementation. The metadata returned refers to the column
     * values, not the column names.
     */
    class CResultSetMetaData implements ResultSetMetaData
    {
        /**
         * return the Cassandra Cluster Name as the Catalog
         */
        public String getCatalogName(int column) throws SQLException
        {
            //checkIndex(column);
            return statement.connection.getCatalog();
        }

        public String getColumnClassName(int column) throws SQLException
        {/*
            checkIndex(column);
            return values.get(column - 1).getValueType().getType().getName();*/
        	if(currentRow!=null){
        		return currentRow.getColumnDefinitions().getType(column-1).asJavaClass().getCanonicalName();
            }else{
            	return driverResultSet.getColumnDefinitions().asList().get(column-1).getType().asJavaClass().getCanonicalName();
            }
        	
        }

        public int getColumnCount() throws SQLException
        {
        	if(currentRow!=null){
        		return currentRow.getColumnDefinitions().size();
        	}else{
        		return driverResultSet.getColumnDefinitions().size();
        	}
        }

        public int getColumnDisplaySize(int column) throws SQLException
        {
            //checkIndex(column);
            Definition col = null;
            if(currentRow!=null){
            	col = currentRow.getColumnDefinitions().asList().get(column-1);
            }else{
            	col = driverResultSet.getColumnDefinitions().asList().get(column-1);
            }
            try{
            	
            	int length = -1;
	            AbstractJdbcType jtype = TypesMap.getTypeForComparator(col.getType().toString());
	            if (jtype instanceof JdbcBytes) length = Integer.MAX_VALUE / 2;
	            if (jtype instanceof JdbcAscii || jtype instanceof JdbcUTF8) length = Integer.MAX_VALUE;
	            if (jtype instanceof JdbcUUID) length = 36;
	            if (jtype instanceof JdbcInt32) length = 4;
	            if (jtype instanceof JdbcLong) length = 8;
            	// String stringValue = getObject(column).toString();
            	//return (stringValue == null ? -1 : stringValue.length());
	            
	            return length;
            }catch(Exception e){
            	return -1;
            }
            //return -1;
        }

        public String getColumnLabel(int column) throws SQLException
        {
            //checkIndex(column);
            return getColumnName(column);
        }

        public String getColumnName(int column) throws SQLException
        {
            //checkIndex(column);
            
            
            if(currentRow!=null){
            	return currentRow.getColumnDefinitions().getName(column-1);
            }else{
            	return driverResultSet.getColumnDefinitions().asList().get(column-1).getName();
            }
        }

        public int getColumnType(int column) throws SQLException
        {        	
        	DataType type = null;        	
        	if(currentRow!=null){
        		type = currentRow.getColumnDefinitions().getType(column-1);
        	}else{
        		type = driverResultSet.getColumnDefinitions().asList().get(column-1).getType();
        	}
        	return TypesMap.getTypeForComparator(type.toString()).getJdbcType();
            
        }

        /**
         * Spec says "database specific type name"; for Cassandra this means the AbstractType.
         */
        public String getColumnTypeName(int column) throws SQLException
        {
        	
            //checkIndex(column);
            DataType type = null;
            if(currentRow!=null){
            	type = currentRow.getColumnDefinitions().getType(column-1);
            }else{
            	type = driverResultSet.getColumnDefinitions().getType(column-1);
            }
        	        	
        	return type.toString();            
        }

        public int getPrecision(int column) throws SQLException
        {
//            checkIndex(column);
//            TypedColumn col = values.get(column - 1);
//            return col.getValueType().getPrecision(col.getValue());
        	return 0;
        }

        public int getScale(int column) throws SQLException
        {
//            checkIndex(column);
//            TypedColumn tc = values.get(column - 1);
//            return tc.getValueType().getScale(tc.getValue());
        	return 0;
        }

        /**
         * return the DEFAULT current Keyspace as the Schema Name
         */
        public String getSchemaName(int column) throws SQLException
        {
            //checkIndex(column);
            return statement.connection.getSchema();
        }

       /* public String getTableName(int column) throws SQLException
        {
            throw new SQLFeatureNotSupportedException();
        }*/

        public boolean isAutoIncrement(int column) throws SQLException
        {
        	return true;
//            checkIndex(column);
//            return values.get(column - 1).getValueType() instanceof JdbcCounterColumn; // todo: check Value is correct.
        }

        public boolean isCaseSensitive(int column) throws SQLException
        {
//            checkIndex(column);
//            TypedColumn tc = values.get(column - 1);
//            return tc.getValueType().isCaseSensitive();
        	return true;
        }

        public boolean isCurrency(int column) throws SQLException
        {
//            checkIndex(column);
//            TypedColumn tc = values.get(column - 1);
//            return tc.getValueType().isCurrency();
        	return false;
        }

        public boolean isDefinitelyWritable(int column) throws SQLException
        {
            //checkIndex(column);
            return isWritable(column);
        }

        /**
         * absence is the equivalent of null in Cassandra
         */
        public int isNullable(int column) throws SQLException
        {
            //checkIndex(column);
            return ResultSetMetaData.columnNullable;
        }

        public boolean isReadOnly(int column) throws SQLException
        {
            //checkIndex(column);
            return column == 0;
        }

        public boolean isSearchable(int column) throws SQLException
        {
            //checkIndex(column);
            return false;
        }

        public boolean isSigned(int column) throws SQLException
        {
//            checkIndex(column);
//            TypedColumn tc = values.get(column - 1);
//            return tc.getValueType().isSigned();
        	return false;
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException
        {
            return false;
        }

        public boolean isWritable(int column) throws SQLException
        {
            //checkIndex(column);
            return column > 0;
        }

        public <T> T unwrap(Class<T> iface) throws SQLException
        {
            throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
        }

		@Override
		public String getTableName(int column) throws SQLException {
			String tableName="";
			if(currentRow!=null){
            	tableName = currentRow.getColumnDefinitions().getTable(column-1);
            }else{
            	tableName = driverResultSet.getColumnDefinitions().getTable(column-1);
            }
			return tableName;
		}
        
        
    }



	
	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		byte[] bytes = new byte[currentRow.getBytes(columnIndex-1).remaining()];
		currentRow.getBytes(columnIndex-1).get(bytes, 0, bytes.length);

		return new ByteArrayInputStream(bytes);
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		checkName(columnLabel);
		byte[] bytes = new byte[currentRow.getBytes(columnLabel).remaining()];
		currentRow.getBytes(columnLabel).get(bytes, 0, bytes.length);

		return new ByteArrayInputStream(bytes);
	}


}
