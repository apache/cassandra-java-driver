package org.apache.cassandra.cql.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MetadataRow implements com.datastax.driver.core.Row{
	
	private ArrayList<String> entries;

	public MetadataRow(){
		entries = Lists.newArrayList();
	}
	
	public MetadataRow addEntry(String key,String value){
		//entries.add(key);
		entries.add(value);
		return this;
	}
	
	
	@Override
	public UDTValue getUDTValue(int i) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TupleValue getTupleValue(int i) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UDTValue getUDTValue(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TupleValue getTupleValue(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnDefinitions getColumnDefinitions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isNull(int i) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isNull(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getBool(int i) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getBool(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getInt(int i) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt(String name) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(int i) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(String name) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Date getDate(int i) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getFloat(int i) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(String name) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble(int i) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble(String name) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ByteBuffer getBytesUnsafe(int i) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteBuffer getBytesUnsafe(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteBuffer getBytes(int i) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteBuffer getBytes(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getString(int i) {
		// TODO Auto-generated method stub
		
		return entries.get(i);
	}

	@Override
	public String getString(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigInteger getVarint(int i) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigInteger getVarint(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getDecimal(int i) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getDecimal(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UUID getUUID(int i) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UUID getUUID(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InetAddress getInet(int i) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InetAddress getInet(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> List<T> getList(int i, Class<T> elementsClass) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> List<T> getList(String name, Class<T> elementsClass) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Set<T> getSet(int i, Class<T> elementsClass) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Set<T> getSet(String name, Class<T> elementsClass) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, V> Map<K, V> getMap(int i, Class<K> keysClass,
			Class<V> valuesClass) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, V> Map<K, V> getMap(String name, Class<K> keysClass,
			Class<V> valuesClass) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/*
	 *
	 *makeCatalogs(CassandraStatement statement)
	 *
	 * // TABLE_CAT String => catalog_name
	 *
	 *
	 *
	 * makeSchemas(CassandraStatement statement, String schemaPattern) throws SQLException    	
        
        if ("%".equals(schemaPattern)) schemaPattern = null;

        // TABLE_SCHEM String => schema name
        // TABLE_CATALOG String => catalog name (may be null)
	 * 
	 * 
	 *  makeTables(CassandraStatement statement, String schemaPattern, String tableNamePattern)
    
        //   1.   TABLE_CAT String => table catalog (may be null)
        //   2.   TABLE_SCHEM String => table schema (may be null)
        //   3.   TABLE_NAME String => table name
        //   4.   TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
        //   5.   REMARKS String => explanatory comment on the table
        //   6.   TYPE_CAT String => the types catalog (may be null)
        //   7.   TYPE_SCHEM String => the types schema (may be null)
        //   8.   TYPE_NAME String => type name (may be null)
        //   9.   SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
        //   10.  REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
         *  
	 * 
	 * 
	 * 
	 * 
	 * 
	 */
	
	public String toString(){
		StringBuilder builder = new StringBuilder();
		for(String entry:entries){
			builder.append(entry + " -- ");
		}
		return "[" + builder.toString() + "]";
	}

}
