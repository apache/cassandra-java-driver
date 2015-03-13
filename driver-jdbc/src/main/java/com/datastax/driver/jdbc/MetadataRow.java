package com.datastax.driver.jdbc;

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







import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.jdbc.ColumnDefinitions.Definition;
import com.datastax.driver.jdbc.ColumnDefinitions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

//public class MetadataRow implements com.datastax.driver.core.Row{
public class MetadataRow{
	
	private ArrayList<String> entries;
	private HashMap<String,Integer> names;
	private ColumnDefinitions colDefinitions;
	private ArrayList<ColumnDefinitions.Definition> definitions;
	
	public MetadataRow(){
		entries = Lists.newArrayList();
		names = Maps.newHashMap();		
		definitions = Lists.newArrayList();
		
		
	}
	
	public MetadataRow addEntry(String key,String value){
		names.put(key,entries.size());
		entries.add(value);
		definitions.add(new Definition("","",key,DataType.text()));
		return this;
	}
	
	
	
	public UDTValue getUDTValue(int i) {
		// TODO Auto-generated method stub
		return null;
	}


	public TupleValue getTupleValue(int i) {
		// TODO Auto-generated method stub
		return null;
	}


	public UDTValue getUDTValue(String name) {
		// TODO Auto-generated method stub
		return null;
	}


	public TupleValue getTupleValue(String name) {
		// TODO Auto-generated method stub
		return null;
	}


	public ColumnDefinitions getColumnDefinitions() {
		// TODO Auto-generated method stub
		Definition[] definitionArr = new Definition[definitions.size()];
		definitionArr = definitions.toArray(definitionArr);
		

		return new ColumnDefinitions(definitionArr);
	}


	public boolean isNull(int i) {
		// TODO Auto-generated method stub
		return entries.get(i)==null;
	}


	public boolean isNull(String name) {
		// TODO Auto-generated method stub
		return isNull(names.get(name));
	}

 
	public boolean getBool(int i) {
		// TODO Auto-generated method stub
		return Boolean.parseBoolean(entries.get(i));
	}

 
	public boolean getBool(String name) {
		// TODO Auto-generated method stub
		return getBool(names.get(name));
	}

 
	public int getInt(int i) {
		// TODO Auto-generated method stub
		return Integer.parseInt(entries.get(i));
	}

 
	public int getInt(String name) {		
		return getInt(names.get(name));
	}

 
	public long getLong(int i) {
		// TODO Auto-generated method stub
		return Long.parseLong(entries.get(i));
	}

 
	public long getLong(String name) {
		// TODO Auto-generated method stub
		return getLong(names.get(name));
	}

 
	public Date getDate(int i) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public Date getDate(String name) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public float getFloat(int i) {
		// TODO Auto-generated method stub
		return 0;
	}

 
	public float getFloat(String name) {
		// TODO Auto-generated method stub
		return 0;
	}

 
	public double getDouble(int i) {
		// TODO Auto-generated method stub
		return 0;
	}

 
	public double getDouble(String name) {
		// TODO Auto-generated method stub
		return 0;
	}

 
	public ByteBuffer getBytesUnsafe(int i) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public ByteBuffer getBytesUnsafe(String name) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public ByteBuffer getBytes(int i) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public ByteBuffer getBytes(String name) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public String getString(int i) {
		// TODO Auto-generated method stub
		
		return entries.get(i);
	}

 
	public String getString(String name) {
		// TODO Auto-generated method stub
		return getString(names.get(name));
	}

 
	public BigInteger getVarint(int i) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public BigInteger getVarint(String name) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public BigDecimal getDecimal(int i) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public BigDecimal getDecimal(String name) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public UUID getUUID(int i) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public UUID getUUID(String name) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public InetAddress getInet(int i) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public InetAddress getInet(String name) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public <T> List<T> getList(int i, Class<T> elementsClass) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public <T> List<T> getList(String name, Class<T> elementsClass) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public <T> Set<T> getSet(int i, Class<T> elementsClass) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public <T> Set<T> getSet(String name, Class<T> elementsClass) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public <K, V> Map<K, V> getMap(int i, Class<K> keysClass,
			Class<V> valuesClass) {
		// TODO Auto-generated method stub
		return null;
	}

 
	public <K, V> Map<K, V> getMap(String name, Class<K> keysClass,
			Class<V> valuesClass) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	public String toString(){
		StringBuilder builder = new StringBuilder();
		for(String entry:entries){
			builder.append(entry + " -- ");
		}
		return "[" + builder.toString() + "]";
	}

}
