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
		return Integer.parseInt(entries.get(i));
	}

	@Override
	public int getInt(String name) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(int i) {
		// TODO Auto-generated method stub
		return Long.parseLong(entries.get(i));
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
	
	
	public String toString(){
		StringBuilder builder = new StringBuilder();
		for(String entry:entries){
			builder.append(entry + " -- ");
		}
		return "[" + builder.toString() + "]";
	}

}
