package org.apache.cassandra.cql.jdbc.utils;

import java.nio.ByteBuffer;
import java.util.List;

public class CqlRow implements java.io.Serializable, Cloneable{
	
	 public Object key; // required
	 public List<Column> columns; // required
	 
	 
	 

}
