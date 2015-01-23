package org.apache.cassandra.cql.jdbc.utils;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

public class Column implements java.io.Serializable {
	
	private ByteBuffer name;
	private ByteBuffer value;
	private long timestamp;
	
	public Column() {
	  }

	public Column(ByteBuffer name) {
		super();
		this.name = name;
	}

	public ByteBuffer getName() {
		return name;
	}

	public void setName(ByteBuffer name) {
		this.name = name;
	}

	public ByteBuffer getValue() {
		return value;
	}

	public Column setValue(ByteBuffer value) {
		this.value = value;
		return this;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Column setTimestamp(long timestamp) {
		this.timestamp = timestamp;
		return this;
	}
	
	
	
	
	
	
	

}
