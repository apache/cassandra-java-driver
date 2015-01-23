package org.apache.cassandra.cql.jdbc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

public class MetadataResultSet implements ResultSet {
	
	

	private ArrayList<Row> rows;
	
	public MetadataResultSet(){
		
	}
	
	public MetadataResultSet setRows(ArrayList<Row> rows){
		this.rows = rows;
		return this;
	}
	
	@Override
	public ColumnDefinitions getColumnDefinitions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isExhausted() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Row one() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Row> all() {
		// TODO Auto-generated method stub
		return rows;
	}

	@Override
	public Iterator<Row> iterator() {
		// TODO Auto-generated method stub
		return rows.iterator();
	}

	@Override
	public int getAvailableWithoutFetching() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isFullyFetched() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ListenableFuture<Void> fetchMoreResults() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ExecutionInfo getExecutionInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ExecutionInfo> getAllExecutionInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean wasApplied() {
		// TODO Auto-generated method stub
		return false;
	}

}
