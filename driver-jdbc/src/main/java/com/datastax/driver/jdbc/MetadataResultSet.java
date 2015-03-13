package com.datastax.driver.jdbc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

public class MetadataResultSet{
	
	

	private ArrayList<MetadataRow> rows;
	
	public MetadataResultSet(){
		
	}
	
	public MetadataResultSet setRows(ArrayList<MetadataRow> schemas){
		this.rows = schemas;
		return this;
	}
	

	public ColumnDefinitions getColumnDefinitions() {
		// TODO Auto-generated method stub
		return null;
	}


	public boolean isExhausted() {
		// TODO Auto-generated method stub
		return false;
	}


	public Row one() {
		// TODO Auto-generated method stub
		return null;
	}


	public List<MetadataRow> all() {
		// TODO Auto-generated method stub
		return rows;
	}


	public Iterator<MetadataRow> iterator() {
		// TODO Auto-generated method stub
		return rows.iterator();
	}


	public int getAvailableWithoutFetching() {
		// TODO Auto-generated method stub
		return 0;
	}


	public boolean isFullyFetched() {
		// TODO Auto-generated method stub
		return false;
	}


	public ListenableFuture<Void> fetchMoreResults() {
		// TODO Auto-generated method stub
		return null;
	}


	public ExecutionInfo getExecutionInfo() {
		// TODO Auto-generated method stub
		return null;
	}


	public List<ExecutionInfo> getAllExecutionInfo() {
		// TODO Auto-generated method stub
		return null;
	}


	public boolean wasApplied() {
		// TODO Auto-generated method stub
		return false;
	}

}
