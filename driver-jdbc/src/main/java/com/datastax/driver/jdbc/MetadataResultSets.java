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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.Lists;

public  class MetadataResultSets
{
    static final String TABLE_CONSTANT = "TABLE";

    public static final MetadataResultSets instance = new MetadataResultSets();
    
    
    // Private Constructor
    private MetadataResultSets() {}
    
    /**
     * Make a {@code Column} from a column name and a value.
     * 
     * @param name
     *          the name of the column
     * @param value
     *          the value of the column as a {@code ByteBufffer}
     * 
     * @return {@code Column}
     */
    private static final Row makeColumn(String name, String value)
    {
      return new MetadataRow().addEntry(name, value);
    }

    public  CassandraResultSet makeTableTypes(CassandraStatement statement) throws SQLException
    {
        final ArrayList<Row> tableTypes = Lists.newArrayList();
        MetadataRow row = new MetadataRow().addEntry("TABLE_TYPE", TABLE_CONSTANT);
        tableTypes.add(row);
        CassandraResultSet result = new CassandraResultSet(statement,new MetadataResultSet().setRows(tableTypes));
        return result;
    }

   public  CassandraResultSet makeCatalogs(CassandraStatement statement) throws SQLException
    {
	   final ArrayList<Row> catalog = Lists.newArrayList();
        MetadataRow row = new MetadataRow().addEntry("TABLE_CAT", statement.connection.getCatalog());
        catalog.add(row);
       
        CassandraResultSet result = new CassandraResultSet(statement,new MetadataResultSet().setRows(catalog));
        return result;
        /* final Entry[][] catalogs = { { new Entry("TABLE_CAT",bytes(statement.connection.getCatalog()),Entry.ASCII_TYPE)} };

        // use catalogs with the key in column number 1 (one based)
        CqlResult cqlresult =  makeCqlResult(catalogs, 1);
        
        CassandraResultSet result = new CassandraResultSet(statement,cqlresult);
        return result; 
        */
    }
 
    public  CassandraResultSet makeSchemas(CassandraStatement statement, String schemaPattern) throws SQLException
    {
    	
    	// TABLE_SCHEM String => schema name
        // TABLE_CATALOG String => catalog name (may be null)
    	
    	final ArrayList<Row> schemas = Lists.newArrayList();
    	List<KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();
    	
    	for(KeyspaceMetadata keyspace:keyspaces){
    		if ("%".equals(schemaPattern)) schemaPattern = null;
    		if((schemaPattern==null?keyspace.getName():schemaPattern).equals(keyspace.getName())){
    			MetadataRow row = new MetadataRow().addEntry("TABLE_SCHEM", keyspace.getName()).addEntry("TABLE_CATALOG", statement.connection.getCatalog());
    			schemas.add(row);
    		}
    		
    	}
       
        CassandraResultSet result = new CassandraResultSet(statement,new MetadataResultSet().setRows(schemas));
        return result;
    	    	
    }
    
    public CassandraResultSet makeTables(CassandraStatement statement, String schemaPattern, String tableNamePattern) throws SQLException
    {
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

        
        
        
        
        final ArrayList<Row> schemas = Lists.newArrayList();
    	List<KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();
    	
    	for(KeyspaceMetadata keyspace:keyspaces){
    		if ("%".equals(schemaPattern)) schemaPattern = null;
    		if((schemaPattern==null?keyspace.getName():schemaPattern).equals(keyspace.getName())){
    			Collection<TableMetadata> tables = keyspace.getTables();
	    		
	    		if ("%".equals(tableNamePattern)) tableNamePattern = null;
	    		for(TableMetadata table:tables){
	    			if((tableNamePattern==null?table.getName():tableNamePattern).equals(table.getName())){
		    			MetadataRow row = new MetadataRow().addEntry("TABLE_CAT", statement.connection.getCatalog())    					
		    					.addEntry("TABLE_SCHEM", keyspace.getName())
		    					.addEntry("TABLE_NAME", table.getName())
		    					.addEntry("TABLE_TYPE", TABLE_CONSTANT)
		    					.addEntry("REMARKS", table.getOptions().getComment())
		    					.addEntry("TYPE_CAT", null)
		    					.addEntry("TYPE_SCHEM", null)
		    					.addEntry("TYPE_NAME", null)
		    					.addEntry("SELF_REFERENCING_COL_NAME", null)
		    					.addEntry("REF_GENERATION", null);
		    			schemas.add(row);
	    			}
	    		}
    		}
    	
    	}
    	
       
        CassandraResultSet result = new CassandraResultSet(statement,new MetadataResultSet().setRows(schemas));
        return result;
        
    }
    
    public CassandraResultSet makeColumns(CassandraStatement statement, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException
    {
    	final ArrayList<Row> schemas = Lists.newArrayList();
    	List<KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();
    	
    	for(KeyspaceMetadata keyspace:keyspaces){
    		if ("%".equals(schemaPattern)) schemaPattern = null;
    		if((schemaPattern==null?keyspace.getName():schemaPattern).equals(keyspace.getName())){
    			Collection<TableMetadata> tables = keyspace.getTables();
	    		
	    		if ("%".equals(tableNamePattern)) tableNamePattern = null;
	    		for(TableMetadata table:tables){
	    			if((tableNamePattern==null?table.getName():tableNamePattern).equals(table.getName())){
	    				List<ColumnMetadata> columns = table.getColumns();
	    				if ("%".equals(columnNamePattern)) columnNamePattern = null;
	    				int columnIndex=1;
	    				for(ColumnMetadata column:columns){	    				
	    					if((columnNamePattern==null?column.getName():columnNamePattern).equals(column.getName())){
		    					// COLUMN_SIZE
		    					int length = -1;
		    		            AbstractJdbcType jtype = TypesMap.getTypeForComparator(column.getType().toString());
		    		            if (jtype instanceof JdbcBytes) length = Integer.MAX_VALUE / 2;
		    		            if (jtype instanceof JdbcAscii || jtype instanceof JdbcUTF8) length = Integer.MAX_VALUE;
		    		            if (jtype instanceof JdbcUUID) length = 36;
		    		            if (jtype instanceof JdbcInt32) length = 4;
		    		            if (jtype instanceof JdbcLong) length = 8;
		    					
		    		            //NUM_PREC_RADIX
		    		            int npr = 2;
		    		            if (jtype != null && (jtype.getJdbcType() == Types.DECIMAL || jtype.getJdbcType() == Types.NUMERIC)) npr = 10;
		    		            
		    		            //CHAR_OCTET_LENGTH
		    		            Integer charol = null;
		    		            if (jtype instanceof JdbcAscii || jtype instanceof JdbcUTF8) charol = Integer.MAX_VALUE;
		    		            
		    		            System.out.println("Type : " + column.getType().toString());
		    		            System.out.println("Name : " + column.getName());
		    		            int jdbcType=Types.OTHER;
		    		            try{
		    		            	jdbcType=TypesMap.getTypeForComparator(column.getType().toString()).getJdbcType();
		    		            }catch(Exception e){
		    		            	
		    		            }
				    			MetadataRow row = new MetadataRow().addEntry("TABLE_CAT", statement.connection.getCatalog())    					
				    					.addEntry("TABLE_SCHEM", keyspace.getName())
				    					.addEntry("TABLE_NAME", table.getName())
				    					.addEntry("COLUMN_NAME", column.getName())
				    					.addEntry("DATA_TYPE", jdbcType+"" )
				    					.addEntry("TYPE_NAME", column.getType().toString())
				    					.addEntry("COLUMN_SIZE", length+"")
				    					.addEntry("BUFFER_LENGTH", "0")
				    					.addEntry("DECIMAL_DIGITS", null)
				    					.addEntry("NUM_PREC_RADIX", npr+"")
				    					.addEntry("NULLABLE", DatabaseMetaData.columnNoNulls+"")
				    					.addEntry("REMARKS", column.toString())
				    					.addEntry("COLUMN_DEF", null)
				    					.addEntry("SQL_DATA_TYPE", null)
				    					.addEntry("SQL_DATETIME_SUB", null)
				    					.addEntry("CHAR_OCTET_LENGTH", charol+"")
				    					.addEntry("ORDINAL_POSITION", columnIndex+"")
				    					.addEntry("IS_NULLABLE", "")
				    					.addEntry("SCOPE_CATALOG", null)
				    					.addEntry("SCOPE_SCHEMA", null)
				    					.addEntry("SCOPE_TABLE", null)
				    					.addEntry("SOURCE_DATA_TYPE", null)
				    					.addEntry("IS_AUTOINCREMENT", "NO")
				    					.addEntry("IS_GENERATEDCOLUMN", "NO");			    					
				    			schemas.add(row);
				    			columnIndex++;
	    					}
	    				}
	    			}
	    		}
    		}
    	
    	}
    	
       
        CassandraResultSet result = new CassandraResultSet(statement,new MetadataResultSet().setRows(schemas));
        return result;
        // 1.TABLE_CAT String => table catalog (may be null)
        // 2.TABLE_SCHEM String => table schema (may be null)
        // 3.TABLE_NAME String => table name
        // 4.COLUMN_NAME String => column name
        // 5.DATA_TYPE int => SQL type from java.sql.Types
        // 6.TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
        // 7.COLUMN_SIZE int => column size.
        // 8.BUFFER_LENGTH is not used.
        // 9.DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
        // 10.NUM_PREC_RADIX int => Radix (typically either 10 or 2)
        // 11.NULLABLE int => is NULL allowed. - columnNoNulls - might not allow NULL values
        // - columnNullable - definitely allows NULL values
        // - columnNullableUnknown - nullability unknown
        //
        // 12.REMARKS String => comment describing column (may be null)
        // 13.COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in
        // single quotes (may be null)
        // 14.SQL_DATA_TYPE int => unused
        // 15.SQL_DATETIME_SUB int => unused
        // 16.CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
        // 17.ORDINAL_POSITION int => index of column in table (starting at 1)
        // 18.IS_NULLABLE String => ISO rules are used to determine the nullability for a column. - YES --- if the parameter can include
        // NULLs
        // - NO --- if the parameter cannot include NULLs
        // - empty string --- if the nullability for the parameter is unknown
        //
        // 19.SCOPE_CATLOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
        // 20.SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
        // 21.SCOPE_TABLE String => table name that this the scope of a reference attribure (null if the DATA_TYPE isn't REF)
        // 22.SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if
        // DATA_TYPE isn't DISTINCT or user-generated REF)
        // 23.IS_AUTOINCREMENT String => Indicates whether this column is auto incremented - YES --- if the column is auto incremented
        // - NO --- if the column is not auto incremented
        // - empty string --- if it cannot be determined whether the column is auto incremented parameter is unknown
        // 24. IS_GENERATEDCOLUMN String => Indicates whether this is a generated column ï YES --- if this a generated column
        // - NO --- if this not a generated column
        // - empty string --- if it cannot be determined whether this is a generated column

      
    }
    
    
    public CassandraResultSet makeIndexes(CassandraStatement statement, String schema, String tableName, boolean unique, boolean approximate) throws SQLException
	{
    	
    	final ArrayList<Row> schemas = Lists.newArrayList();
    	List<KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();
    	
    	for(KeyspaceMetadata keyspace:keyspaces){
    		if(schema.equals(keyspace.getName())){
    			Collection<TableMetadata> tables = keyspace.getTables();
	    		
	    		
	    		for(TableMetadata table:tables){
	    			if(tableName.equals(table.getName())){
	    				
	    				for(ColumnMetadata col:table.getColumns()){
	    					if(col.getIndex()!=null){
	    						MetadataRow row = new MetadataRow().addEntry("TABLE_CAT", statement.connection.getCatalog())    					
			    					.addEntry("TABLE_SCHEM", keyspace.getName())
			    					.addEntry("TABLE_NAME", table.getName())
			    					.addEntry("NON_UNIQUE", true+"")
			    					.addEntry("INDEX_QUALIFIER",  statement.connection.getCatalog())
			    					.addEntry("INDEX_NAME", col.getIndex().getName())
			    					.addEntry("TYPE", DatabaseMetaData.tableIndexHashed+"")
	    							.addEntry("ORDINAL_POSITION", 1+"")
	    							.addEntry("COLUMN_NAME", col.getName())
	    							.addEntry("ASC_OR_DESC", null)
	    							.addEntry("CARDINALITY", -1+"")
	    							.addEntry("PAGES", -1+"")
	    							.addEntry("FILTER_CONDITION", null);
	    						schemas.add(row);
	    					}
	    				}
	    			}

    		}
    	
    	}
    	}
	
    	
       
        CassandraResultSet result = new CassandraResultSet(statement,new MetadataResultSet().setRows(schemas));
        return result;
	
		//1.TABLE_CAT String => table catalog (may be null) 
		//2.TABLE_SCHEM String => table schema (may be null) 
		//3.TABLE_NAME String => table name 
		//4.NON_UNIQUE boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic 
		//5.INDEX_QUALIFIER String => index catalog (may be null); null when TYPE is tableIndexStatistic 
		//6.INDEX_NAME String => index name; null when TYPE is tableIndexStatistic 
		//7.TYPE short => index type: - tableIndexStatistic - this identifies table statistics that are returned in conjuction with a table's index descriptions 
		//- tableIndexClustered - this is a clustered index 
		//- tableIndexHashed - this is a hashed index 
		//- tableIndexOther - this is some other style of index 
		//
		//8.ORDINAL_POSITION short => column sequence number within index; zero when TYPE is tableIndexStatistic 
		//9.COLUMN_NAME String => column name; null when TYPE is tableIndexStatistic 
		//10.ASC_OR_DESC String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic 
		//11.CARDINALITY int => When TYPE is tableIndexStatistic, then this is the number of rows in the table; otherwise, it is the number of unique values in the index. 
		//12.PAGES int => When TYPE is tableIndexStatisic then this is the number of pages used for the table, otherwise it is the number of pages used for the current index. 
		//13.FILTER_CONDITION String => Filter condition, if any. (may be null) 
	
	    /* StringBuilder query = new StringBuilder("SELECT keyspace_name, columnfamily_name, column_name, component_index, index_name, index_options, index_type FROM system.schema_columns");*/
	
	}       
	

    
   
	
	public CassandraResultSet makePrimaryKeys(CassandraStatement statement, String schema, String tableName) throws SQLException
	{
		final ArrayList<Row> schemas = Lists.newArrayList();
    	List<KeyspaceMetadata> keyspaces = statement.connection.getClusterMetadata().getKeyspaces();
    	
    	for(KeyspaceMetadata keyspace:keyspaces){
    		if(schema.equals(keyspace.getName())){
    			Collection<TableMetadata> tables = keyspace.getTables();
	    		
	    		
	    		for(TableMetadata table:tables){
	    			if(tableName.equals(table.getName())){
	    				int seq=0;
	    				for(ColumnMetadata col:table.getPrimaryKey()){
	    						MetadataRow row = new MetadataRow().addEntry("TABLE_CAT", statement.connection.getCatalog())    					
			    					.addEntry("TABLE_SCHEM", keyspace.getName())
			    					.addEntry("TABLE_NAME", table.getName())
			    					.addEntry("COLUMN_NAME", col.getName())
	    							.addEntry("KEY_SEQ", seq+"")
	    							.addEntry("PK_NAME", null);
	    						schemas.add(row);
	    						seq++;
	    				}
	    				
	    			}

    		}
    	
    	}
    }
	
    	
       
        CassandraResultSet result = new CassandraResultSet(statement,new MetadataResultSet().setRows(schemas));
        return result;
		
		//1.TABLE_CAT String => table catalog (may be null) 
		//2.TABLE_SCHEM String => table schema (may be null) 
		//3.TABLE_NAME String => table name 
		//4.COLUMN_NAME String => column name 
		//5.KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key). 
		//6.PK_NAME String => primary key name (may be null) 

		
	}
	
	
}
