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
package org.apache.cassandra.cql.jdbc;

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

import org.apache.cassandra.cql.jdbc.utils.Column;

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

    /*private static final CqlRow makeRow(String key, List<Column> columnList)
    {
      return new CqlRow(bytes(key), columnList);
    }*/
    
/*    private static CqlMetadata makeMetadataAllString(List<String> colNameList)
    {
        Map<ByteBuffer,String> namesMap = new HashMap<ByteBuffer,String>();
        Map<ByteBuffer,String> valuesMap = new HashMap<ByteBuffer,String>();
        
        for (String name : colNameList)
        {
            namesMap.put(bytes(name), Entry.ASCII_TYPE);
            valuesMap.put(bytes(name), Entry.UTF8_TYPE);
        }
        
        return new CqlMetadata(namesMap,valuesMap,Entry.ASCII_TYPE,Entry.UTF8_TYPE);
    }
*/
/*    private static CqlMetadata makeMetadata(List<Entry> entries)
    {
        Map<ByteBuffer,String> namesMap = new HashMap<ByteBuffer,String>();
        Map<ByteBuffer,String> valuesMap = new HashMap<ByteBuffer,String>();
        
        for (Entry entry : entries)
        {
            namesMap.put(bytes(entry.name), Entry.ASCII_TYPE);
            valuesMap.put(bytes(entry.name), entry.type);
        }
        
        return new CqlMetadata(namesMap,valuesMap,Entry.ASCII_TYPE,Entry.UTF8_TYPE);
    }

*/
/*    private ResultSet makeCqlResult(Entry[][] rowsOfcolsOfKvps, int position)
    {
    	ResultSet result = new ResultSet();
        CqlMetadata meta = null;
        CqlRow row = null;
        Column column = null;
        List<Column> columnlist = new LinkedList<Column>();
        List<CqlRow> rowlist = new LinkedList<CqlRow>();
        List<String> colNamesList = new ArrayList<String>();
        
        
        for (int rowcnt = 0; rowcnt < rowsOfcolsOfKvps.length; rowcnt++ )
        {
            colNamesList = new ArrayList<String>();
            columnlist = new LinkedList<Column>();
            for (int colcnt = 0; colcnt < rowsOfcolsOfKvps[0].length; colcnt++ )
            {
                column = makeColumn(rowsOfcolsOfKvps[rowcnt][colcnt].name,rowsOfcolsOfKvps[rowcnt][colcnt].value);
                columnlist.add(column);
                colNamesList.add(rowsOfcolsOfKvps[rowcnt][colcnt].name);
            }
            row = makeRow(rowsOfcolsOfKvps[rowcnt][position-1].name,columnlist);
            rowlist.add(row);
        }
        
        meta = makeMetadataAllString(colNamesList);
        result.setSchema(meta).setRows(rowlist);
        return result;
    }
    
    private CqlResult makeCqlResult(List<List<Entry>> rows, int position) throws CharacterCodingException
    {
        CqlResult result = new CqlResult(CqlResultType.ROWS);
        CqlMetadata meta = null;
        CqlRow row = null;
        Column column = null;
        List<Column> columnlist = new LinkedList<Column>();
        List<CqlRow> rowlist = new LinkedList<CqlRow>();
        
        assert(!rows.isEmpty());
        
        for (List<Entry> aRow : rows )
        {
            columnlist = new LinkedList<Column>();
            
            assert (!aRow.isEmpty());
            
            // only need to do it once
            if (meta == null) meta = makeMetadata(aRow);
            
            for (Entry entry : aRow )
            {
                column = makeColumn(entry.name,entry.value);
                columnlist.add(column);
            }
            row = makeRow(string(columnlist.get(position-1).name),columnlist);
            rowlist.add(row);
        }
        
        result.setSchema(meta).setRows(rowlist);
        return result;
    }
 
    
*/
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
    	
    	/* if ("%".equals(schemaPattern)) schemaPattern = null;

        // TABLE_SCHEM String => schema name
        // TABLE_CATALOG String => catalog name (may be null)
        
        String query = "SELECT keyspace_name FROM system.schema_keyspaces";
        if (schemaPattern!=null) query = query + " where keyspace_name = '" + schemaPattern + "'";
        
        String catalog = statement.connection.getCatalog();
        Entry entryCatalog = new Entry("TABLE_CATALOG",bytes(catalog),Entry.ASCII_TYPE);
        
        CassandraResultSet result;
        List<Entry> col;
        List<List<Entry>> rows = new ArrayList<List<Entry>>();
        // determine the schemas
        result = (CassandraResultSet)statement.executeQuery(query);
        
        while (result.next())
        {
            Entry entrySchema = new Entry("TABLE_SCHEM",bytes(result.getString(1)),Entry.ASCII_TYPE);
            col = new ArrayList<Entry>();
            col.add(entrySchema);
            col.add(entryCatalog);
            rows.add(col);
        }
        
        // just return the empty result if there were no rows
        if (rows.isEmpty() )return result;

        // use schemas with the key in column number 2 (one based)
        CqlResult cqlresult;
        try
        {
            cqlresult = makeCqlResult(rows, 1);
        }
        catch (CharacterCodingException e)
        {
            throw new SQLTransientException(e);
        }
        
        result = new CassandraResultSet(statement,cqlresult);
        return result;
        */
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
    
    /*
    public CassandraResultSet makeIndexes(CassandraStatement statement, String schema, String table, boolean unique, boolean approximate) throws SQLException
	{
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
	
	    StringBuilder query = new StringBuilder("SELECT keyspace_name, columnfamily_name, column_name, component_index, index_name, index_options, index_type FROM system.schema_columns");
	
	    int filterCount = 0;
	    if (schema != null) filterCount++;
	    if (table != null) filterCount++;
	
	    // check to see if it is qualified
	    if (filterCount > 0)
	    {
	        String expr = "%s = '%s'";
	        query.append(" WHERE ");
	        if (schema != null) 
	        {
	        	query.append(String.format(expr, "keyspace_name", schema));
                filterCount--;
		        if (filterCount > 0) query.append(" AND ");
	        }
	        if (table != null) query.append(String.format(expr, "columnfamily_name", table));
	        query.append(" ALLOW FILTERING");
	    }
	    // System.out.println(query.toString());
	
	    String catalog = statement.connection.getCatalog();
	    Entry entryCatalog = new Entry("TABLE_CAT", bytes(catalog), Entry.ASCII_TYPE);
	
	    CassandraResultSet result;
	    List<Entry> col;
	    List<List<Entry>> rows = new ArrayList<List<Entry>>();
	
        int ordinalPosition = 0;
	    // define the columns
	    result = (CassandraResultSet) statement.executeQuery(query.toString());
	    while (result.next())
	    {
	        if (result.getString(7) == null) continue; //if there is no index_type its not an index
	        ordinalPosition++;

	        Entry entrySchema = new Entry("TABLE_SCHEM", bytes(result.getString(1)), Entry.ASCII_TYPE);
	        Entry entryTableName = new Entry("TABLE_NAME",
	            (result.getString(2) == null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER : bytes(result.getString(2)),
	            Entry.ASCII_TYPE);
	        Entry entryNonUnique = new Entry("NON_UNIQUE", bytes("true"),Entry.BOOLEAN_TYPE);
	        Entry entryIndexQualifier = new Entry("INDEX_QUALIFIER", bytes(catalog), Entry.ASCII_TYPE);
	        Entry entryIndexName = new Entry("INDEX_NAME", (result.getString(5) == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : bytes(result.getString(5))), Entry.ASCII_TYPE);
	        Entry entryType = new Entry("TYPE", bytes(DatabaseMetaData.tableIndexHashed), Entry.INT32_TYPE);
            Entry entryOrdinalPosition = new Entry("ORDINAL_POSITION", bytes(ordinalPosition), Entry.INT32_TYPE);
            Entry entryColumnName = new Entry("COLUMN_NAME",
                    (result.getString(3) == null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER : bytes(result.getString(3)),
                    Entry.ASCII_TYPE);
            Entry entryAoD = new Entry("ASC_OR_DESC",ByteBufferUtil.EMPTY_BYTE_BUFFER, Entry.ASCII_TYPE);
            Entry entryCardinality = new Entry("CARDINALITY", bytes(-1), Entry.INT32_TYPE);
            Entry entryPages = new Entry("PAGES", bytes(-1), Entry.INT32_TYPE);
            Entry entryFilter = new Entry("FILTER_CONDITION",ByteBufferUtil.EMPTY_BYTE_BUFFER, Entry.ASCII_TYPE);
	
	        col = new ArrayList<Entry>();
	        col.add(entryCatalog);
	        col.add(entrySchema);
	        col.add(entryTableName);
	        col.add(entryNonUnique);
	        col.add(entryIndexQualifier);
	        col.add(entryIndexName);
	        col.add(entryType);
	        col.add(entryOrdinalPosition);
	        col.add(entryColumnName);
	        col.add(entryAoD);
	        col.add(entryCardinality);
	        col.add(entryPages);
	        col.add(entryFilter);
	        rows.add(col);
	    }
	
	    // just return the empty result if there were no rows
	    if (rows.isEmpty()) return result;
	
	    // use schemas with the key in column number 2 (one based)
	    CqlResult cqlresult;
	    try
	    {
	        cqlresult = makeCqlResult(rows, 1);
	    }
	    catch (CharacterCodingException e)
	    {
	        throw new SQLTransientException(e);
	    }
	
	    result = new CassandraResultSet(statement, cqlresult);
	    return result;
	}

	public List<PKInfo> getPrimaryKeys(CassandraStatement statement, String schema, String table) throws SQLException
	{
		StringBuilder query = new StringBuilder("SELECT keyspace_name, columnfamily_name, key_aliases, key_validator, column_aliases, comparator FROM system.schema_columnfamilies");
		
	    int filterCount = 0;
	    if (schema != null) filterCount++;
	    if (table != null) filterCount++;
	
	    // check to see if it is qualified
	    if (filterCount > 0)
	    {
	        String expr = "%s = '%s'";
	        query.append(" WHERE ");
	        if (schema != null) 
	        {
	        	query.append(String.format(expr, "keyspace_name", schema));
                filterCount--;
		        if (filterCount > 0) query.append(" AND ");
	        }
	        if (table != null) query.append(String.format(expr, "columnfamily_name", table));
	        query.append(" ALLOW FILTERING");
	    }
	    // System.out.println(query.toString());
	
	    List<PKInfo> retval = new ArrayList<PKInfo>();
	    CassandraResultSet result = (CassandraResultSet) statement.executeQuery(query.toString());
	    if (result.next()) // all is reported back in one json row
	    {
	    	String rschema = result.getString(1);
   	    	String rtable = result.getString(2);
	        String validator = result.getString(4);
	        String key_aliases = result.getString(3);
	        buildPKInfo(retval,rschema,rtable,key_aliases,validator);
	        
	        String comparator = result.getString(6);
	        String column_aliases = result.getString(5);
	        buildPKInfo(retval,rschema,rtable,column_aliases,comparator);
	    }
	    return retval;
	}
	
	private void buildPKInfo(List<PKInfo> retval,String schema, String table,String aliases,String validator)
	{
		String[] typeNames = new String[0];
    	int[] types = new int[0]; 
        if (validator != null)
        {
        	String[] validatorArray = new String[]{validator};
        	String check = "CompositeType";
        	int idx = validator.indexOf(check);
        	if (idx > 0)
        	{
        		validator = validator.substring(idx+check.length()+1,validator.length()-1);
        		validatorArray = validator.split(",");
        	}
        	types = new int[validatorArray.length];
        	typeNames = new String[validatorArray.length];
        	for (int i = 0; i < validatorArray.length; i++) 
        	{
	        	AbstractJdbcType jtype = TypesMap.getTypeForComparator(validatorArray[i]);
	        	types[i] = (jtype != null ? jtype.getJdbcType() : Types.OTHER);

	            int dotidx = validatorArray[i].lastIndexOf('.');
	            typeNames[i] = validatorArray[i].substring(dotidx + 1);
        	}
        }

        if (aliases != null)
        {
        	aliases = aliases.replace("[","");
        	aliases = aliases.replace("]","");
        	aliases = aliases.replace("\"","");
        	if (aliases.trim().length() != 0)
        	{
	        	String[] kaArray = aliases.split(",");
	        	for (int i = 0; i < kaArray.length; i++) 
	        	{
	        		PKInfo pki = new PKInfo();
	        		pki.name = kaArray[i];
	        		pki.schema = schema;
	        		pki.table = table;
	        		pki.type = (i < types.length ? types[i] : Types.OTHER);
	        		pki.typeName = (i < typeNames.length ? typeNames[i] : "unknown");
	        		retval.add(pki);
				}
        	}
        }
	}
	
	private class PKInfo
	{
		public String typeName;
		public String schema;
		public String table;
		public String name;
		public int type;
	}
	
	public CassandraResultSet makePrimaryKeys(CassandraStatement statement, String schema, String table) throws SQLException
	{
		//1.TABLE_CAT String => table catalog (may be null) 
		//2.TABLE_SCHEM String => table schema (may be null) 
		//3.TABLE_NAME String => table name 
		//4.COLUMN_NAME String => column name 
		//5.KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key). 
		//6.PK_NAME String => primary key name (may be null) 

		List<PKInfo> pks = getPrimaryKeys(statement, schema, table);
		Iterator<PKInfo> it = pks.iterator();
		
	    String catalog = statement.connection.getCatalog();
	    Entry entryCatalog = new Entry("TABLE_CAT", (Object)catalog, Entry.ASCII_TYPE);

	    List<Entry> col;
	    List<List<Entry>> rows = new ArrayList<List<Entry>>();
	
        int seq = 0;
	    // define the columns
        while (it.hasNext()) 
        {
			PKInfo info = it.next();
	        Entry entrySchema = new Entry("TABLE_SCHEM", (Object)info.schema, Entry.ASCII_TYPE);
	        Entry entryTableName = new Entry("TABLE_NAME", (Object)info.table,Entry.ASCII_TYPE);
	        Entry entryColumnName = new Entry("COLUMN_NAME", (Object)info.name, Entry.ASCII_TYPE);
	        seq++;
	        Entry entryKeySeq = new Entry("KEY_SEQ", (Object)seq, Entry.INT32_TYPE);
	        Entry entryPKName = new Entry("PK_NAME", (Object)null, Entry.ASCII_TYPE);
	
	        col = new ArrayList<Entry>();
	        col.add(entryCatalog);
	        col.add(entrySchema);
	        col.add(entryTableName);
	        col.add(entryColumnName);
	        col.add(entryKeySeq);
	        col.add(entryPKName);
	        rows.add(col);
	    }
	
	    // just return the empty result if there were no rows
	    if (rows.isEmpty()) return new CassandraResultSet();
	
	    // use schemas with the key in column number 2 (one based)
	    CqlResult cqlresult;
	    try
	    {
	        cqlresult = makeCqlResult(rows, 1);
	    }
	    catch (CharacterCodingException e)
	    {
	        throw new SQLTransientException(e);
	    }
	
	    return new CassandraResultSet(statement, cqlresult);
	}

	private class Entry
    {
    	static final String UTF8_TYPE = "UTF8Type";
        static final String ASCII_TYPE = "AsciiType";
        static final String INT32_TYPE = "Int32Type";
        static final String BOOLEAN_TYPE = "BooleanType";

        String name = null;
        Object value = null;
        String type = null;
        
        private Entry(String name,Object value,String type)
        {
            this.name = name;
            this.value = value;
            this.type = type;
        }        
    }
	
	public static ByteBuffer bytes(String s)
    {
        return ByteBuffer.wrap(s.getBytes("UTF-8"));
    }*/
}
