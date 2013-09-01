/*
 * Copyright 2013 Otávio Gonçalves de Santana (otaviojava)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core.orm;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.orm.AddColumnUtil.AddColumn;
import com.datastax.driver.core.orm.VerifyRowUtil.VerifyRow;
import com.datastax.driver.core.orm.exception.FieldJavaNotEquivalentCQLException;
import com.datastax.driver.core.orm.exception.KeyProblemsException;

/**
 * Class to fix a column family
 * 
 * @author otaviojava
 * 
 */
class FixColumnFamily {

    /**
     * verify if exist column family and try to create
     * 
     * @param session
     *            - bridge to cassandra data base
     * @param familyColumn
     *            - name of family column
     * @param class1
     *            - bean
     * @return - if get it or not
     */
    public boolean verifyColumnFamily(Session session, String familyColumn,Class<?> class1) {
        try {
            ResultSet resultSet = session.execute("SELECT * FROM " + familyColumn + " LIMIT 1");
            verifyRowType(resultSet, class1, session);
            findIndex(class1, session);
            return true;
        } catch (InvalidQueryException exception) {

            if (exception.getCause().getMessage().contains("unconfigured columnfamily ")) {
                Logger.getLogger(FixColumnFamily.class.getName()).info("Column family doesn't exist, try to create");
                createColumnFamily(familyColumn, class1, session);
                findIndex(class1, session);
                return true;
            }
        }

        return false;
    }

    /**
     * Column family exists verify
     * 
     * @param resultSet
     */
    private void verifyRowType(ResultSet resultSet, Class<?> class1, Session session) {
        Map<String, String> mapNameType = new HashMap<String, String>();
        for (Definition column : resultSet.getColumnDefinitions()) {
            mapNameType.put(column.getName(), column.getType().getName().name());
        }
        verifyRow(class1, session, mapNameType);
    }

    /**
     * verify relationship beteween Java and CQL type
     * 
     * @param class1
     * @param session
     * @param mapNameType
     */
    private void verifyRow(Class<?> class1, Session session,
            Map<String, String> mapNameType) {

        for (Field field : ColumnUtil.INTANCE.listFields(class1)) {
            
            if (ColumnUtil.INTANCE.isEmbeddedField(field) || ColumnUtil.INTANCE.isEmbeddedIdField(field)) {
                continue;
            } else if (ColumnUtil.INTANCE.isEmbeddedField(field)) {
                verifyRow(field.getType(), session, mapNameType);
                continue;
            }

            String cqlType = mapNameType.get(ColumnUtil.INTANCE.getColumnName(field).toLowerCase());
            if (cqlType == null) {
                executeAlterTableAdd(class1, session, field);
                continue;
            }
            
            VerifyRow verifyRow=VerifyRowUtil.INTANCE.factory(field);
            List<String> cqlTypes = verifyRow.getTypes(field);

            if (!cqlTypes.contains(cqlType.toLowerCase())) {
                createMessageErro(class1, field, cqlType);
            }

        }
    }

    /**
     * call the command to alter table adding a field
     * 
     * @param class1
     *            - bean within column family
     * @param session
     *            - bridge to cassandra
     * @param field
     *            - field to add in column family
     */
    private void executeAlterTableAdd(Class<?> class1, Session session,Field field) {
        StringBuilder cqlAlterTable = new StringBuilder();
        cqlAlterTable.append("ALTER TABLE ").append( ColumnUtil.INTANCE.getColumnFamilyNameSchema(class1));
        cqlAlterTable.append(" ADD ");
        AddColumn addColumn=AddColumnUtil.INSTANCE.factory(field);
        cqlAlterTable.append(addColumn.addRow(field, RelationShipJavaCassandra.INSTANCE));
        cqlAlterTable.deleteCharAt(cqlAlterTable.length()-1);
        cqlAlterTable.append(";");
        session.execute(cqlAlterTable.toString());
    }

    /**
     * Field Java isn't equivalents with CQL type create error mensage
     * 
     * @param class1
     * @param field
     * @param cqlTypes
     */
    private void createMessageErro(Class<?> class1, Field field, String cqlType) {
        StringBuilder erroMensage = new StringBuilder();
        erroMensage.append("In the objetc ").append(class1.getName());
        erroMensage.append(" the field ").append(field.getName());
        erroMensage.append(" of the type ").append(field.getType().getName());
        erroMensage.append(" isn't equivalent with CQL type ").append(cqlType);
        erroMensage.append(" was expected: ").append(RelationShipJavaCassandra.INSTANCE.getJavaValue(cqlType.toLowerCase()));
        throw new FieldJavaNotEquivalentCQLException(erroMensage.toString());
    }

    /**
     * Column family doen'snt exist create with this method
     * 
     * @param familyColumn
     *            - name of column family
     * @param class1
     *            - bean
     * @param session
     *            bridge of cassandra data base
     */
    private void createColumnFamily(String familyColumn, Class<?> class1,Session session) {
        StringBuilder cqlCreateTable = new StringBuilder();
        cqlCreateTable.append("create table ");

        cqlCreateTable.append(familyColumn).append("( ");
        
        boolean isComplexID = createQueryCreateTable(class1, cqlCreateTable);
        if (isComplexID) {
            addComlexID(class1, cqlCreateTable);
        } else {
            addSimpleId(class1, cqlCreateTable);
        }

        session.execute(cqlCreateTable.toString());
    }

    /**
     * add in the query a simple id
     * @param class1
     * @param cqlCreateTable
     */
    private void addSimpleId(Class<?> class1, StringBuilder cqlCreateTable) {
        Field keyField = ColumnUtil.INTANCE.getKeyField(class1);
        if(keyField == null){
            createErro(class1);
        }
        cqlCreateTable.append(" PRIMARY KEY (").append(ColumnUtil.INTANCE.getColumnName(keyField)).append(") );");
    }

    
    
    
    /**
     * add in the query a complex key
     * @param class1
     * @param cqlCreateTable
     */
    private void addComlexID(Class<?> class1, StringBuilder cqlCreateTable) {
        Field keyField = ColumnUtil.INTANCE.getKeyComplexField(class1);
        cqlCreateTable.append(" PRIMARY KEY (");
        boolean firstTime = true;
        for (Field subKey : ColumnUtil.INTANCE.listFields(keyField.getType())) {
            if (firstTime) {
                cqlCreateTable.append(subKey.getName());
                firstTime = false;
            } else {
                cqlCreateTable.append(",").append(subKey.getName());
            }
        }
        cqlCreateTable.append(") );");
    }

    /**
     * 
     * @param bean
     */
    private void createErro(Class<?> bean) {
        
        StringBuilder erroMensage=new StringBuilder();
           
        erroMensage.append("the bean ").append(ColumnUtil.INTANCE.getColumnFamilyNameSchema(bean));
        erroMensage.append(" hasn't  a field with id annotation, you may to use either javax.persistence.Id");
        erroMensage.append(" to simple id or javax.persistence.EmbeddedId");
        erroMensage.append(" to complex id, another object with one or more fields annotated with java.persistence.Column.");
        throw new KeyProblemsException(erroMensage.toString());
    }

    /**
     * create a query to create table and return if exist a complex id or not
     * 
     * @param class1
     * @param cqlCreateTable
     * @param javaCassandra
     * @return
     */
    private boolean createQueryCreateTable(Class<?> class1,StringBuilder cqlCreateTable) {
        boolean isComplexID = false;
        for (Field field : ColumnUtil.INTANCE.listFields(class1)) {
            if (ColumnUtil.INTANCE.isEmbeddedField(field) || ColumnUtil.INTANCE.isEmbeddedIdField(field)) {

                isComplexID = createQueryCreateTable(field.getType(),  cqlCreateTable);
                if (ColumnUtil.INTANCE.isEmbeddedIdField(field)) {
                    isComplexID = true;
                }
                continue;
            }
            
            AddColumn addColumn=AddColumnUtil.INSTANCE.factory(field);
            cqlCreateTable.append(addColumn.addRow(field, RelationShipJavaCassandra.INSTANCE));
        }
        return isComplexID;
    }

    /**
     * Find if exists
     * REMARK edited by : Dinusha Nandika;
     */
    private void findIndex(Class<?> familyColumn, Session session) {
        List<Field> indexes = ColumnUtil.INTANCE.getIndexFields(familyColumn);
        if (indexes.size()==0) {
            return;
        }
        for (Field index : indexes) {
        	createIndex(familyColumn, session, index);
		}
    }

    /**
     * method to create index
     * @param familyColumn
     * @param session
     * @param index
     */
	private void createIndex(Class<?> familyColumn, Session session, Field index) {
		StringBuilder createIndexQuery;
		createIndexQuery = new StringBuilder();
		createIndexQuery.append("CREATE INDEX ");
		createIndexQuery.append(ColumnUtil.INTANCE.getColumnName(index)).append(" ON ");
		createIndexQuery.append(ColumnUtil.INTANCE.getColumnFamilyNameSchema(familyColumn));
		createIndexQuery.append(" (").append(ColumnUtil.INTANCE.getColumnName(index)).append(");");
		try {
			session.execute(createIndexQuery.toString());
		} catch (InvalidQueryException exception) {
			Logger.getLogger(FixColumnFamily.class.getName()).info("Index already exists");
		}
	}
}
