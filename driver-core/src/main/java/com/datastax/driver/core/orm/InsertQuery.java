/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datastax.driver.core.orm;

import java.lang.reflect.Field;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.orm.ColumnUtil.KeySpaceInformation;
import com.datastax.driver.core.orm.InsertColumnUtil.InsertColumn;
import com.datastax.driver.core.orm.exception.KeyProblemsException;
import com.datastax.driver.core.orm.mapping.EmbeddedKey;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Class to mounts and runs the query to insert a row in a column family
 * 
 * @author otaviojava
 * 
 */
class InsertQuery {

	private String keySpace;
	
	InsertQuery(String keySpace){
		this.keySpace = keySpace;
	}
	
    public <T> boolean prepare(T bean, Session session,ConsistencyLevel consistency) {
    	
        session.execute(createStatment(bean,consistency));
        return true;
    }
    public <T> boolean prepare(Iterable<T> beans, Session session,ConsistencyLevel consistency) {
    	
    	Batch batch = null;
    	
    	for(T bean:beans){
    		if(batch == null){
    	 	 batch=QueryBuilder.batch(createStatment(bean,consistency));
    	 	}else{
    	 		batch.add(createStatment(bean,consistency));
    	 	}
    	}
    	session.execute(batch);
    	return true;
    }
    
    
	private <T> Statement createStatment(T bean, ConsistencyLevel consistency) {
		isKeyNull(bean);
		KeySpaceInformation key=ColumnUtil.INTANCE.getKeySpace(keySpace, bean.getClass());
        Insert insert=QueryBuilder.insertInto(key.getKeySpace(), key.getColumnFamily());
        insert=createInsert(bean, insert);
        insert.setConsistencyLevel(consistency);
       return insert;
	}

	
	private <T> Insert createInsert(T bean, Insert insert){
		
		 for (Field field : ColumnUtil.INTANCE.listFields(bean.getClass())) {
	            
	            if (ColumnUtil.INTANCE.isEmbeddedField(field) || ColumnUtil.INTANCE.isEmbeddedIdField(field)) {
	                if (ReflectionUtil.INSTANCE.getMethod(bean, field) != null) {
	                	insert = createInsert(ReflectionUtil.INSTANCE.getMethod(bean, field), insert);
	                }
	                continue;
	            }

	            else if (ReflectionUtil.INSTANCE.getMethod(bean, field) != null) {
	                InsertColumn insertColumn=InsertColumnUtil.INSTANCE.factory(field);
	                insert.value(ColumnUtil.INTANCE.getColumnName(field), insertColumn.getObject(bean, field));

	            }
	        }
		return insert;
	}
  
    /**
     * Verify if key is nut and make a exception
     * 
     * @param bean
     */
    private void isKeyNull(Object bean) {
        Field key = ColumnUtil.INTANCE.getKeyField(bean.getClass());
        if (key == null) {
            
            key = ColumnUtil.INTANCE.getField(bean.getClass(), EmbeddedKey.class);
            isKeyNull(ReflectionUtil.INSTANCE.getMethod(bean, key), key.getType().getDeclaredFields());
        } else {
            isKeyNull(bean, key);
        }

    }

    private void isKeyNull(Object bean, Field... fields) {
        for (Field field : fields) {
            if (ReflectionUtil.INSTANCE.getMethod(bean, field) == null) {
                throw new KeyProblemsException("Key is mandatory to insert a new column family");
            }
        }
    }


}
