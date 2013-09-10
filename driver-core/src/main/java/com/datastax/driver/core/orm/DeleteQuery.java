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
import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.orm.ColumnUtil.KeySpaceInformation;
import com.datastax.driver.core.orm.exception.KeyProblemsException;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Class to create a query to delete beans
 * 
 * @author otaviojava
 * 
 */
class DeleteQuery{

	private String keySpace;
	
	public DeleteQuery(String keySpace){
	  this.keySpace = keySpace;	
	}
    public <T> boolean deleteByKey(T bean, Session session,ConsistencyLevel consistency) {
    	
        Field key = getKeyField(bean);
     
        return deleteByKey(ReflectionUtil.INSTANCE.getMethod(bean, key),bean.getClass(), session,consistency);
    }
    
    public <T> boolean deleteByKey(Iterable<T> beans, Session session,ConsistencyLevel consistency) {
    	
    	List<Object> keys=new LinkedList<Object>();
    	Class<?> beanClass=null;
    	for(T bean:beans){
    		
    		if(beanClass == null){
    			beanClass=bean.getClass();
    		}
    		
    	}
    	deleteByKey(keys, beanClass, session,consistency);
    	return true;
    }

    public <K> boolean deleteByKey(K key, Class<?> bean, Session session,ConsistencyLevel consistency) {
        Delete delete=runDelete(key, bean,consistency);
        session.execute(delete);
        return true;
    }
    
    public <K> boolean deleteByKey(Iterable<K> keys, Class<?> bean, Session session,ConsistencyLevel consistency) {
    	
    	Batch batch= null;
    	for(K key:keys){
    		if(batch ==null){
    			QueryBuilder.batch(runDelete(key, bean,consistency));
    		}else{
    			batch.add(batch);
    		}
    	}
        session.execute(batch);
        return true;
    }
    
	private Delete runDelete(Object key, Class<?> bean,ConsistencyLevel consistency) {
		if (key == null) {
            throw new KeyProblemsException("The parameter key to column family should be passed");
        }
        
        KeySpaceInformation keyInformation=ColumnUtil.INTANCE.getKeySpace(keySpace, bean);
    	Delete delete=QueryBuilder.delete().all().from(keyInformation.getKeySpace(), keyInformation.getColumnFamily());

        Field keyField = ColumnUtil.INTANCE.getKeyField(bean);
        if (keyField != null) {
        	
           delete.where(QueryBuilder.eq(ColumnUtil.INTANCE.getColumnName(keyField), key));  
        } else {
        	runComplexKey(delete, key);
        }
        delete.setConsistencyLevel(consistency);
        return delete;
	}
    
    
	private void runComplexKey( Delete delete, Object key) {
		
		for(Field subKey:ColumnUtil.INTANCE.listFields(key.getClass())){
		  delete.where(QueryBuilder.eq(ColumnUtil.INTANCE.getColumnName(subKey), ReflectionUtil.INSTANCE.getMethod(key, subKey)));
		}
	}

	private <T> Field getKeyField(T bean) {
		Field key = ColumnUtil.INTANCE.getKeyField(bean.getClass());
        if (key == null) {
            key = ColumnUtil.INTANCE.getKeyComplexField(bean.getClass());
        }
		return key;
	}
        
    
}
