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
import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.orm.exception.IndexProblemException;

/**
 * Class to execute a index
 * 
 * @author otaviojava
 * 
 */
class FindByIndexQuery extends FindByKeyQuery {

	public FindByIndexQuery(String keySpace) {
		super(keySpace);
	}

	public <T,I> List<T> findByIndex(I index, Class<T> bean, Session session,ConsistencyLevel consistency) {
		Field field=ColumnUtil.INTANCE.getIndexField(bean);
		checkFieldNull(bean, field);
		return findByIndex(field.getName(),index, bean, session,consistency);
	}

	private <T> void checkFieldNull(Class<T> bean, Field field) {
		if(field==null){
			StringBuffer erro=new StringBuffer();
			erro.append("No found some field with @com.datastax.driver.core.orm.mapping.Index within ");
			erro.append(bean.getName()).append(" class.");
			throw new IndexProblemException(erro.toString());
		}
	}
	
    public <T,I> List<T> findByIndex(String indexName, I key, Class<T> bean, Session session,ConsistencyLevel consistency) {
    	QueryBean byKeyBean = createQueryBean(bean,consistency);
        return executeConditions(indexName,key, bean, session, byKeyBean);
    }

 
    private <T> List<T> executeConditions(String indexName, Object key, Class<T> bean,Session session, QueryBean byKeyBean) {
    	  /**
         * Edited by Dinusha Nandika
         * Add indexName parameter 
         */
    	byKeyBean.key = ColumnUtil.INTANCE.getFieldByColumnName(indexName,bean);
        checkIndexProblem(bean, byKeyBean);
        ResultSet resultSet = executeQuery(key, bean, session, byKeyBean);
        return RecoveryObject.INTANCE.recoverObjet(bean, resultSet);
    }

	private <T> void checkIndexProblem(Class<T> bean, QueryBean byKeyBean) {
		if (byKeyBean.key == null) {
            StringBuilder erro = new StringBuilder();
            erro.append("Some field in a class ").append(bean.getName());
            erro.append(" should be a annotation: @com.datastax.driver.core.orm.mapping.Index ");
            throw new IndexProblemException(erro.toString());
        }
	}
}
