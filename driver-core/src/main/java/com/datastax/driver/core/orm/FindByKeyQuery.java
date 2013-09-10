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
import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.orm.exception.KeyProblemsException;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Class to execute query for find by id
 * 
 * @author otaviojava
 * 
 */
class FindByKeyQuery extends FindAllQuery {

    public FindByKeyQuery(String keySpace) {
		super(keySpace);
	}

	public <T> T findByKey(Object key, Class<T> bean, Session session,ConsistencyLevel consistency) {
    	QueryBean byKeyBean = createQueryBean(bean,consistency);
    	
        return executeConditions(key, bean, session, byKeyBean);
    }

    private <T> T executeConditions(Object key, Class<T> bean, Session session,QueryBean byKeyBean) {
        ResultSet resultSet = executeQuery(key, bean, session, byKeyBean);
        List<T> list = RecoveryObject.INTANCE.recoverObjet(bean, resultSet);
        if (!list.isEmpty()) {
            return list.get(0);
        }

        return null;
    }

    /**
     * execute the query and returns the result set
     * 
     * @param key
     *            - value of key
     * @param bean
     *            - bean represents column family
     * @param session
     * @param byKeyBean
     * @return
     */
    protected ResultSet executeQuery(Object key, Class<?> bean,Session session, QueryBean byKeyBean) {

        if (!key.getClass().equals(byKeyBean.key.getType())) {
            createKeyProblemMensage(key, byKeyBean);
        }
        if (ColumnUtil.INTANCE.isEmbeddedIdField(byKeyBean.key)) {
        	return executeComplexKey(key, session, byKeyBean);
        } else{
        	return executeSingleKey(key, session, byKeyBean); 
        }
    }

	private void createKeyProblemMensage(Object key, QueryBean byKeyBean) {
		StringBuilder erro = new StringBuilder();
		erro.append("The parameter key should be the same type of the key of column family, the type passed was ");
		erro.append(key.getClass().getName()).append(" and was expected ").append(byKeyBean.key.getType().getName());
		throw new KeyProblemsException(erro.toString());
	}

    /**
     * query with just one key in column family
     * 
     * @param key
     * @param session
     * @param byKeyBean
     * @return
     */
    protected ResultSet executeSingleKey(Object key, Session session, QueryBean byKeyBean) {
        byKeyBean.select.where(QueryBuilder.eq(ColumnUtil.INTANCE.getColumnName(byKeyBean.key), key));
        return session.execute(byKeyBean.select);

    }

    /**
     * query with complex query in column family
     * 
     * @param key
     * @param session
     * @param byKeyBean
     * @return
     */
    protected ResultSet executeComplexKey(Object key, Session session, QueryBean byKeyBean) {
        for(Field complexKey:ColumnUtil.INTANCE.listFields(key.getClass())){
        	 byKeyBean.select.where(QueryBuilder.eq(ColumnUtil.INTANCE.getColumnName(complexKey), ReflectionUtil.INSTANCE.getMethod(key, complexKey)));
        }
        return session.execute(byKeyBean.select);

    }
}
