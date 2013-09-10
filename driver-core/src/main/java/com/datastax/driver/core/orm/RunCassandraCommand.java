package com.datastax.driver.core.orm;

import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

public class RunCassandraCommand {
	
	private static final ConsistencyLevel DEFAULT_CASSANDRA_CL = ConsistencyLevel.ONE;
	
	public <T> T  insert(T bean,Session session,String keySpace){
		return insert(bean, session, keySpace,DEFAULT_CASSANDRA_CL);
	}
	
	public <T> T  insert(T bean,Session session,String keySpace,ConsistencyLevel consistencyLevel){
          new InsertQuery(keySpace).prepare(bean, session,consistencyLevel);
          return bean;
	}
	
	public <T> boolean insert(Iterable<T> beans,Session session,String keySpace){
		return insert(beans, session, keySpace, DEFAULT_CASSANDRA_CL);
	}
	
	public <T> boolean insert(Iterable<T> beans,Session session,String keySpace,ConsistencyLevel consistencyLevel){
		  new InsertQuery(keySpace).prepare(beans, session,consistencyLevel);
		return true;
	}
	
	public <T> List<T> findAll(Class<T> bean,Session session,String keySpace){
		return findAll(bean, session, keySpace,DEFAULT_CASSANDRA_CL);
	}
	
	public <T> List<T> findAll(Class<T> bean,Session session,String keySpace,ConsistencyLevel consistency){
		return new FindAllQuery(keySpace).listAll(bean, session,consistency);
	}
	
	public <T> T findByKey(Object key, Class<T> bean,Session session,String keySpace){
		return findByKey(key, bean, session, keySpace,DEFAULT_CASSANDRA_CL);
	}
	
	public <T> T findByKey(Object key, Class<T> bean,Session session,String keySpace,ConsistencyLevel consistency){
		return new FindByKeyQuery(keySpace).findByKey(key, bean, session,consistency);
	}
	
	public boolean deleteByKey(Object key, Class<?> bean,Session session,String keySpace){
		return deleteByKey(key, bean, session, keySpace,DEFAULT_CASSANDRA_CL);
	}
	
	public boolean deleteByKey(Object key, Class<?> bean,Session session,String keySpace,ConsistencyLevel consistency){
		return new DeleteQuery(keySpace).deleteByKey(key, bean, session,consistency);
	}
	
	
	public <T> boolean delete(T bean,Session session,String keySpace) {
		return delete(bean, session, keySpace,DEFAULT_CASSANDRA_CL);
	}
	public <T> boolean delete(T bean,Session session,String keySpace,ConsistencyLevel consistency) {
		return new DeleteQuery(keySpace).deleteByKey(bean, session,consistency);
	}
	
	public <K, T> boolean deleteByKey(Iterable<K> keys, Class<T> entity,Session session,String keySpace){
		return deleteByKey(keys, entity, session, keySpace,DEFAULT_CASSANDRA_CL);
	}
	public <K, T> boolean deleteByKey(Iterable<K> keys, Class<T> entity,Session session,String keySpace,ConsistencyLevel consistency){
		 new DeleteQuery(keySpace).deleteByKey(keys,entity, session,consistency);
		 return true;
	}
	
	public <T> boolean delete(Iterable<T> beans,Session session,String keySpace) {
		return delete(beans, session, keySpace,DEFAULT_CASSANDRA_CL);
	}
	public <T> boolean delete(Iterable<T> beans,Session session,String keySpace,ConsistencyLevel consistency) {
		return new DeleteQuery(keySpace).deleteByKey(beans, session,consistency);
	}
	
	
	
	
	  /**
     * Edited by Dinusha Nandika
     * Add indexName parameter 
     */
	public <T> List<T> findByIndex(String indexName,Object index, Class<T> bean,Session session,String keySpace){
		return findByIndex(indexName, index, bean, session, keySpace,DEFAULT_CASSANDRA_CL);
	}
	public <T> List<T> findByIndex(String indexName,Object index, Class<T> bean,Session session,String keySpace,ConsistencyLevel consistency){
		return new FindByIndexQuery(keySpace).findByIndex(indexName,index, bean, session,consistency);
	}
	
	public <T> List<T> findByIndex(Object index, Class<T> bean,Session session,String keySpace,ConsistencyLevel consistency){
		return new FindByIndexQuery(keySpace).findByIndex(index, bean, session,consistency);
	}
	public <T> List<T> findByIndex(Object index, Class<T> bean,Session session,String keySpace){
		return findByIndex(index, bean, session, keySpace,DEFAULT_CASSANDRA_CL);
	}
	
	public <T> Long count(Class<T> bean,Session session,String keySpace){
		return count(bean, session, keySpace,DEFAULT_CASSANDRA_CL);
	}
	
	public <T> Long count(Class<T> bean,Session session,String keySpace,ConsistencyLevel consistency){
		return new CountQuery(keySpace).count(bean, session,consistency);
	}
	
	public <K, T> List<T> findByKeys(Iterable<K> keys, Class<T> bean,Session session,String keySpace){
		return findByKeys(keys, bean, session, keySpace,DEFAULT_CASSANDRA_CL);
	}
	public <K, T> List<T> findByKeys(Iterable<K> keys, Class<T> bean,Session session,String keySpace,ConsistencyLevel consistency){
		List<T> beans = new LinkedList<T>();

		for (K key : keys) {
			T entity = findByKey(key, bean, session,keySpace,consistency);
			if (entity != null) {
				beans.add(entity);
			}
		}

		return beans;
	}
	
	public <T> void removeAll(Class<T> bean,Session session){
		 new RemoveAll().truncate(bean, session);
	}
}
