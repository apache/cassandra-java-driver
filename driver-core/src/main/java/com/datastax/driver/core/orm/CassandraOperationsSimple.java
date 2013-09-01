package com.datastax.driver.core.orm;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;
/**
 * class to do simple operations by ORM
 * @author otaviojava
 * @param <T> - the entity
 * @param <K> - the id or row key
 */
public  abstract class CassandraOperationsSimple <T,K extends Serializable> implements CassandraORMOperations<T, K> { 

	protected  abstract Persistence getCassandraPersistence();
	
	private Class<T> beanClass;
	
	@Override
	public <S extends T> S save(S entity) {
		 getCassandraPersistence().save(entity);
		 return entity;
	}
	
	@Override
	public <S extends T> S save(S entity,ConsistencyLevel consistency) {
		 getCassandraPersistence().save(entity,consistency);
		 return entity;
	}

	@Override
	public <S extends T> Iterable<S> save(Iterable<S> entities) {
		 getCassandraPersistence().save(entities);
		 return entities;
	}
	
	@Override
	public <S extends T> Iterable<S> save(Iterable<S> entities,ConsistencyLevel consistency) {
		 getCassandraPersistence().save(entities,consistency);
		 return entities;
	}

	@Override
	public T findOne(K id) {
		return getCassandraPersistence().findByKey(id, beanClass);
	}
	
	@Override
	public T findOne(K id,ConsistencyLevel consistency) {
		return getCassandraPersistence().findByKey(id, beanClass,consistency);
	}
	

	@Override
	public <I> List<T> findByIndex(I indexValue) {
		return getCassandraPersistence().findByIndex(beanClass,indexValue);
	}

	@Override
	public <I> List<T> findByIndex(I indexValue,String indexName) {
		
		return getCassandraPersistence().findByIndex(beanClass,indexValue,indexName);
	}

	@Override
	public <I> List<T> findByIndex(I indexValue,ConsistencyLevel consistency) {
		
		return getCassandraPersistence().findByIndex(beanClass,indexValue,consistency);
	}

	@Override
	public <I> List<T> findByIndex( I indexValue,String indexName, ConsistencyLevel consistency) {
		return getCassandraPersistence().findByIndex(beanClass, indexValue, consistency, indexName);
	}


	
	@Override
	public boolean exists(K id) {
		
		return findOne(id) != null;
	}
	@Override
	public boolean exists(K id,ConsistencyLevel consistency) {
		
		return findOne(id,consistency) != null;
	}

	@Override
	public Iterable<T> findAll() {
		return getCassandraPersistence().findAll(beanClass);
	}
	
	@Override
	public Iterable<T> findAll(ConsistencyLevel consistency) {
		return getCassandraPersistence().findAll(beanClass,consistency);
	}

	@Override
	public Iterable<T> findAll(Iterable<K> ids) {
		return getCassandraPersistence().findByKeys(ids, beanClass);
	}

	@Override
	public Iterable<T> findAll(Iterable<K> ids,ConsistencyLevel consistency) {
		return getCassandraPersistence().findByKeys(ids, beanClass,consistency);
	}
	
	@Override
	public long count() {
		return getCassandraPersistence().count(beanClass);
	}
	
	@Override
	public long count(ConsistencyLevel consistency) {
		return getCassandraPersistence().count(beanClass,consistency);
	}

	@Override
	public void delete(K id) {
		getCassandraPersistence().deleteByKey(id, beanClass);
	}
	
	@Override
	public void delete(K id,ConsistencyLevel consistency) {
		getCassandraPersistence().deleteByKey(id, beanClass,consistency);
	}

	@Override
	public void delete(T entity) {
		getCassandraPersistence().delete(entity);
		
	}
	
	@Override
	public void delete(T entity,ConsistencyLevel consistency) {
		getCassandraPersistence().delete(entity,consistency);
		
	}
	
	@Override
	public void delete(Iterable<? extends T> entities) {
		getCassandraPersistence().delete(entities);		
	}

	@Override
	public void delete(Iterable<? extends T> entities,ConsistencyLevel consistency) {
		getCassandraPersistence().delete(entities,consistency);
		
	}
	

	@Override
	public void deleteAll() {
		getCassandraPersistence().deleteAll(beanClass);
	}
	
	@SuppressWarnings("unchecked")
	public CassandraOperationsSimple() {
		ParameterizedType genericType=(ParameterizedType)this.getClass().getGenericSuperclass();
		this.beanClass=(Class<T>) genericType.getActualTypeArguments()[0];
	}

	
}
