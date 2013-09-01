package com.datastax.driver.core.orm;

import java.io.Serializable;
import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;

public interface CassandraORMOperations <T, K extends Serializable>{

	/**
	 * Saves a given entity. Use the returned instance for further operations as the save operation might have changed the
	 * entity instance completely.
	 * 
	 * @param entity
	 * @return the saved entity
	 */
	<S extends T> S save(S entity);
	
	<S extends T> S save(S entity,ConsistencyLevel consistency);

	/**
	 * Saves all given entities.
	 * 
	 * @param entities
	 * @return the saved entities
	 * @throws IllegalArgumentException in case the given entity is (@literal null}.
	 */
	<S extends T> Iterable<S> save(Iterable<S> entities);
	
	<S extends T> Iterable<S> save(Iterable<S> entities,ConsistencyLevel consistency);

	/**
	 * Retrives an entity by its id.
	 * 
	 * @param id must not be {@literal null}.
	 * @return the entity with the given id or {@literal null} if none found
	 * @throws IllegalArgumentException if {@code id} is {@literal null}
	 */
	T findOne(K id);
	
	T findOne(K id,ConsistencyLevel consistency);
	
	<I> List<T> findByIndex(I indexValue);

	<I> List<T> findByIndex(I indexValue,String indexName);
	
	<I> List<T> findByIndex(I indexValue,ConsistencyLevel consistency);

	<I> List<T> findByIndex(I indexValue,String indexName,ConsistencyLevel consistency);

	/**
	 * Returns whether an entity with the given id exists.
	 * 
	 * @param id must not be {@literal null}.
	 * @return true if an entity with the given id exists, alse otherwise
	 * @throws IllegalArgumentException if {@code id} is {@literal null}
	 */
	boolean exists(K id);
	
	boolean exists(K id,ConsistencyLevel consistency);

	/**
	 * Returns all instances of the type.
	 * 
	 * @return all entities
	 */
	Iterable<T> findAll();
	
	Iterable<T> findAll(ConsistencyLevel consistency);

	/**
	 * Returns all instances of the type with the given IDs.
	 * 
	 * @param ids
	 * @return
	 */
	Iterable<T> findAll(Iterable<K> ids);
	
	Iterable<T> findAll(Iterable<K> ids,ConsistencyLevel consistency);

	/**
	 * Returns the number of entities available.
	 * 
	 * @return the number of entities
	 */
	long count();
	
	long count(ConsistencyLevel consistency);

	/**
	 * Deletes the entity with the given id.
	 * 
	 * @param id must not be {@literal null}.
	 * @throws IllegalArgumentException in case the given {@code id} is {@literal null}
	 */
	void delete(K id);
	
	void delete(K id,ConsistencyLevel consistency);

	/**
	 * Deletes a given entity.
	 * 
	 * @param entity
	 * @throws IllegalArgumentException in case the given entity is (@literal null}.
	 */
	void delete(T entity);
	void delete(T entity,ConsistencyLevel consistency);

	/**
	 * Deletes the given entities.
	 * 
	 * @param entities
	 * @throws IllegalArgumentException in case the given {@link Iterable} is (@literal null}.
	 */
	void delete(Iterable<? extends T> entities);
	
	void delete(Iterable<? extends T> entities,ConsistencyLevel consistency);

	/**
	 * Deletes all entities managed by the repository.
	 */
	void deleteAll();
}


