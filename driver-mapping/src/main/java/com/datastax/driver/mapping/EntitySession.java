package com.datastax.driver.mapping;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;

/**
 * A {@code MappedSession} has the same role as a {@link Session} object in
 * the core package except that this one provides a different interface that
 * let the user manipulate some objects as input and output instead of 
 * {@link Query} or {@link CQLRow}.
 */
public class EntitySession {

	private static Map<Session, EntitySession> sessionMap = new HashMap<Session, EntitySession>();
	
	private final Session session;
	private volatile Map<Class<?>, Mapper> mappers;
	
	
	private EntitySession(Session session) {
		this.session = session;
		mappers = new HashMap<Class<?>, Mapper>();
		mappers.put(Void.class, new Mapper.VoidMapper());
	}

	/**
	 * Creates a new {@code EntitySession} on top of a given session. Only one
	 * {@code EntitySession} instance will be created per session, thus several
	 * calls of this method with the same session will always return the same
	 * instance.
	 *  
	 * @param session the session to be wrapped.
	 * @return an {@code EntitySession} object built on top of the given session.
	 */
	public static synchronized EntitySession create(Session session) {
		EntitySession entitySession = sessionMap.get(session);
		if (entitySession == null) {
			entitySession = new EntitySession(session);
			sessionMap.put(session, entitySession);
		}
		return entitySession;
	}


    /**
     * Execute the provided query.
     *
     * This method is a shortcut for {@code execute(new SimpleStatement(query))}.
     *
     * @param query the CQL query to execute.
     * @param resultClass the class of the entity to be returned.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     */
	public <T> Result<T> execute(String query, Class<?> resultClass) throws NoHostAvailableException {
		ResultSetFuture resultSetFuture = session.executeAsync(query);
		return new ResultFuture<T>(resultSetFuture, getMapper(resultClass)).getUninterruptibly();
	}

    /**
     * Execute the provided query.
     *
     * This method blocks until at least some result has been received from the
     * database. However, for SELECT queries, it does not guarantee that the
     * result has been received in full. But it does guarantee that some
     * response has been received from the database, and in particular
     * guarantee that if the request is invalid, an exception will be thrown
     * by this method.
     *
     * @param query the CQL query to execute (that can be either a {@code
     * Statement} or a {@code BoundStatement}). If it is a {@code
     * BoundStatement}, all variables must have been bound (the statement must
     * be ready).
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     * @throws IllegalStateException if {@code query} is a {@code BoundStatement}
     * but {@code !query.isReady()}.
     */
	public <T> Result<T> execute(Query query, Class<?> resultClass) throws NoHostAvailableException {
		ResultSetFuture resultSetFuture = session.executeAsync(query);
		return new ResultFuture<T>(resultSetFuture, getMapper(resultClass)).getUninterruptibly();
	}	
	
    /**
     * Execute the provided query asynchronously.
     *
     * This method is a shortcut for {@code executeAsync(new SimpleStatement(query))}.
     *
     * @param query the CQL query to execute.
     * @return a future on the result of the query.
     */	
	public <T> ResultFuture<T> executeAsync(String query, Class<?> resultClass) {
		ResultSetFuture resultSetFuture = session.executeAsync(query);
		return new ResultFuture<T>(resultSetFuture, getMapper(resultClass));
	}

    /**
     * Execute the provided query asynchronously.
     *
     * This method does not block. It returns as soon as the query has been
     * successfully sent to a Cassandra node. In particular, returning from
     * this method does not guarantee that the query is valid. Any exception
     * pertaining to the failure of the query will be thrown by the first
     * access to the {@link ResultSet}.
     *
     * Note that for queries that doesn't return a result (INSERT, UPDATE and
     * DELETE), you will need to access the ResultSet (i.e. call any of its
     * method) to make sure the query was successful.
     *
     * @param query the CQL query to execute (that can be either a {@code
     * Statement} or a {@code BoundStatement}). If it is a {@code
     * BoundStatement}, all variables must have been bound (the statement must
     * be ready).
     * @return a future on the result of the query.
     *
     * @throws IllegalStateException if {@code query} is a {@code BoundStatement}
     * but {@code !query.isReady()}.
     */
	public <T> ResultFuture<T> executeAsync(Query query, Class<?> resultClass) {
		ResultSetFuture resultSetFuture = session.executeAsync(query);
		return new ResultFuture<T>(resultSetFuture, getMapper(resultClass));
	}

	/**
	 * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     * @throws IllegalStateException if {@code query} is a {@code BoundStatement}
     * but {@code !query.isReady()}.
     */
	public <T> Result<Void> save(T entity) throws NoHostAvailableException {
		return saveAsync(entity).getUninterruptibly();
	}

    /**
     * Save the provided entity asynchronously.
     *
     * This method is a shortcut for {@code executeAsync(new SaveQuery(entitySessin, entity))}.
     *
     * @param entity the entity to save.
     * @return a future on the result of the query.
     */	
	public <T> ResultFuture<Void> saveAsync(T entity) {
		return executeAsync(new SaveQuery(this, entity), Void.class);
	}

	/**
	 * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     * @throws IllegalStateException if {@code query} is a {@code BoundStatement}
     * but {@code !query.isReady()}.
     */
	public <T> Result<Void> delete(T entity) throws NoHostAvailableException {
		return deleteAsync(entity).getUninterruptibly();
	}

    /**
     * Delete the provided entity asynchronously.
     *
     * This method is a shortcut for {@code executeAsync(new DeleteQuery(entitySessin, entity), Void.class)}.
     *
     * @param entity the entity to delete.
     * @return a future on the result of the query.
     */	
	public <T> ResultFuture<Void> deleteAsync(T entity) {
		return executeAsync(new DeleteQuery(this, entity), Void.class);
	}

	/**
	 * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     * @throws IllegalStateException if {@code query} is a {@code BoundStatement}
     * but {@code !query.isReady()}.
     */
	public <T> Result<T> find(T example, Class<T> entityClass) throws NoHostAvailableException {
		return findAsync(example, entityClass).getUninterruptibly();
	}

    /**
     * Performs an asynchronous find by example query.
     *
     * This method is a shortcut for {@code executeAsync(new FindQuery(entitySessin, entity), entityClass)}.
     *
     * @param entity the entity to save.
     * @return a future on the result of the query.
     */	
	public <T> ResultFuture<T> findAsync(T example, Class<T> entityClass) {
		return executeAsync(new FindQuery(this, example), entityClass);
	}

	
    /**
     * Prepare the provided query.
     *
     * @param query the CQL query to prepare
     * @return the prepared statement corresponding to {@code query}.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     */	
	public PreparedStatement prepare(String query)
			throws NoHostAvailableException {
		return session.prepare(query);
	}

    /**
     * Shutdown the underlying session instance. All other {@link EntitySession}
     * that are built on the same session will be shutdown.
     * <p>
     * This closes all connections used by this sessions. Note that if you want
     * to shutdown the full {@code Cluster} instance this session is part of,
     * you should use {@link Cluster#shutdown} instead (which will call this
     * method for all session but also release some additional resources).
     * <p>
     * This method has no effect if the session was already shutdown.
     */	
	public void shutdown() {
		session.shutdown();
	}

    /**
     * Returns the {@code Cluster} object this session is part of.
     */
	public Cluster getCluster() {
		return session.getCluster();
	}
	
	/**
	 * Returns the underlying {@link Session} object on which this
	 * {@code EntitySession} has been built.
	 */
	public Session getSession() {
		return session;
	}
	
	Mapper getMapper(Class<?> clazz) {
		Mapper mapper = mappers.get(clazz);
		if (mapper == null) {
			synchronized (mappers) {
				mapper = mappers.get(clazz);
				if (mapper == null) {
					EntityDefinition def = AnnotationParser.parseEntity(clazz);
					mapper = new ReflectionMapper(def);
					Map<Class<?>, Mapper> newMappers = new HashMap<Class<?>, Mapper>(mappers);
					newMappers.put(clazz, mapper);
					mappers = newMappers;
				}
			}
		}
		return mapper;
	}
}
