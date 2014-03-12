package com.datastax.driver.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

/**
 * An object handling the mapping of a particular class.
 * <p>
 * A {@code Mapper} object is obtained from a {@code MappingManager} using the
 * {@link MappingManager#mapper} method.
 */
public class Mapper<T> {

    private static final Logger logger = LoggerFactory.getLogger(EntityMapper.class);

    final MappingManager manager;
    final Class<T> klass;
    final EntityMapper<T> mapper;
    final TableMetadata tableMetadata;

    // Cache prepared statements for each type of query we use.
    private volatile Map<QueryType, PreparedStatement> preparedQueries = Collections.<QueryType, PreparedStatement>emptyMap();

    private static final Function<Object, Void> NOOP = Functions.<Void>constant(null);

    final Function<ResultSet, T> mapOneFunction;
    final Function<ResultSet, Result<T>> mapAllFunction;

    Mapper(MappingManager manager, Class<T> klass, EntityMapper<T> mapper) {
        this.manager = manager;
        this.klass = klass;
        this.mapper = mapper;

        KeyspaceMetadata keyspace = session().getCluster().getMetadata().getKeyspace(mapper.getKeyspace());
        this.tableMetadata = keyspace == null ? null : keyspace.getTable(Metadata.quote(mapper.getTable()));

        this.mapOneFunction = new Function<ResultSet, T>() {
            public T apply(ResultSet rs) {
                return Mapper.this.map(rs).one();
            }
        };
        this.mapAllFunction = new Function<ResultSet, Result<T>>() {
            public Result<T> apply(ResultSet rs) {
                return Mapper.this.map(rs);
            }
        };
    }

    Session session() {
        return manager.getSession();
    }

    PreparedStatement getPreparedQuery(QueryType type) {
        PreparedStatement stmt = preparedQueries.get(type);
        if (stmt == null) {
            synchronized (preparedQueries) {
                stmt = preparedQueries.get(type);
                if (stmt == null) {
                    String query = type.makePreparedQueryString(tableMetadata, mapper);
                    logger.debug("Preparing query {}", query);
                    stmt = session().prepare(query);
                    Map<QueryType, PreparedStatement> newQueries = new HashMap<QueryType, PreparedStatement>(preparedQueries);
                    newQueries.put(type, stmt);
                    preparedQueries = newQueries;
                }
            }
        }
        return stmt;
    }

    /**
     * The {@code MappingManager} managing this mapper.
     *
     * @return the {@code MappingManager} managing this mapper.
     */
    public MappingManager getManager() {
        return manager;
    }

    /**
     * Creates a query that can be used to save the provided entity.
     * <p>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #save}
     * or {@link #saveAsync} is shorter.
     *
     * @param entity the entity to save.
     * @return a query that saves {@code entity} (based on it's defined mapping).
     */
    public Statement saveQuery(T entity) {
        PreparedStatement ps = getPreparedQuery(QueryType.SAVE);

        BoundStatement bs = ps.bind();
        int i = 0;
        for (ColumnMapper<T> cm : mapper.allColumns()) {
            Object value = cm.getValue(entity);
            bs.setBytesUnsafe(i++, value == null ? null : cm.getDataType().serialize(value));
        }

        if (mapper.writeConsistency != null)
            bs.setConsistencyLevel(mapper.writeConsistency);
        return bs;
    }

    /**
     * Save an entity mapped by this mapper.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().execute(saveQuery(entity))}.
     *
     * @param entity the entity to save.
     */
    public void save(T entity) {
        session().execute(saveQuery(entity));
    }

    /**
     * Save an entity mapped by this mapper asynchonously.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(saveQuery(entity))}.
     *
     * @param entity the entity to save.
     * @return a future on the completion of the save operation.
     */
    public ListenableFuture<Void> saveAsync(T entity) {
        return Futures.transform(session().executeAsync(saveQuery(entity)), NOOP);
    }

    /**
     * Creates a query that can be used to delete the provided entity.
     * <p>
     * This method is a shortcut that extract the PRIMARY KEY from the
     * provided entity and call {@link #deleteQuery(Object...)} with it.
     * <p>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #delete}
     * or {@link #deleteAsync} is shorter.
     *
     * @param entity the entity to delete.
     * @return a query that delete {@code entity} (based on it's defined mapping).
     */
    public Statement deleteQuery(T entity) {
        Object[] pks = new Object[mapper.primaryKeySize()];
        for (int i = 0; i < pks.length; i++)
            pks[i] = mapper.getPrimaryKeyColumn(i).getValue(entity);

        return deleteQuery(pks);
    }

    /**
     * Creates a query that can be used to delete an entity given its PRIMARY KEY.
     * <p>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #delete}
     * or {@link #deleteAsync} is shorter.
     *
     * @param primaryKey the primary key of the entity to delete, or more precisely
     * the values for the columns of said primary key in the order of the primary key.
     * @return a query that delete the entity of PRIMARY KEY {@code primaryKey}.
     *
     * @throws IllegalArgumentException if the number of value provided differ from
     * the number of columns composing the PRIMARY KEY of the mapped class, or if
     * at least one of those values is {@code null}.
     */
    public Statement deleteQuery(Object...primaryKey) {
        if (primaryKey.length != mapper.primaryKeySize())
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d", mapper.primaryKeySize(), primaryKey.length));

        PreparedStatement ps = getPreparedQuery(QueryType.DEL);

        BoundStatement bs = ps.bind();
        for (int i = 0; i < primaryKey.length; i++) {
            ColumnMapper<T> column = mapper.getPrimaryKeyColumn(i);
            Object value = primaryKey[i];
            if (value == null)
                throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)", column.getColumnName(), i));
            bs.setBytesUnsafe(i, column.getDataType().serialize(value));
        }

        if (mapper.writeConsistency != null)
            bs.setConsistencyLevel(mapper.writeConsistency);
        return bs;
    }

    /**
     * Deletes an entity mapped by this mapper.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().execute(deleteQuery(entity))}.
     *
     * @param entity the entity to delete.
     */
    public void delete(T entity) {
        session().execute(deleteQuery(entity));
    }

    /**
     * Deletes an entity mapped by this mapper asynchronously.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(deleteQuery(entity))}.
     *
     * @param entity the entity to delete.
     */
    public ListenableFuture<Void> deleteAsync(T entity) {
        return Futures.transform(session().executeAsync(deleteQuery(entity)), NOOP);
    }

    /**
     * Deletes an entity based on its primary key.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().execute(deleteQuery(primaryKey))}.
     *
     * @param primaryKey the primary key of the entity to delete, or more precisely
     * the values for the columns of said primary key in the order of the primary key.
     *
     * @throws IllegalArgumentException if the number of value provided differ from
     * the number of columns composing the PRIMARY KEY of the mapped class, or if
     * at least one of those values is {@code null}.
     */
    public void delete(Object... primaryKey) {
        session().execute(deleteQuery(primaryKey));
    }

    /**
     * Deletes an entity based on its primary key asynchronously.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(deleteQuery(primaryKey))}.
     *
     * @param primaryKey the primary key of the entity to delete, or more precisely
     * the values for the columns of said primary key in the order of the primary key.
     *
     * @throws IllegalArgumentException if the number of value provided differ from
     * the number of columns composing the PRIMARY KEY of the mapped class, or if
     * at least one of those values is {@code null}.
     */
    public ListenableFuture<Void> deleteAsync(Object... primaryKey) {
        return Futures.transform(session().executeAsync(deleteQuery(primaryKey)), NOOP);
    }

    /**
     * Map the rows from a {@code ResultSet} into the class this is mapper of.
     *
     * @param resultSet the {@code ResultSet} to map.
     * @return the mapped result set. Note that the returned mapped result set
     * will encapsulate {@code resultSet} and so consuming results from this
     * returned mapped result set will consume results from {@code resultSet}
     * and vice-versa.
     */
    public Result<T> map(ResultSet resultSet) {
        return new Result<T>(resultSet, mapper);
    }

    /**
     * Creates a query to fetch entity given its PRIMARY KEY.
     * <p>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually,
     * but in other cases, calling {@link #get} or {@link #getAsync} is shorter.
     *
     * @param primaryKey the primary key of the entity to fetch, or more precisely
     * the values for the columns of said primary key in the order of the primary key.
     * @return a query that fetch the entity of PRIMARY KEY {@code primaryKey}.
     *
     * @throws IllegalArgumentException if the number of value provided differ from
     * the number of columns composing the PRIMARY KEY of the mapped class, or if
     * at least one of those values is {@code null}.
     */
    public Statement getQuery(Object... primaryKey) {
        if (primaryKey.length != mapper.primaryKeySize())
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d", mapper.primaryKeySize(), primaryKey.length));

        PreparedStatement ps = getPreparedQuery(QueryType.GET);

        BoundStatement bs = ps.bind();
        for (int i = 0; i < primaryKey.length; i++) {
            ColumnMapper<T> column = mapper.getPrimaryKeyColumn(i);
            Object value = primaryKey[i];
            if (value == null)
                throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)", column.getColumnName(), i));
            bs.setBytesUnsafe(i, column.getDataType().serialize(value));
        }

        if (mapper.readConsistency != null)
            bs.setConsistencyLevel(mapper.readConsistency);
        return bs;
    }

    /**
     * Fetch an entity based on its primary key.
     * <p>
     * This method is basically equivalent to: {@code map(getManager().getSession().execute(getQuery(primaryKey))).one()}.
     *
     * @param primaryKey the primary key of the entity to fetch, or more precisely
     * the values for the columns of said primary key in the order of the primary key.
     * @return the entity fetched or {@code null} if it doesn't exist.
     *
     * @throws IllegalArgumentException if the number of value provided differ from
     * the number of columns composing the PRIMARY KEY of the mapped class, or if
     * at least one of those values is {@code null}.
     */
    public T get(Object... primaryKey) {
        return map(session().execute(getQuery(primaryKey))).one();
    }

    /**
     * Fetch an entity based on its primary key asynchronously.
     * <p>
     * This method is basically equivalent to mapping the result of: {@code getManager().getSession().executeAsync(getQuery(primaryKey))}.
     *
     * @param primaryKey the primary key of the entity to fetch, or more precisely
     * the values for the columns of said primary key in the order of the primary key.
     * @return a future on the fetched entity. The return future will yield
     * {@code null} if said entity doesn't exist.
     *
     * @throws IllegalArgumentException if the number of value provided differ from
     * the number of columns composing the PRIMARY KEY of the mapped class, or if
     * at least one of those values is {@code null}.
     */
    public ListenableFuture<T> getAsync(Object... primaryKey) {
        return Futures.transform(session().executeAsync(getQuery(primaryKey)), mapOneFunction);
    }
}
