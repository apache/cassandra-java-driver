package com.datastax.driver.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * A mapper that maps Cassandra rows to annoted entities.
 *
 * This object is thread safe and you are in fact encouraged to create a single
 * Mapper for your application and to share that object as this minimize ressources.
 */
public class Mapper {

    private volatile Map<Class<?>, EntityMapper<?>> mappers = Collections.<Class<?>, EntityMapper<?>>emptyMap();

    private final Session session;

    /**
     * Creates a new {@code Mapper}.
     */
    public Mapper(Session session) {
        this.session = session;
    }

    /**
     * Map the rows from a {@code ResultSet} into the provided entity class.
     *
     * @param resultSet the {@code ResultSet} to map.
     * @param resultClass the entity class to which map the rows from {@code resultSet}.
     * @return the mapped result set. Note that the returned mapped result set
     * will encapsulate {@code resultSet} and so consuming results from this
     * returned mapped result set will consume results from {@code resultSet}
     * and vice-versa.
     */
    public <T> Result<T> map(ResultSet resultSet, Class<T> resultClass) {
        return new Result<T>(resultSet, getEntityMapper(resultClass));
    }

    /**
     * Creates a query that saves the provided entity.
     *
     * @param entity the entity to save.
     * @return a query that saves {@code entity} (based on it's defined mapping).
     */
    @SuppressWarnings("unchecked")
    public <T> Statement save(T entity) {
        EntityMapper<T> mapper = getEntityMapper((Class<T>)entity.getClass());
        PreparedStatement ps = mapper.getPreparedQuery(session, EntityMapper.QueryType.SAVE);

        BoundStatement bs = ps.bind();
        for (ColumnMapper<T> cm : mapper.allColumns()) {
            Object value = cm.getValue(entity);
            bs.setBytesUnsafe(cm.getColumnName(), value == null ? null : cm.getDataType().serialize(value));
        }
        return bs;
    }

    public <T> Statement get(Class<T> klass, Object... primaryKey) {
        EntityMapper<T> mapper = getEntityMapper(klass);
        if (primaryKey.length != mapper.primaryKeySize())
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d", mapper.primaryKeySize(), primaryKey.length));

        PreparedStatement ps = mapper.getPreparedQuery(session, EntityMapper.QueryType.GET);

        BoundStatement bs = ps.bind();
        for (int i = 0; i < primaryKey.length; i++) {
            ColumnMapper<T> column = mapper.getPrimaryKeyColumn(i);
            Object value = primaryKey[i];
            if (value == null)
                throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)", column.getColumnName(), i));
            bs.setBytesUnsafe(column.getColumnName(), column.getDataType().serialize(value));
        }
        return bs;
    }

    public <T> Statement delete(Class<T> klass, Object...primaryKey) {
        EntityMapper<T> mapper = getEntityMapper(klass);
        if (primaryKey.length != mapper.primaryKeySize())
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d", mapper.primaryKeySize(), primaryKey.length));

        PreparedStatement ps = mapper.getPreparedQuery(session, EntityMapper.QueryType.DEL);

        BoundStatement bs = ps.bind();
        for (int i = 0; i < primaryKey.length; i++) {
            ColumnMapper<T> column = mapper.getPrimaryKeyColumn(i);
            Object value = primaryKey[i];
            if (value == null)
                throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)", column.getColumnName(), i));
            bs.setBytesUnsafe(column.getColumnName(), column.getDataType().serialize(value));
        }
        return bs;
    }

    public <T> Select select(Class<T> klass) {
        EntityMapper<T> mapper = getEntityMapper(klass);
        return QueryBuilder.select().all().from(mapper.keyspace, mapper.table);
    }

    @SuppressWarnings("unchecked")
    private <T> EntityMapper<T> getEntityMapper(Class<T> klass) {
        EntityMapper<T> mapper = (EntityMapper<T>)mappers.get(klass);
        if (mapper == null) {
            synchronized (mappers) {
                mapper = (EntityMapper<T>)mappers.get(klass);
                if (mapper == null) {
                    mapper = AnnotationParser.parseEntity(klass, ReflectionMapper.factory());
                    Map<Class<?>, EntityMapper<?>> newMappers = new HashMap<Class<?>, EntityMapper<?>>(mappers);
                    newMappers.put(klass, mapper);
                    mappers = newMappers;
                }
            }
        }
        return mapper;
    }
}
