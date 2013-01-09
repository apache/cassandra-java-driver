package com.datastax.driver.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;

/**
 * A mapper that maps Cassandra rows to annoted entities.
 *
 * This object is thread safe and you are in fact encouraged to create a single
 * Mapper for your application and to share that object as this minimize ressources.
 */
public class Mapper {

    private volatile Map<Class<?>, EntityMapper<?>> mappers = Collections.<Class<?>, EntityMapper<?>>emptyMap();

    /**
     * Creates a new {@code Mapper}.
     */
    public Mapper() {}

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
    public <T> Query save(T entity) {
        return new SaveQuery(getEntityMapper(entity.getClass()), entity);
    }

    public <T> Query find(T entity) {
        return new FindQuery(getEntityMapper(entity.getClass()), entity);
    }

    public <T> Query delete(T entity) {
        return new DeleteQuery(getEntityMapper(entity.getClass()), entity);
    }

    @SuppressWarnings("unchecked")
    private <T> EntityMapper<T> getEntityMapper(Class<T> clazz) {
        EntityMapper<T> mapper = (EntityMapper<T>)mappers.get(clazz);
        if (mapper == null) {
            synchronized (mappers) {
                mapper = (EntityMapper<T>)mappers.get(clazz);
                if (mapper == null) {
                    EntityDefinition<T> def = AnnotationParser.parseEntity(clazz);
                    mapper = new ReflectionMapper<T>(def);
                    Map<Class<?>, EntityMapper<?>> newMappers = new HashMap<Class<?>, EntityMapper<?>>(mappers);
                    newMappers.put(clazz, mapper);
                    mappers = newMappers;
                }
            }
        }
        return mapper;
    }
}
