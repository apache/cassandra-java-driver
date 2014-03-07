package com.datastax.driver.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Mapping manager from which to obtain entity mappers.
 */
public class MappingManager {

    private final Session session;

    private volatile Map<Class<?>, Mapper<?>> mappers = Collections.<Class<?>, Mapper<?>>emptyMap();

    /**
     * Creates a new {@code MappingManager} using the provided {@code Session}.
     *
     * @param Session the {@code Session} to use. Note that the Mapping always use
     * fully qualified table names for its queries and thus whether or not the provided
     * session is connected to a keyspace has no effect on it whatsoever.
     */
    public MappingManager(Session session) {
        this.session = session;
    }

    /**
     * The underlying {@code Session} used by this manager.
     * <p>
     * Note that you can get obtain the {@code Cluster} object corresponding
     * to that session using {@code getSession().getCluster()}.
     * <p>
     * It is inadvisable to close the returned Session while this manager and
     * its mappers are in use.
     *
     * @return the underlying session used by this manager.
     */
    public Session getSession() {
        return session;
    }

    /**
     * Creates a {@code Mapper} for the provided class (that must be annotated properly).
     * <p>
     * The {@code MappingManager} only ever one Mapper for each class, and so calling this
     * method multiple time on the same class will always return the same object.
     *
     * @param klass the (annotated) class for which to return the mapper.
     * @return the {@code Mapper} object for class {@code klass}.
     */
    public <T> Mapper<T> mapper(Class<T> klass) {
        return getMapper(klass);
    }

    public <T> T createDAO(Class<T> klass) {
        DAOMapper<T> mapper = AnnotationParser.parseDAO(klass, DAOReflectionMapper.factory());
        mapper.prepare(this);
        return mapper.createProxy();
    }

    @SuppressWarnings("unchecked")
    private <T> Mapper<T> getMapper(Class<T> klass) {
        Mapper<T> mapper = (Mapper<T>)mappers.get(klass);
        if (mapper == null) {
            synchronized (mappers) {
                mapper = (Mapper<T>)mappers.get(klass);
                if (mapper == null) {
                    EntityMapper<T> entityMapper = AnnotationParser.parseEntity(klass, ReflectionMapper.factory());
                    mapper = new Mapper<T>(this, klass, entityMapper);
                    Map<Class<?>, Mapper<?>> newMappers = new HashMap<Class<?>, Mapper<?>>(mappers);
                    newMappers.put(klass, mapper);
                    mappers = newMappers;
                }
            }
        }
        return mapper;
    }
}
