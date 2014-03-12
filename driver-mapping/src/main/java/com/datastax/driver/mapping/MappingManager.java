package com.datastax.driver.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Mapping manager from which to obtain entity mappers.
 */
public class MappingManager {

    private final Session session;

    private volatile Map<Class<?>, Mapper<?>> mappers = Collections.<Class<?>, Mapper<?>>emptyMap();
    private volatile Map<Class<?>, Object> accessors = Collections.<Class<?>, Object>emptyMap();

    /**
     * Creates a new {@code MappingManager} using the provided {@code Session}.
     *
     * @param session the {@code Session} to use.
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
     * Creates a {@code Mapper} for the provided class (that must be annotated by a
     * {@link Table} annotation).
     * <p>
     * The {@code MappingManager} only ever keep one Mapper for each class, and so calling this
     * method multiple time on the same class will always return the same object.
     *
     * @param klass the (annotated) class for which to return the mapper.
     * @return the {@code Mapper} object for class {@code klass}.
     */
    public <T> Mapper<T> mapper(Class<T> klass) {
        return getMapper(klass);
    }

    /**
     * Creates an accessor object based on teh provided interface (that must be annotated by
     * a {@link Accessor} annotation).
     * <p>
     * The {@code MappingManager} only ever keep one Accessor for each class, and so calling this
     * method multiple time on the same class will always return the same object.
     *
     * @param klass the (annotated) class for which to create an accessor object.
     * @return the accessor object for class {@code klass}.
     */
    public <T> T createAccessor(Class<T> klass) {
        return getAccessor(klass);
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

    @SuppressWarnings("unchecked")
    private <T> T getAccessor(Class<T> klass) {
        T accessor = (T)accessors.get(klass);
        if (accessor == null) {
            synchronized (accessors) {
                accessor = (T)accessors.get(klass);
                if (accessor == null) {
                    AccessorMapper<T> mapper = AnnotationParser.parseAccessor(klass, AccessorReflectionMapper.factory());
                    mapper.prepare(this);
                    accessor = mapper.createProxy();
                    Map<Class<?>, Object> newAccessors = new HashMap<Class<?>, Object>(accessors);
                    newAccessors.put(klass, accessor);
                    accessors = newAccessors;
                }
            }
        }
        return accessor;
    }
}
