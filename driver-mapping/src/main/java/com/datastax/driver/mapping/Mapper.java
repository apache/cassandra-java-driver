package com.datastax.driver.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;

public class Mapper {

    private volatile Map<Class<?>, EntityMapper> mappers = Collections.<Class<?>, EntityMapper>emptyMap();

    public <T> Result<T> map(ResultSet rs, Class<T> resultClass) {
        return new Result<T>(rs, getEntityMapper(resultClass));
    }

    public <T> Query save(T entity) {
        return new SaveQuery(getEntityMapper(entity.getClass()), entity);
    }

    public <T> Query find(T entity) {
        return new FindQuery(getEntityMapper(entity.getClass()), entity);
    }

    public <T> Query delete(T entity) {
        return new DeleteQuery(getEntityMapper(entity.getClass()), entity);
    }

    private EntityMapper getEntityMapper(Class<?> clazz) {
        EntityMapper mapper = mappers.get(clazz);
        if (mapper == null) {
            synchronized (mappers) {
                mapper = mappers.get(clazz);
                if (mapper == null) {
                    EntityDefinition def = AnnotationParser.parseEntity(clazz);
                    mapper = new ReflectionMapper(def);
                    Map<Class<?>, EntityMapper> newMappers = new HashMap<Class<?>, EntityMapper>(mappers);
                    newMappers.put(clazz, mapper);
                    mappers = newMappers;
                }
            }
        }
        return mapper;
    }
}
