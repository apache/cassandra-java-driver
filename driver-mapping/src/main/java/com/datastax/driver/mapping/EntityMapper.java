package com.datastax.driver.mapping;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Row;

abstract class EntityMapper<T> {
    final EntityDefinition<T> entityDef;

    EntityMapper(EntityDefinition<T> entityDef) {
        this.entityDef = entityDef;
    }

    abstract Map<String, Object> entityToColumns(T entity);

    abstract T rowToEntity(Row row);
}
