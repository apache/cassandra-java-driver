package com.datastax.driver.mapping;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Row;

abstract class EntityMapper {
    final EntityDefinition entityDef;

    EntityMapper(EntityDefinition entityDef) {
        this.entityDef = entityDef;
    }

    abstract Map<String, Object> entityToColumns(Object entity);

    abstract Object rowToEntity(Row row);
}
