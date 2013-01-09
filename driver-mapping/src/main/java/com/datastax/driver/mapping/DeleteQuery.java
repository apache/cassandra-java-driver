package com.datastax.driver.mapping;

import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;

class DeleteQuery extends Statement{
    private final EntityMapper mapper;
    private final Map<String, Object> columns;

    public DeleteQuery(EntityMapper mapper, Object entity) {
        this.mapper = mapper;
        this.columns = mapper.entityToColumns(entity);
        setConsistencyLevel(mapper.entityDef.defaultWriteCL);
    }

    @Override
    public ByteBuffer getRoutingKey() {
        return null;
    }

    @Override
    public String getQueryString() {
        Delete delete = delete().all().from(mapper.entityDef.tableName);
        Delete.Where whereClause = delete.where();
        for (Entry<String, Object> entry : columns.entrySet()) {
            whereClause.and(eq(entry.getKey(), entry.getValue()));
        }
        return delete.getQueryString();
    }
}
