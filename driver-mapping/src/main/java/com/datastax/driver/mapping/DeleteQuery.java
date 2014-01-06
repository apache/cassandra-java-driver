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

class DeleteQuery<T> extends Statement{

    private final EntityMapper<T> mapper;
    private final T entity;

    public DeleteQuery(EntityMapper<T> mapper, T entity) {
        this.mapper = mapper;
        this.entity = entity;
    }

    @Override
    public ByteBuffer getRoutingKey() {
        // TODO
        return null;
    }

    @Override
    public String getQueryString() {
        Delete delete = delete().all().from(mapper.keyspace, mapper.table);
        Delete.Where whereClause = delete.where();
        for (ColumnMapper<T> cm : mapper.partitionKeys)
            whereClause.and(eq(cm.columnName, cm.getValue(entity)));
        for (ColumnMapper<T> cm : mapper.clusteringColumns)
            whereClause.and(eq(cm.columnName, cm.getValue(entity)));
        return delete.getQueryString();
    }
}
