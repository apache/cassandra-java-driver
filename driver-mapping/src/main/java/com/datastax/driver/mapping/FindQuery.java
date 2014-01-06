package com.datastax.driver.mapping;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;

/**
 * A query that performs a find by example using the entity provided as constructor.
 */
class FindQuery<T> extends Statement {

    private final EntityMapper<T> mapper;
    private final T entity;

    public FindQuery(EntityMapper<T> mapper, T entity) {
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
        Select select = select().all().from(mapper.keyspace, mapper.table);
        Select.Where whereClause = select.where();
        for (ColumnMapper<T> cm : mapper.partitionKeys)
            whereClause.and(eq(cm.columnName, cm.getValue(entity)));
        for (ColumnMapper<T> cm : mapper.clusteringColumns)
            whereClause.and(eq(cm.columnName, cm.getValue(entity)));
        return select.getQueryString();
    }
}
