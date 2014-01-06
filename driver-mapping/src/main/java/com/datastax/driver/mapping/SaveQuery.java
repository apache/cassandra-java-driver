package com.datastax.driver.mapping;

import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;

import java.nio.ByteBuffer;
import java.util.Map;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;

/**
 * A query that performs an insert for the provided entity.
 */
class SaveQuery<T> extends Statement {
    private final EntityMapper<T> mapper;
    private final T entity;

    public SaveQuery(EntityMapper<T> mapper, T entity) {
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
        Insert insert = insertInto(mapper.keyspace, mapper.table);
        for (ColumnMapper<T> cm : mapper.allColumns())
            insert.value(cm.getColumnName(), cm.getValue(entity));

        return insert.getQueryString();
    }
}
