package com.datastax.driver.mapping;

import static com.datastax.driver.core.utils.querybuilder.Clause.eq;
import static com.datastax.driver.core.utils.querybuilder.QueryBuilder.all;
import static com.datastax.driver.core.utils.querybuilder.QueryBuilder.delete;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.querybuilder.Clause;
import com.datastax.driver.core.utils.querybuilder.Delete;
import com.datastax.driver.core.utils.querybuilder.Select;

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
        Clause[] clauses = new Clause[columns.size()];
        int i = 0;
        for (Entry<String, Object> entry : columns.entrySet()) {
            clauses[i++] = eq(entry.getKey(), entry.getValue());
        }
        Delete delete = delete(all()).from(mapper.entityDef.tableName).where(clauses);
        return delete.getQueryString();
    }
}
