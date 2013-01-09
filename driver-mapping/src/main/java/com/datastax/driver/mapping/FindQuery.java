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
 *
 */
class FindQuery extends Statement {
    private final EntityMapper mapper;
    private final Map<String, Object> columns;

    public FindQuery(EntityMapper mapper, Object entity) {
        this.mapper = mapper;
        this.columns = mapper.entityToColumns(entity);
        setConsistencyLevel(mapper.entityDef.defaultReadCL);
    }

    @Override
    public ByteBuffer getRoutingKey() {
        return null;
    }

    @Override
    public String getQueryString() {
        Select select = select().all().from(mapper.entityDef.tableName);
        Select.Where whereClause = select.where();
        for (Entry<String, Object> entry : columns.entrySet()) {
            whereClause.and(eq(entry.getKey(), entry.getValue()));
        }
        return select.getQueryString();
    }

}
