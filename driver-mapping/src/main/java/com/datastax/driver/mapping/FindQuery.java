package com.datastax.driver.mapping;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.querybuilder.Clause;
import com.datastax.driver.core.utils.querybuilder.Select;

import static com.datastax.driver.core.utils.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.utils.querybuilder.Clause.*;

/** 
 * A query that performs a find by example using the entity provided as constructor.
 *
 */
public class FindQuery extends Statement {
	private final Mapper mapper;
	private final Map<String, Object> columns;

	public FindQuery(EntitySession session, Object entity) {
		mapper = session.getMapper(entity.getClass());
		columns = mapper.entityToColumns(entity);
		setConsistencyLevel(mapper.entityDef.defaultReadCL);
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
		Select select = select(all()).from(mapper.entityDef.tableName).where(clauses);
		return select.getQueryString();
	}

}
