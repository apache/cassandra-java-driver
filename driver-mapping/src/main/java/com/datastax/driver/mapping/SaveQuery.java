package com.datastax.driver.mapping;

import static com.datastax.driver.core.utils.querybuilder.QueryBuilder.insert;

import java.nio.ByteBuffer;
import java.util.Map;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.querybuilder.Insert;

/**
 * A query that performs an insert for the provided entity.
 */
public class SaveQuery extends Statement {
	private final Mapper mapper;
	private final Map<String, Object> columns;

	public SaveQuery(EntitySession session, Object entity) {
		mapper = session.getMapper(entity.getClass());
		columns = mapper.entityToColumns(entity);
		setConsistencyLevel(mapper.entityDef.defaultWriteCL);
	}
	
	@Override
	public ByteBuffer getRoutingKey() {
		return null;
	}

	@Override
	public String getQueryString() {
		Insert insert =
				insert(columns.keySet().toArray(new String[columns.size()]))
				.into(mapper.entityDef.tableName)
			    .values(columns.values().toArray());
		return insert.getQueryString();
	}


}
