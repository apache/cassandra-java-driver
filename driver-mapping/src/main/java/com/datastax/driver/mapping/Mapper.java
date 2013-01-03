package com.datastax.driver.mapping;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Row;

public abstract class Mapper {
	final EntityDefinition entityDef;
	
	Mapper(EntityDefinition entityDef) {
		this.entityDef = entityDef;
	}
	
	abstract Map<String, Object> entityToColumns(Object entity);

	abstract Object rowToEntity(Row row);
	
	public static class VoidMapper extends Mapper {
		public VoidMapper() {
			super(null);
		}
		
		@Override
		Map<String, Object> entityToColumns(Object entity) {
			return new HashMap<String, Object>();
		}

		@Override
		Object rowToEntity(Row row) {
			return null;
		}
		
	}
}
