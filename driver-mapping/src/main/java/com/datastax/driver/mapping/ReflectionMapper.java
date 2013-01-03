package com.datastax.driver.mapping;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import antlr.ByteBuffer;

import com.datastax.driver.core.Row;
import com.datastax.driver.mapping.EntityDefinition.ColumnDefinition;
import com.datastax.driver.mapping.EntityDefinition.EnumColumnDefinition;
import com.datastax.driver.mapping.EntityDefinition.SubEntityDefinition;

/**
 * A {@link Mapper} implementation that use reflection to read and write fields
 * of an entity.
 */
public class ReflectionMapper extends Mapper {
	/* TODO: a more efficient implementation should be added but would be overkill
	 * for now as queries are currently moved around as Statements rather than 
	 * as PS.
	 */

	final Map<String, ColumnMapper> columnNamesToMapper;
	final Map<String, SubEntityMapper> valueToMapper;
	final Map<Class, SubEntityMapper> classToMapper;


	public ReflectionMapper(EntityDefinition entityDef) {
		super(entityDef);
		columnNamesToMapper = new HashMap<String, ColumnMapper>();
		valueToMapper = new HashMap<String, SubEntityMapper>();
		classToMapper = new HashMap<Class, SubEntityMapper>();

		for (ColumnDefinition columnDef: entityDef.columns) {
			ColumnMapper columnMapper;
			if (columnDef instanceof EnumColumnDefinition) {
				columnMapper = new EnumColumnMapper((EnumColumnDefinition)columnDef);
			} else {
				columnMapper = new LiteralColumnMapper(columnDef);
			}
			columnNamesToMapper.put(columnDef.columnName, columnMapper);
		}

		for (SubEntityDefinition subEntityDef : entityDef.subEntities) {
			SubEntityMapper subEntityMapper = new SubEntityMapper(subEntityDef);
			valueToMapper.put(subEntityDef.inheritanceColumnValue, subEntityMapper);
			classToMapper.put(subEntityDef.javaType, subEntityMapper);
		}
	}

	@Override
	public Map<String, Object> entityToColumns(Object entity) {
		Map<String, Object> columns = new HashMap<String, Object>();
		Map<String, ColumnMapper> columnMappers;
		if (entityDef.inheritanceColumn != null) {
			SubEntityMapper mapper = classToMapper.get(entity.getClass());
			String inheritanceColumn = mapper.subEntityDef.parentEntity.inheritanceColumn;
			String inheritanceValue = mapper.subEntityDef.inheritanceColumnValue;
			columns.put(inheritanceColumn, inheritanceValue);
			columnMappers = mapper.columnNamesToMapper;
		} else {
			columnMappers = columnNamesToMapper;
		}
		for (ColumnMapper columnMapper : columnMappers.values()) {
			String name = columnMapper.getColumnDef().columnName;
			Object value = columnMapper.getField(entity);
			columns.put(name, value);
		}
		return columns;
	}

	@Override
	public Object rowToEntity(Row row) {
		Object entity;
		Map<String, ColumnMapper> columnMappers;
		if (entityDef.inheritanceColumn == null) {
			try {
				entity = entityDef.entityClass.newInstance();
			} catch (Exception e) {
				throw new RuntimeException("Can't create an instance of " + entityDef.entityClass.getName());
			}
			columnMappers = columnNamesToMapper;
		} else {			
			Object inheritanceValue = row.getString(entityDef.inheritanceColumn);
			if (inheritanceValue == null) {
				throw new IllegalArgumentException("Undefined value of inheritance column '" 
						+ entityDef.inheritanceColumn + "', can't create an entity");
			}
			SubEntityMapper subEntityMapper = valueToMapper.get(inheritanceValue);
			if (subEntityMapper == null) {
				throw new IllegalArgumentException("Value '" + inheritanceValue + 
						"' hasn't been defined in any @InheritanceValue annotation on referenced subclasses, can't create an entity");
			}
			try {
				entity = subEntityMapper.subEntityDef.javaType.newInstance();
			} catch (Exception e) {
				throw new RuntimeException("Can't create an instance of " + subEntityMapper.subEntityDef.javaType.getName());
			}
			columnMappers = subEntityMapper.columnNamesToMapper;
		}

		for (Entry<String, ColumnMapper> entry : columnMappers.entrySet()) {
			ColumnMapper columnMapper = entry.getValue();
			columnMapper.setField(entity, getColumn(row, columnMapper.getColumnDef()));
		}
		return entity;
	}

	private Object getColumn(Row row, ColumnDefinition def) {
		String name = def.columnName;
		Class type = def.javaType;
		if (type == String.class)
			return row.getString(name);
		if (type == ByteBuffer.class) 	
			return row.getBytes(name);
		if (type == Boolean.class || type == boolean.class)
			return row.getBool(name);
		if (type == Long.class || type == long.class)
			return row.getLong(name);
		if (type == BigDecimal.class)
			return row.getDecimal(name);
		if (type == Double.class || type == double.class)
			return row.getDouble(name);
		if (type == Float.class || type == float.class)
			return row.getFloat(name);
		if (type == InetAddress.class)
			return row.getInet(name);
		if (type == Integer.class || type == int.class)
			return row.getInt(name);
		if (type == Date.class)
			return row.getDate(name);
		if (type == UUID.class)
			return row.getUUID(name);
		if (type == BigInteger.class)
			return row.getVarint(name);

		throw new UnsupportedOperationException("Unsupported type '" + type.getName() + "'");

	}

	interface ColumnMapper {
		void setField(Object entity, Object value);

		Object getField(Object entity);

		ColumnDefinition getColumnDef();
	}

	class LiteralColumnMapper implements ColumnMapper {
		final ColumnDefinition columnDef;

		LiteralColumnMapper(ColumnDefinition columnDef) {
			this.columnDef = columnDef;
		}

		@Override
		public void setField(Object entity, Object value) {
			try {
				columnDef.writeMethod.invoke(entity, value);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("Could not set field '" + columnDef.fieldName + "' to value '" + value + "'");
			} catch (Exception e) {
				throw new IllegalStateException("Unable to access setter for '" + columnDef.fieldName + "' in " + entity.getClass().getName(), e);
			}

		}

		@Override
		public Object getField(Object entity) {
			Object value;
			try {
				value = columnDef.readMethod.invoke(entity);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("Could not get field '" + columnDef.fieldName + "'");
			} catch (Exception e) {
				throw new IllegalStateException("Unable to access setter for '" + columnDef.fieldName + "' in " + entity.getClass().getName(), e);
			}

			return value;
		}

		@Override
		public ColumnDefinition getColumnDef() {
			return columnDef;
		}


	}

	class EnumColumnMapper implements ColumnMapper {
		final EnumColumnDefinition columnDef;

		EnumColumnMapper(EnumColumnDefinition columnDef) {
			this.columnDef = columnDef;
		}

		@Override
		public void setField(Object entity, Object value) {
			Object enm = columnDef.valueToEnum.get(value);
			try {
				columnDef.writeMethod.invoke(entity, enm);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("Could not set field '" + columnDef.fieldName + "' to value '" + value + "'");
			} catch (Exception e) {
				throw new IllegalStateException("Unable to access setter for '" + columnDef.fieldName + "' in " + entity.getClass().getName(), e);
			}

		}

		@Override
		public Object getField(Object entity) {
			Object enm;
			try {
				enm = columnDef.writeMethod.invoke(entity);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("Could not get field '" + columnDef.fieldName + "'");
			} catch (Exception e) {
				throw new IllegalStateException("Unable to access setter for '" + columnDef.fieldName + "' in " + entity.getClass().getName(), e);
			}

			return columnDef.enumToValue.get(enm);
		}

		@Override
		public ColumnDefinition getColumnDef() {
			return columnDef;
		}
	}

	class SubEntityMapper {
		final SubEntityDefinition subEntityDef;
		final Map<String, ColumnMapper> columnNamesToMapper;
		final Map<String, ColumnMapper> fieldNamesToMapper;

		public SubEntityMapper(SubEntityDefinition subEntityDef) {
			this.subEntityDef = subEntityDef;
			columnNamesToMapper = new HashMap<String, ColumnMapper>();
			fieldNamesToMapper = new HashMap<String, ColumnMapper>();

			for (ColumnDefinition columnDef: subEntityDef.columns) {
				ColumnMapper columnMapper;
				if (columnDef instanceof EnumColumnDefinition) {
					columnMapper = new EnumColumnMapper((EnumColumnDefinition)columnDef);
				} else {
					columnMapper = new LiteralColumnMapper(columnDef);
				}
				columnNamesToMapper.put(columnDef.columnName, columnMapper);
				fieldNamesToMapper.put(columnDef.fieldName, columnMapper);
			}

			SubEntityMapper.this.columnNamesToMapper.putAll(ReflectionMapper.this.columnNamesToMapper);
		}
	}
	
}
