package com.datastax.driver.mapping;

import java.lang.reflect.Field;
import java.util.*;

import com.datastax.driver.core.DataType;
import static com.datastax.driver.core.querybuilder.QueryBuilder.quote;

abstract class ColumnMapper<T> {

    public enum Kind { PARTITION_KEY, CLUSTERING_COLUMN, REGULAR };

    private final String columnName;
    protected final String fieldName;
    protected final Class<?> javaType;
    protected final DataType dataType;
    protected final Kind kind;
    protected final int position;

    protected ColumnMapper(Field field, DataType dataType, int position) {
        this(AnnotationParser.columnName(field), field.getName(), field.getType(), dataType, AnnotationParser.kind(field), position);
    }

    private ColumnMapper(String columnName, String fieldName, Class<?> javaType, DataType dataType, Kind kind, int position) {
        this.columnName = columnName;
        this.fieldName = fieldName;
        this.javaType = javaType;
        this.dataType = dataType;
        this.kind = kind;
        this.position = position;
    }

    public abstract Object getValue(T entity);
    public abstract void setValue(T entity, Object value);

    public String getColumnName() {
        return quote(columnName);
    }

    public DataType getDataType() {
        return dataType;
    }

}
