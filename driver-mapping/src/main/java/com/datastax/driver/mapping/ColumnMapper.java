package com.datastax.driver.mapping;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.*;

import com.datastax.driver.core.DataType;

abstract class ColumnMapper<T> {

    public enum Kind { PARTITION_KEY, CLUSTERING_COLUMN, REGULAR };

    protected final String columnName;
    protected final String fieldName;
    protected final Class<?> javaType;
    protected final DataType dataType;
    protected final Kind kind;
    protected final int position;

    protected ColumnMapper(Field field, int position) {
        this(AnnotationParser.columnName(field), field.getName(), field.getType(), AnnotationParser.kind(field), position);
    }

    private ColumnMapper(String columnName, String fieldName, Class<?> javaType, Kind kind, int position) {
        this.columnName = columnName;
        this.fieldName = fieldName;
        this.javaType = javaType;
        this.dataType = fromJavaType(javaType);
        this.kind = kind;
        this.position = position;
    }

    public abstract Object getValue(T entity);
    public abstract void setValue(T entity, Object value);

    public String getColumnName() {
        return columnName;
    }

    public DataType getDataType() {
        return dataType;
    }

    // TODO: move that in the core (in DataType)
    private static DataType fromJavaType(Class<?> type) {
        if (type == String.class)
            return DataType.text();
        if (type == ByteBuffer.class)
            return DataType.blob();
        if (type == Boolean.class || type == boolean.class)
            return DataType.cboolean();
        if (type == Long.class || type == long.class)
            return DataType.bigint();
        if (type == BigDecimal.class)
            return DataType.decimal();
        if (type == Double.class || type == double.class)
            return DataType.cdouble();
        if (type == Float.class || type == float.class)
            return DataType.cfloat();
        if (type == InetAddress.class)
            return DataType.inet();
        if (type == Integer.class || type == int.class)
            return DataType.cint();
        if (type == Date.class)
            return DataType.timestamp();
        if (type == UUID.class)
            return DataType.uuid();
        if (type == BigInteger.class)
            return DataType.varint();

        throw new UnsupportedOperationException("Unsupported type '" + type.getName() + "'");

    }
}

