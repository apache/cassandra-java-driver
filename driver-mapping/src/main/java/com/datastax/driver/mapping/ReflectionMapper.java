package com.datastax.driver.mapping;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.*;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;

/**
 * An {@link EntityMapper} implementation that use reflection to read and write fields
 * of an entity.
 */
class ReflectionMapper<T> extends EntityMapper<T> {

    private static ReflectionFactory factory = new ReflectionFactory();

    private ReflectionMapper(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
        super(entityClass, keyspace, table, writeConsistency, readConsistency);
    }

    public static Factory factory() {
        return factory;
    }

    @Override
    public T newEntity() {
        try {
            return entityClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Can't create an instance of " + entityClass.getName());
        }
    }

    private static class LiteralMapper<T> extends ColumnMapper<T> {

        private final Method readMethod;
        private final Method writeMethod;

        private LiteralMapper(Field field, int position, PropertyDescriptor pd) {
            this(field, fromJavaType(field.getType()), position, pd);
        }

        private LiteralMapper(Field field, DataType type, int position, PropertyDescriptor pd) {
            super(field, type, position);
            this.readMethod = pd.getReadMethod();
            this.writeMethod = pd.getWriteMethod();
        }

        @Override
        public Object getValue(T entity) {
            try {
                return readMethod.invoke(entity);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Could not get field '" + fieldName + "'");
            } catch (Exception e) {
                throw new IllegalStateException("Unable to access getter for '" + fieldName + "' in " + entity.getClass().getName(), e);
            }
        }

        @Override
        public void setValue(Object entity, Object value) {
            try {
                writeMethod.invoke(entity, value);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Could not set field '" + fieldName + "' to value '" + value + "'");
            } catch (Exception e) {
                throw new IllegalStateException("Unable to access setter for '" + fieldName + "' in " + entity.getClass().getName(), e);
            }
        }

        // TODO: move that in the core (in DataType)
        // (though we still need to handle enums here)
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

    private static class EnumMapper<T> extends LiteralMapper<T> {

        private final EnumType enumType;
        private final Map<String, Object> fromString;

        private EnumMapper(Field field, int position, PropertyDescriptor pd, EnumType enumType) {
            super(field, enumType == EnumType.STRING ? DataType.text() : DataType.cint(), position, pd);
            this.enumType = enumType;

            if (enumType == EnumType.STRING) {
                fromString = new HashMap<String, Object>(javaType.getEnumConstants().length);
                for (Object constant : javaType.getEnumConstants())
                    fromString.put(constant.toString().toLowerCase(), constant);

            } else {
                fromString = null;
            }
        }

        @Override
        public Object getValue(T entity) {
            Object value = super.getValue(entity);
            switch (enumType) {
                case STRING:
                    return value.toString();
                case ORDINAL:
                    return ((Enum)value).ordinal();
            }
            throw new AssertionError();
        }

        @Override
        public void setValue(Object entity, Object value) {
            Object converted = null;
            switch (enumType) {
                case STRING:
                    converted = fromString.get(value.toString().toLowerCase());
                    break;
                case ORDINAL:
                    converted = javaType.getEnumConstants()[(Integer)converted];
                    break;
            }
            super.setValue(entity, converted);
        }
    }

    private static class ReflectionFactory implements Factory {

        public <T> ColumnMapper<T> createColumnMapper(Class<T> entityClass, Field field, int position) {
            String fieldName = field.getName();
            try {
                PropertyDescriptor pd = new PropertyDescriptor(fieldName, field.getDeclaringClass());
                return field.getType().isEnum()
                     ? new EnumMapper<T>(field, position, pd, AnnotationParser.enumType(field))
                     : new LiteralMapper<T>(field, position, pd);
            } catch (IntrospectionException e) {
                throw new IllegalArgumentException("Cannot find matching getter and setter for field '" + fieldName + "'");
            }
        }

        public <T> EntityMapper<T> create(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
            return new ReflectionMapper<T>(entityClass, keyspace, table, writeConsistency, readConsistency);
        }
    }
}
