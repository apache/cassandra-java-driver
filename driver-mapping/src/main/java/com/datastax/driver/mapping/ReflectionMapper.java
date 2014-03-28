package com.datastax.driver.mapping;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
            this(field, extractType(field), position, pd);
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

        private static Class<?> getParam(ParameterizedType pt, int arg, Field f) {
            Type ft = pt.getActualTypeArguments()[arg];
            if (!(ft instanceof Class))
                throw new IllegalArgumentException(String.format("Cannot map parameter of class %s for field %s", pt, f.getName()));
            return (Class<?>)ft;
        }

        // TODO: move that in the core (in DataType)
        // (though we still need to handle enums here)
        private static DataType extractType(Field f) {
            Type type = f.getGenericType();

            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType)type;
                Type raw = pt.getRawType();
                if (!(raw instanceof Class))
                    throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", type, f.getName()));

                Class<?> klass = (Class<?>)raw;
                if (List.class.isAssignableFrom(klass)) {
                    return DataType.list(getSimpleType(getParam(pt, 0, f), f));
                }
                if (Set.class.isAssignableFrom(klass)) {
                    return DataType.set(getSimpleType(getParam(pt, 0, f), f));
                }
                if (Map.class.isAssignableFrom(klass)) {
                    return DataType.map(getSimpleType(getParam(pt, 0, f), f), getSimpleType(getParam(pt, 1, f), f));
                }
                throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", type, f.getName()));
            }

            if (!(type instanceof Class))
                throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", type, f.getName()));

            return getSimpleType((Class<?>)type, f);
        }

        private static DataType getSimpleType(Class<?> klass, Field f) {
            if (ByteBuffer.class.isAssignableFrom(klass))
                return DataType.blob();

            if (klass == int.class || Integer.class.isAssignableFrom(klass))
                    return DataType.cint();
            if (klass == long.class || Long.class.isAssignableFrom(klass))
                return DataType.bigint();
            if (klass == float.class || Float.class.isAssignableFrom(klass))
                return DataType.cfloat();
            if (klass == double.class || Double.class.isAssignableFrom(klass))
                return DataType.cdouble();
            if (klass == boolean.class || Boolean.class.isAssignableFrom(klass))
                return DataType.cboolean();

            if (BigDecimal.class.isAssignableFrom(klass))
                return DataType.decimal();
            if (BigInteger.class.isAssignableFrom(klass))
                return DataType.decimal();

            if (String.class.isAssignableFrom(klass))
                return DataType.text();
            if (InetAddress.class.isAssignableFrom(klass))
                return DataType.inet();
            if (Date.class.isAssignableFrom(klass))
                return DataType.timestamp();
            if (UUID.class.isAssignableFrom(klass))
                return DataType.uuid();

            if (Collection.class.isAssignableFrom(klass))
                throw new IllegalArgumentException(String.format("Cannot map non-parametrized collection type %s for field %s; Please use a concrete type parameter", klass.getName(), f.getName()));

            throw new IllegalArgumentException(String.format("Cannot map unknow class %s for field %s", klass.getName(), f));
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

        @SuppressWarnings("rawtypes")
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
                    converted = javaType.getEnumConstants()[(Integer)value];
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
