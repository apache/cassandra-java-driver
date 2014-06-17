package com.datastax.driver.mapping;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.Reflection;
import com.datastax.driver.mapping.annotations.UserDefinedType;

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
            this(field, DataType.of(field), position, pd);
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

    private static class UDTColumnMapper<T, U> extends LiteralMapper<T> {
        private final NestedMapper<U> nestedMapper;

        private UDTColumnMapper(Field field, int position, PropertyDescriptor pd, NestedMapper<U> nestedMapper) {
            super(field, DataType.userType(nestedMapper.getUdtDefinition()), position, pd);
            this.nestedMapper = nestedMapper;
        }

        @Override
        public Object getValue(T entity) {
            @SuppressWarnings("unchecked")
            U nestedEntity = (U) super.getValue(entity);
            return nestedMapper.toUDTValue(nestedEntity);
        }

        @Override
        public void setValue(Object entity, Object value) {
            assert value instanceof UDTValue;
            UDTValue udtValue = (UDTValue) value;
            assert udtValue.getDefinition().equals(nestedMapper.getUdtDefinition());

            super.setValue(entity, nestedMapper.toNestedEntity((udtValue)));
        }
    }

    private static class UDTListMapper<T, V> extends LiteralMapper<T> {

        private final NestedMapper<V> valueMapper;

        UDTListMapper(Field field, int position, PropertyDescriptor pd, NestedMapper<V> valueMapper) {
            super(field, DataType.list(DataType.userType(valueMapper.getUdtDefinition())), position, pd);
            this.valueMapper = valueMapper;
        }

        @Override
        public Object getValue(T entity) {
            @SuppressWarnings("unchecked")
            List<V> nestedEntities = (List<V>) super.getValue(entity);
            return valueMapper.toUDTValues(nestedEntities);
        }

        @Override
        public void setValue(Object entity, Object value) {
            @SuppressWarnings("unchecked")
            List<UDTValue> udtValues = (List<UDTValue>) value;
            super.setValue(entity, valueMapper.toNestedEntities(udtValues));
        }
    }

    private static class UDTSetMapper<T, V> extends LiteralMapper<T> {

        private final NestedMapper<V> valueMapper;

        UDTSetMapper(Field field, int position, PropertyDescriptor pd, NestedMapper<V> valueMapper) {
            super(field, DataType.set(DataType.userType(valueMapper.getUdtDefinition())), position, pd);
            this.valueMapper = valueMapper;
        }

        @Override
        public Object getValue(T entity) {
            @SuppressWarnings("unchecked")
            Set<V> nestedEntities = (Set<V>) super.getValue(entity);
            return valueMapper.toUDTValues(nestedEntities);
        }

        @Override
        public void setValue(Object entity, Object value) {
            @SuppressWarnings("unchecked")
            Set<UDTValue> udtValues = (Set<UDTValue>) value;
            super.setValue(entity, valueMapper.toNestedEntities(udtValues));
        }
    }

    /**
     * A map field that contains UDT values.
     * <p>
     * UDTs may be used either as keys, or as values, or both. This is reflected
     * in keyMapper and valueMapper being null or non-null.
     * </p>
     */
    private static class UDTMapMapper<T, K, V> extends LiteralMapper<T> {
        private final NestedMapper<K> keyMapper;
        private final NestedMapper<V> valueMapper;

        UDTMapMapper(Field field, int position, PropertyDescriptor pd, NestedMapper<K> keyMapper, NestedMapper<V> valueMapper, Class<?> keyClass, Class<?> valueClass) {
            super(field, buildDataType(field, keyMapper, valueMapper, keyClass, valueClass), position, pd);
            this.keyMapper = keyMapper;
            this.valueMapper = valueMapper;
        }

        @Override
        public Object getValue(T entity) {
            @SuppressWarnings("unchecked")
            Map<K, V> nestedEntities = (Map<K, V>) super.getValue(entity);
            return NestedMapper.toUDTValues(nestedEntities, keyMapper, valueMapper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void setValue(Object entity, Object fieldValue) {
            Map<Object, Object> udtValues = (Map<Object, Object>) fieldValue;
            super.setValue(entity, NestedMapper.toNestedEntities(udtValues, keyMapper, valueMapper));
        }

        private static <K, V> DataType buildDataType(Field field, NestedMapper<K> keyMapper, NestedMapper<V> valueMapper, Class<?> keyClass, Class<?> valueClass) {
            assert keyMapper != null || valueMapper != null;

            DataType keyType = (keyMapper != null) ?
                                                  DataType.userType(keyMapper.getUdtDefinition()) :
                                                  DataType.of(keyClass, field);
            DataType valueType = (valueMapper != null) ?
                                                  DataType.userType(valueMapper.getUdtDefinition()) :
                                                  DataType.of(valueClass, field);
            return DataType.map(keyType, valueType);
        }
    }

    private static class ReflectionFactory implements Factory {

        public <T> EntityMapper<T> create(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
            return new ReflectionMapper<T>(entityClass, keyspace, table, writeConsistency, readConsistency);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T> ColumnMapper<T> createColumnMapper(Class<T> entityClass, Field field, int position, MappingManager mappingManager) {
            String fieldName = field.getName();
            try {
                PropertyDescriptor pd = new PropertyDescriptor(fieldName, field.getDeclaringClass());

                if (field.getType().isEnum()) {
                    return new EnumMapper<T>(field, position, pd, AnnotationParser.enumType(field));
                }

                if (field.getType().isAnnotationPresent(UserDefinedType.class)) {
                    NestedMapper<?> nestedMapper = mappingManager.getNestedMapper(field.getType());
                    return (ColumnMapper<T>) new UDTColumnMapper(field, position, pd, nestedMapper);
                }

                if (field.getGenericType() instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType) field.getGenericType();
                    Type raw = pt.getRawType();
                    if (!(raw instanceof Class))
                        throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", field, field.getName()));

                    Class<?> klass = (Class<?>)raw;
                    Class<?> firstTypeParam = Reflection.getParam(pt, 0, field.getName());
                    if (List.class.isAssignableFrom(klass) && firstTypeParam.isAnnotationPresent(UserDefinedType.class)) {
                        NestedMapper<?> valueMapper = mappingManager.getNestedMapper(firstTypeParam);
                        return (ColumnMapper<T>) new UDTListMapper(field, position, pd, valueMapper);
                    }
                    if (Set.class.isAssignableFrom(klass) && firstTypeParam.isAnnotationPresent(UserDefinedType.class)) {
                        NestedMapper<?> valueMapper = mappingManager.getNestedMapper(firstTypeParam);
                        return (ColumnMapper<T>) new UDTSetMapper(field, position, pd, valueMapper);
                    }
                    if (Map.class.isAssignableFrom(klass)) {
                        Class<?> secondTypeParam = Reflection.getParam(pt, 1, field.getName());
                        NestedMapper<?> keyMapper = firstTypeParam.isAnnotationPresent(UserDefinedType.class) ? mappingManager.getNestedMapper(firstTypeParam) : null;
                        NestedMapper<?> valueMapper = secondTypeParam.isAnnotationPresent(UserDefinedType.class) ? mappingManager.getNestedMapper(secondTypeParam) : null;

                        if (keyMapper != null || valueMapper != null) {
                            return (ColumnMapper<T>) new UDTMapMapper(field, position, pd, keyMapper, valueMapper, firstTypeParam, secondTypeParam);
                        }
                    }
                }

                return new LiteralMapper<T>(field, position, pd);

            } catch (IntrospectionException e) {
                throw new IllegalArgumentException("Cannot find matching getter and setter for field '" + fieldName + "'");
            }
        }
    }
}
