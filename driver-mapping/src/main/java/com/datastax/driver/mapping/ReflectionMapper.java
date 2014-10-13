/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
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

import com.datastax.driver.core.*;

import com.datastax.driver.mapping.annotations.UDT;

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
                    return (value == null) ? null : value.toString();
                case ORDINAL:
                    return (value == null) ? null : ((Enum)value).ordinal();
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
        private final UDTMapper<U> udtMapper;

        private UDTColumnMapper(Field field, int position, PropertyDescriptor pd, UDTMapper<U> udtMapper) {
            super(field, udtMapper.getUserType(), position, pd);
            this.udtMapper = udtMapper;
        }

        @Override
        public Object getValue(T entity) {
            @SuppressWarnings("unchecked")
            U udtEntity = (U) super.getValue(entity);
            return udtEntity == null ? null : udtMapper.toUDT(udtEntity);
        }

        @Override
        public void setValue(Object entity, Object value) {
            assert value instanceof UDTValue;
            UDTValue udtValue = (UDTValue) value;
            assert udtValue.getType().equals(udtMapper.getUserType());

            super.setValue(entity, udtMapper.toEntity((udtValue)));
        }
    }

    private static class UDTListMapper<T, V> extends LiteralMapper<T> {

        private final UDTMapper<V> valueMapper;

        UDTListMapper(Field field, int position, PropertyDescriptor pd, UDTMapper<V> valueMapper) {
            super(field, DataType.list(valueMapper.getUserType()), position, pd);
            this.valueMapper = valueMapper;
        }

        @Override
        public Object getValue(T entity) {
            @SuppressWarnings("unchecked")
            List<V> entities = (List<V>) super.getValue(entity);
            return valueMapper.toUDTValues(entities);
        }

        @Override
        public void setValue(Object entity, Object value) {
            @SuppressWarnings("unchecked")
            List<UDTValue> udtValues = (List<UDTValue>) value;
            super.setValue(entity, valueMapper.toEntities(udtValues));
        }
    }

    private static class UDTSetMapper<T, V> extends LiteralMapper<T> {

        private final UDTMapper<V> valueMapper;

        UDTSetMapper(Field field, int position, PropertyDescriptor pd, UDTMapper<V> valueMapper) {
            super(field, DataType.set(valueMapper.getUserType()), position, pd);
            this.valueMapper = valueMapper;
        }

        @Override
        public Object getValue(T entity) {
            @SuppressWarnings("unchecked")
            Set<V> entities = (Set<V>) super.getValue(entity);
            return valueMapper.toUDTValues(entities);
        }

        @Override
        public void setValue(Object entity, Object value) {
            @SuppressWarnings("unchecked")
            Set<UDTValue> udtValues = (Set<UDTValue>) value;
            super.setValue(entity, valueMapper.toEntities(udtValues));
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
        private final UDTMapper<K> keyMapper;
        private final UDTMapper<V> valueMapper;

        UDTMapMapper(Field field, int position, PropertyDescriptor pd, UDTMapper<K> keyMapper, UDTMapper<V> valueMapper, Class<?> keyClass, Class<?> valueClass) {
            super(field, buildDataType(field, keyMapper, valueMapper, keyClass, valueClass), position, pd);
            this.keyMapper = keyMapper;
            this.valueMapper = valueMapper;
        }

        @Override
        public Object getValue(T entity) {
            @SuppressWarnings("unchecked")
            Map<K, V> entities = (Map<K, V>) super.getValue(entity);
            return UDTMapper.toUDTValues(entities, keyMapper, valueMapper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void setValue(Object entity, Object fieldValue) {
            Map<Object, Object> udtValues = (Map<Object, Object>) fieldValue;
            super.setValue(entity, UDTMapper.toEntities(udtValues, keyMapper, valueMapper));
        }

        private static <K, V> DataType buildDataType(Field field, UDTMapper<K> keyMapper, UDTMapper<V> valueMapper, Class<?> keyClass, Class<?> valueClass) {
            assert keyMapper != null || valueMapper != null;

            DataType keyType = (keyMapper != null) ?
                                                  keyMapper.getUserType() :
                                                  getSimpleType(keyClass, field);
            DataType valueType = (valueMapper != null) ?
                                                  valueMapper.getUserType() :
                                                  getSimpleType(valueClass, field);
            return DataType.map(keyType, valueType);
        }
    }

    static DataType extractType(Field f) {
        Type type = f.getGenericType();

        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType)type;
            Type raw = pt.getRawType();
            if (!(raw instanceof Class))
                throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", type, f.getName()));

            Class<?> klass = (Class<?>)raw;
            if (List.class.isAssignableFrom(klass)) {
                return DataType.list(getSimpleType(ReflectionUtils.getParam(pt, 0, f.getName()), f));
            }
            if (Set.class.isAssignableFrom(klass)) {
                return DataType.set(getSimpleType(ReflectionUtils.getParam(pt, 0, f.getName()), f));
            }
            if (Map.class.isAssignableFrom(klass)) {
                return DataType.map(getSimpleType(ReflectionUtils.getParam(pt, 0, f.getName()), f), getSimpleType(ReflectionUtils.getParam(pt, 1, f.getName()), f));
            }
            throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", type, f.getName()));
        }

        if (!(type instanceof Class))
            throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", type, f.getName()));

        return getSimpleType((Class<?>)type, f);
    }

    static DataType getSimpleType(Class<?> klass, Field f) {
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
            return DataType.varint();

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

        throw new IllegalArgumentException(String.format("Cannot map unknown class %s for field %s", klass.getName(), f));
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

                if (field.getType().isAnnotationPresent(UDT.class)) {
                    UDTMapper<?> udtMapper = mappingManager.getUDTMapper(field.getType());
                    return (ColumnMapper<T>) new UDTColumnMapper(field, position, pd, udtMapper);
                }

                if (field.getGenericType() instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType) field.getGenericType();
                    Type raw = pt.getRawType();
                    if (!(raw instanceof Class))
                        throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", field, field.getName()));

                    Class<?> klass = (Class<?>)raw;
                    Class<?> firstTypeParam = ReflectionUtils.getParam(pt, 0, field.getName());
                    if (List.class.isAssignableFrom(klass) && firstTypeParam.isAnnotationPresent(UDT.class)) {
                        UDTMapper<?> valueMapper = mappingManager.getUDTMapper(firstTypeParam);
                        return (ColumnMapper<T>) new UDTListMapper(field, position, pd, valueMapper);
                    }
                    if (Set.class.isAssignableFrom(klass) && firstTypeParam.isAnnotationPresent(UDT.class)) {
                        UDTMapper<?> valueMapper = mappingManager.getUDTMapper(firstTypeParam);
                        return (ColumnMapper<T>) new UDTSetMapper(field, position, pd, valueMapper);
                    }
                    if (Map.class.isAssignableFrom(klass)) {
                        Class<?> secondTypeParam = ReflectionUtils.getParam(pt, 1, field.getName());
                        UDTMapper<?> keyMapper = firstTypeParam.isAnnotationPresent(UDT.class) ? mappingManager.getUDTMapper(firstTypeParam) : null;
                        UDTMapper<?> valueMapper = secondTypeParam.isAnnotationPresent(UDT.class) ? mappingManager.getUDTMapper(secondTypeParam) : null;

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
