/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.annotations.Computed;

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

        private LiteralMapper(Field field, int position, PropertyDescriptor pd, AtomicInteger columnNumber) {
            this(field, extractSimpleType(field), position, pd, columnNumber);
        }

        private LiteralMapper(Field field, DataType type, int position, PropertyDescriptor pd, AtomicInteger columnCounter) {
            super(field, type, position, columnCounter);
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

        private EnumMapper(Field field, int position, PropertyDescriptor pd, EnumType enumType, AtomicInteger columnCounter) {
            super(field, enumType == EnumType.STRING ? DataType.text() : DataType.cint(), position, pd, columnCounter);
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

        private UDTColumnMapper(Field field, int position, PropertyDescriptor pd, UDTMapper<U> udtMapper, AtomicInteger columnCounter) {
            super(field, udtMapper.getUserType(), position, pd, columnCounter);
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

    private static class NestedUDTMapper<T> extends LiteralMapper<T> {
        private final InferredCQLType inferredCQLType;

        public NestedUDTMapper(Field field, int position, PropertyDescriptor pd, InferredCQLType inferredCQLType, AtomicInteger columnCounter) {
            super(field, inferredCQLType.dataType, position, pd, columnCounter);
            this.inferredCQLType = inferredCQLType;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object getValue(T entity) {
            Object valueWithEntities = super.getValue(entity);
            return (T)UDTMapper.convertEntitiesToUDTs(valueWithEntities, inferredCQLType);
        }

        @Override
        public void setValue(Object entity, Object valueWithUDTValues) {
            super.setValue(entity, UDTMapper.convertUDTsToEntities(valueWithUDTValues, inferredCQLType));
        }
    }

    static DataType extractSimpleType(Field f) {
        Type type = f.getGenericType();

        assert !(type instanceof ParameterizedType);

        if (!(type instanceof Class))
            throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", type, f.getName()));

        return TypeMappings.getSimpleType((Class<?>)type, f.getName());
    }

    private static class ReflectionFactory implements Factory {

        public <T> EntityMapper<T> create(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
            return new ReflectionMapper<T>(entityClass, keyspace, table, writeConsistency, readConsistency);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T> ColumnMapper<T> createColumnMapper(Class<T> entityClass, Field field, int position, MappingManager mappingManager, AtomicInteger columnCounter) {
            String fieldName = field.getName();
            try {
                PropertyDescriptor pd = new PropertyDescriptor(fieldName, field.getDeclaringClass());

                if (field.getType().isEnum()) {
                    return new EnumMapper<T>(field, position, pd, AnnotationParser.enumType(field), columnCounter);
                }

                if (TypeMappings.isMappedUDT(field.getType())) {
                    UDTMapper<?> udtMapper = mappingManager.getUDTMapper(field.getType());
                    return (ColumnMapper<T>) new UDTColumnMapper(field, position, pd, udtMapper, columnCounter);
                }

                if (field.getGenericType() instanceof ParameterizedType) {
                    InferredCQLType inferredCQLType = InferredCQLType.from(field, mappingManager);
                    if (inferredCQLType.containsMappedUDT) {
                        // We need a specialized mapper to convert UDT instances in the hierarchy.
                        return (ColumnMapper<T>)new NestedUDTMapper(field, position, pd, inferredCQLType, columnCounter);
                    } else {
                        // The default codecs will know how to handle the extracted datatype.
                        return new LiteralMapper<T>(field, inferredCQLType.dataType, position, pd, columnCounter);
                    }
                }

                return new LiteralMapper<T>(field, position, pd, columnCounter);

            } catch (IntrospectionException e) {
                throw new IllegalArgumentException("Cannot find matching getter and setter for field '" + fieldName + "'");
            }
        }
    }
}
