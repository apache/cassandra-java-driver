/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.mapping;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.reflect.TypeToken;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation of {@link MappedProperty}.
 */
class DefaultMappedProperty<T> implements MappedProperty<T> {

    static <T> DefaultMappedProperty<T> create(Class<?> mappedClass, String propertyName, String mappedName, Field field, Method getter, Method setter, Map<Class<? extends Annotation>, Annotation> annotations) {
        @SuppressWarnings("unchecked")
        TypeToken<T> propertyType = (TypeToken<T>) inferType(field, getter);
        boolean partitionKey = annotations.containsKey(PartitionKey.class);
        boolean clusteringColumn = annotations.containsKey(ClusteringColumn.class);
        boolean computed = annotations.containsKey(Computed.class);
        int position = inferPosition(annotations);
        @SuppressWarnings("unchecked")
        Class<? extends TypeCodec<T>> codecClass = (Class<? extends TypeCodec<T>>) getCustomCodecClass(annotations);
        return new DefaultMappedProperty<T>(
                mappedClass, propertyName, mappedName, propertyType,
                partitionKey, clusteringColumn, computed, position, codecClass, field, getter, setter);

    }

    private final Class<?> mappedClass;
    private final String propertyName;
    private final TypeToken<T> propertyType;
    private final String mappedName;
    private final boolean partitionKey;
    private final boolean clusteringColumn;
    private final boolean computed;
    private final int position;
    private final TypeCodec<T> customCodec;
    private final Field field;
    private final Method getter;
    private final Method setter;

    private DefaultMappedProperty(
            Class<?> mappedClass, String propertyName, String mappedName, TypeToken<T> propertyType,
            boolean partitionKey, boolean clusteringColumn, boolean computed, int position,
            Class<? extends TypeCodec<T>> codecClass, Field field, Method getter, Method setter) {
        checkArgument(propertyName != null && !propertyName.isEmpty());
        checkArgument(mappedName != null && !mappedName.isEmpty());
        checkNotNull(propertyType);
        this.mappedClass = mappedClass;
        this.propertyName = propertyName;
        this.mappedName = mappedName;
        this.propertyType = propertyType;
        this.partitionKey = partitionKey;
        this.clusteringColumn = clusteringColumn;
        this.computed = computed;
        this.position = position;
        this.customCodec = codecClass == null || codecClass.equals(Defaults.NoCodec.class) ? null : ReflectionUtils.newInstance(codecClass);
        this.field = field;
        this.getter = getter;
        this.setter = setter;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public TypeToken<T> getPropertyType() {
        return propertyType;
    }

    @Override
    public String getMappedName() {
        return mappedName;
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public TypeCodec<T> getCustomCodec() {
        return customCodec;
    }

    @Override
    public boolean isComputed() {
        return computed;
    }

    @Override
    public boolean isPartitionKey() {
        return partitionKey;
    }

    @Override
    public boolean isClusteringColumn() {
        return clusteringColumn;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T getValue(Object entity) {
        try {
            // try getter first, if available, otherwise direct field access
            if (getter != null && getter.isAccessible())
                return (T) getter.invoke(entity);
            else
                return (T) checkNotNull(field).get(entity);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to read property '" + getPropertyName() + "' in " + entity.getClass(), e);
        }
    }

    @Override
    public void setValue(Object entity, T value) {
        try {
            // try setter first, if available, otherwise direct field access
            if (setter != null && setter.isAccessible())
                setter.invoke(entity, value);
            else
                checkNotNull(field).set(entity, value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to write property '" + getPropertyName() + "' in " + entity.getClass(), e);
        }
    }

    @Override
    public String toString() {
        return mappedClass.getSimpleName() + "." + getPropertyName();
    }

    private static TypeToken<?> inferType(Field field, Method getter) {
        if (getter != null)
            return TypeToken.of(getter.getGenericReturnType());
        else
            return TypeToken.of(checkNotNull(field).getGenericType());
    }

    private static int inferPosition(Map<Class<? extends Annotation>, Annotation> annotations) {
        if (annotations.containsKey(PartitionKey.class)) {
            return ((PartitionKey) annotations.get(PartitionKey.class)).value();
        }
        if (annotations.containsKey(ClusteringColumn.class)) {
            return ((ClusteringColumn) annotations.get(ClusteringColumn.class)).value();
        }
        return -1;
    }

    private static Class<? extends TypeCodec<?>> getCustomCodecClass(Map<Class<? extends Annotation>, Annotation> annotations) {
        Column column = (Column) annotations.get(Column.class);
        if (column != null)
            return column.codec();
        com.datastax.driver.mapping.annotations.Field udtField =
                (com.datastax.driver.mapping.annotations.Field) annotations.get(com.datastax.driver.mapping.annotations.Field.class);
        if (udtField != null)
            return udtField.codec();
        return null;
    }

}
