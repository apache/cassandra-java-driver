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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datastax.driver.core.*;

/**
 * An object handling the mapping of a particular class to a UDT.
 * <p>
 * A {@code UDTMapper} object is obtained from a {@code MappingManager} using the
 * {@link MappingManager#udtMapper} method.
 * </p>
 */
public class UDTMapper<T> {
    // UDTs are always serialized with the v3 protocol
    private static final ProtocolVersion UDT_PROTOCOL_VERSION = ProtocolVersion.V3;

    private final EntityMapper<T> entityMapper;
    private final UserType userType;

    UDTMapper(EntityMapper<T> entityMapper, Session session) {
        this.entityMapper = entityMapper;

        String keyspace = entityMapper.getKeyspace();
        String udt = entityMapper.getTable();
        userType = session.getCluster().getMetadata().getKeyspace(keyspace).getUserType(udt);
        if (userType == null) {
            throw new IllegalArgumentException(String.format("Type \"%s\" does not exist in keyspace \"%s\"", udt, keyspace));
        }
    }

    /**
     * Converts a {@link UDTValue} (returned by the core API) to the mapped
     * class.
     *
     * @param v the {@code UDTValue}.
     * @return an instance of the mapped class.
     *
     * @throws IllegalArgumentException if the {@code UDTValue} is not of the
     * type indicated in the mapped class's {@code @UDT} annotation.
     */
    public T fromUDT(UDTValue v) {
        if (!v.getType().equals(userType)) {
            String message = String.format("UDT conversion mismatch: expected type %s, got %s",
                                           userType, v.getType());
            throw new IllegalArgumentException(message);
        }
        return toEntity(v);
    }

    /**
     * Converts a mapped class to a {@link UDTValue}.
     *
     * @param entity an instance of the mapped class.
     *
     * @return the corresponding {@code UDTValue}.
     */
    public UDTValue toUDT(T entity) {
        UDTValue udtValue = userType.newValue();
        for (ColumnMapper<T> cm : entityMapper.allColumns()) {
            Object value = cm.getValue(entity);
            udtValue.setBytesUnsafe(cm.getColumnName(), value == null ? null : cm.getDataType().serialize(value, UDT_PROTOCOL_VERSION));
        }
        return udtValue;
    }

    UserType getUserType() {
        return userType;
    }

    List<UDTValue> toUDTValues(List<T> entities) {
        if (entities == null)
            return null;

        List<UDTValue> udtValues = new ArrayList<UDTValue>(entities.size());
        for (T entity : entities) {
            UDTValue udtValue = toUDT(entity);
            udtValues.add(udtValue);
        }
        return udtValues;
    }

    Set<UDTValue> toUDTValues(Set<T> entities) {
        if (entities == null)
            return null;

        Set<UDTValue> udtValues = Sets.newHashSetWithExpectedSize(entities.size());
        for (T entity : entities) {
            UDTValue udtValue = toUDT(entity);
            udtValues.add(udtValue);
        }
        return udtValues;
    }

    /*
     * Map conversion methods are static because they use two mappers, either of
     * which (but not both) may be null.
     *
     * This reflects the fact that the map datatype can use UDTs as keys, or
     * values, or both.
     */
    static <K, V> Map<Object, Object> toUDTValues(Map<K, V> entities, UDTMapper<K> keyMapper, UDTMapper<V> valueMapper) {
        assert keyMapper != null || valueMapper != null;
        if (entities == null)
            return null;

        Map<Object, Object> udtValues = Maps.newHashMapWithExpectedSize(entities.size());
        for (Entry<K, V> entry : entities.entrySet()) {
            Object key = (keyMapper == null) ? entry.getKey() : keyMapper.toUDT(entry.getKey());
            Object value = (valueMapper == null) ? entry.getValue() : valueMapper.toUDT(entry.getValue());
            udtValues.put(key, value);
        }
        return udtValues;
    }

    T toEntity(UDTValue udtValue) {
        T entity = entityMapper.newEntity();
        for (ColumnMapper<T> cm : entityMapper.allColumns()) {
            ByteBuffer bytes = udtValue.getBytesUnsafe(cm.getColumnName());
            if (bytes != null)
                cm.setValue(entity, cm.getDataType().deserialize(bytes, UDT_PROTOCOL_VERSION));
        }
        return entity;
    }

    List<T> toEntities(List<UDTValue> udtValues) {
        List<T> entities = new ArrayList<T>(udtValues.size());
        for (UDTValue udtValue : udtValues) {
            entities.add(toEntity(udtValue));
        }
        return entities;
    }

    Set<T> toEntities(Set<UDTValue> udtValues) {
        Set<T> entities = Sets.newHashSetWithExpectedSize(udtValues.size());
        for (UDTValue udtValue : udtValues) {
            entities.add(toEntity(udtValue));
        }
        return entities;
    }

    @SuppressWarnings("unchecked")
    static <K, V> Map<K, V> toEntities(Map<Object, Object> udtValues, UDTMapper<K> keyMapper, UDTMapper<V> valueMapper) {
        Map<K, V> entities = Maps.newHashMapWithExpectedSize(udtValues.size());
        for (Entry<Object, Object> entry : udtValues.entrySet()) {
            K key = (keyMapper == null) ? (K) entry.getKey() : keyMapper.toEntity((UDTValue) entry.getKey());
            V value = (valueMapper == null) ? (V) entry.getValue() : valueMapper.toEntity((UDTValue)entry.getValue());
            entities.put(key, value);
        }
        return entities;
    }
}
