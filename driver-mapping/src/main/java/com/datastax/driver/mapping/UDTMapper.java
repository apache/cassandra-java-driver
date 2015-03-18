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

    T toEntity(UDTValue udtValue) {
        T entity = entityMapper.newEntity();
        for (ColumnMapper<T> cm : entityMapper.allColumns()) {
            ByteBuffer bytes = udtValue.getBytesUnsafe(cm.getColumnName());
            if (bytes != null)
                cm.setValue(entity, cm.getDataType().deserialize(bytes, UDT_PROTOCOL_VERSION));
        }
        return entity;
    }

    /**
     * Handles a (possibly nested) collection where some of the elements are domain classes that
     * must be converted to {@code UDTValue} instances.
     */
    @SuppressWarnings("unchecked")
    static Object convertEntitiesToUDTs(Object value, InferredCQLType type) {
        if (value == null)
            return null;

        if (!type.containsMappedUDT)
            return value;

        if (type.udtMapper != null)
            return type.udtMapper.toUDT(value);

        if (type.dataType.getName() == DataType.Name.LIST) {
            InferredCQLType elementType = type.childTypes.get(0);
            List<Object> result = new ArrayList<Object>();
            for (Object element : (List<Object>)value)
                result.add(convertEntitiesToUDTs(element, elementType));
            return result;
        }

        if (type.dataType.getName() == DataType.Name.SET) {
            InferredCQLType elementType = type.childTypes.get(0);
            Set<Object> result = new LinkedHashSet<Object>();
            for (Object element : (Set<Object>)value)
                result.add(convertEntitiesToUDTs(element, elementType));
            return result;
        }

        if (type.dataType.getName() == DataType.Name.MAP) {
            InferredCQLType keyType = type.childTypes.get(0);
            InferredCQLType valueType = type.childTypes.get(1);
            Map<Object, Object> result = new LinkedHashMap<Object, Object>();
            for (Entry<Object, Object> entry : ((Map<Object, Object>)value).entrySet())
                result.put(
                    convertEntitiesToUDTs(entry.getKey(), keyType),
                    convertEntitiesToUDTs(entry.getValue(), valueType)
                );
            return result;
        }
        throw new IllegalArgumentException("Error converting " + value);
    }

    /**
     * Handles a (possibly nested) collection where some of the elements are {@code UDTValue}
     * instances that must be converted to domain classes.
     */
    @SuppressWarnings("unchecked")
    static Object convertUDTsToEntities(Object value, InferredCQLType type) {
        if (value == null)
            return null;

        if (!type.containsMappedUDT)
            return value;

        if (type.udtMapper != null)
            return type.udtMapper.toEntity((UDTValue)value);

        if (type.dataType.getName() == DataType.Name.LIST) {
            InferredCQLType elementType = type.childTypes.get(0);
            List<Object> result = new ArrayList<Object>();
            for (Object element : (List<Object>)value)
                result.add(convertUDTsToEntities(element, elementType));
            return result;
        }

        if (type.dataType.getName() == DataType.Name.SET) {
            InferredCQLType elementType = type.childTypes.get(0);
            Set<Object> result = new LinkedHashSet<Object>();
            for (Object element : (Set<Object>)value)
                result.add(convertUDTsToEntities(element, elementType));
            return result;
        }

        if (type.dataType.getName() == DataType.Name.MAP) {
            InferredCQLType keyType = type.childTypes.get(0);
            InferredCQLType valueType = type.childTypes.get(1);
            Map<Object, Object> result = new LinkedHashMap<Object, Object>();
            for (Entry<Object, Object> entry : ((Map<Object, Object>)value).entrySet())
                result.put(
                    convertUDTsToEntities(entry.getKey(), keyType),
                    convertUDTsToEntities(entry.getValue(), valueType)
                );
            return result;
        }
        throw new IllegalArgumentException("Error converting " + value);
    }
}
