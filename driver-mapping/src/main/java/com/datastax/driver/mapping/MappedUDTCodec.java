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

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UserType;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Serializes a class annotated with {@code @UDT} to the corresponding CQL user-defined type.
 */
class MappedUDTCodec<T> extends TypeCodec.AbstractUDTCodec<T> {
    private final UserType cqlUserType;
    private final Class<T> udtClass;
    private final Map<String, AliasedMappedProperty> columnMappers;
    private final CodecRegistry codecRegistry;

    MappedUDTCodec(UserType cqlUserType, Class<T> udtClass, Map<String, AliasedMappedProperty> columnMappers, MappingManager mappingManager) {
        super(cqlUserType, udtClass);
        this.cqlUserType = cqlUserType;
        this.udtClass = udtClass;
        this.columnMappers = columnMappers;
        this.codecRegistry = mappingManager.getSession().getCluster().getConfiguration().getCodecRegistry();
    }

    @Override
    protected T newInstance() {
        return ReflectionUtils.newInstance(udtClass);
    }

    Class<T> getUdtClass() {
        return udtClass;
    }

    @Override
    protected ByteBuffer serializeField(T source, String fieldName, ProtocolVersion protocolVersion) {
        @SuppressWarnings("unchecked")
        AliasedMappedProperty aliasedMappedProperty = columnMappers.get(fieldName);

        if (aliasedMappedProperty == null)
            return null;

        Object value = aliasedMappedProperty.mappedProperty.getValue(source);

        TypeCodec<Object> codec = aliasedMappedProperty.mappedProperty.getCustomCodec();
        if (codec == null)
            codec = codecRegistry.codecFor(cqlUserType.getFieldType(
                    aliasedMappedProperty.mappedProperty.getMappedName()),
                    aliasedMappedProperty.mappedProperty.getPropertyType());

        return codec.serialize(value, protocolVersion);
    }

    @Override
    protected T deserializeAndSetField(ByteBuffer input, T target, String fieldName, ProtocolVersion protocolVersion) {
        @SuppressWarnings("unchecked")
        AliasedMappedProperty aliasedMappedProperty = columnMappers.get(fieldName);
        if (aliasedMappedProperty != null) {
            TypeCodec<Object> codec = aliasedMappedProperty.mappedProperty.getCustomCodec();
            if (codec == null)
                codec = codecRegistry.codecFor(cqlUserType.getFieldType(
                        aliasedMappedProperty.mappedProperty.getMappedName()),
                        aliasedMappedProperty.mappedProperty.getPropertyType());
            aliasedMappedProperty.mappedProperty.setValue(target, codec.deserialize(input, protocolVersion));
        }
        return target;
    }

    @Override
    protected String formatField(T source, String fieldName) {
        @SuppressWarnings("unchecked")
        AliasedMappedProperty aliasedMappedProperty = columnMappers.get(fieldName);
        if (aliasedMappedProperty == null)
            return null;
        Object value = aliasedMappedProperty.mappedProperty.getValue(source);
        TypeCodec<Object> codec = aliasedMappedProperty.mappedProperty.getCustomCodec();
        if (codec == null)
            codec = codecRegistry.codecFor(cqlUserType.getFieldType(
                    aliasedMappedProperty.mappedProperty.getMappedName()),
                    aliasedMappedProperty.mappedProperty.getPropertyType());
        return codec.format(value);
    }

    @Override
    protected T parseAndSetField(String input, T target, String fieldName) {
        @SuppressWarnings("unchecked")
        AliasedMappedProperty aliasedMappedProperty = columnMappers.get(fieldName);
        if (aliasedMappedProperty != null) {
            TypeCodec<Object> codec = aliasedMappedProperty.mappedProperty.getCustomCodec();
            if (codec == null)
                codec = codecRegistry.codecFor(cqlUserType.getFieldType(
                        aliasedMappedProperty.mappedProperty.getMappedName()),
                        aliasedMappedProperty.mappedProperty.getPropertyType());
            aliasedMappedProperty.mappedProperty.setValue(target, codec.parse(input));
        }
        return target;
    }
}
