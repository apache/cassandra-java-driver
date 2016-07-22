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
    private final Map<String, PropertyMapper> columnMappers;
    private final CodecRegistry codecRegistry;

    MappedUDTCodec(UserType cqlUserType, Class<T> udtClass, Map<String, PropertyMapper> columnMappers, MappingManager mappingManager) {
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
        PropertyMapper propertyMapper = columnMappers.get(fieldName);

        if (propertyMapper == null)
            return null;

        Object value = propertyMapper.getValue(source);

        TypeCodec<Object> codec = propertyMapper.customCodec;
        if (codec == null)
            codec = codecRegistry.codecFor(cqlUserType.getFieldType(propertyMapper.columnName), propertyMapper.javaType);

        return codec.serialize(value, protocolVersion);
    }

    @Override
    protected T deserializeAndSetField(ByteBuffer input, T target, String fieldName, ProtocolVersion protocolVersion) {
        PropertyMapper propertyMapper = columnMappers.get(fieldName);
        if (propertyMapper != null) {
            TypeCodec<Object> codec = propertyMapper.customCodec;
            if (codec == null)
                codec = codecRegistry.codecFor(cqlUserType.getFieldType(propertyMapper.columnName), propertyMapper.javaType);
            propertyMapper.setValue(target, codec.deserialize(input, protocolVersion));
        }
        return target;
    }

    @Override
    protected String formatField(T source, String fieldName) {
        // This codec implementation is internal-use only, and its format method is never used
        throw new UnsupportedOperationException();
    }

    @Override
    protected T parseAndSetField(String input, T target, String fieldName) {
        // This codec implementation is internal-use only, and its parse method is never used
        throw new UnsupportedOperationException();
    }
}
