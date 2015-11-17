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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Serializes a class annotated with {@code @UDT} to the corresponding CQL user-defined type.
 */
class MappedUDTCodec<T> extends TypeCodec.AbstractUDTCodec<T> {
    private final UserType cqlUserType;
    private final Class<T> udtClass;
    private final Map<String, ColumnMapper<T>> columnMappers;
    private final CodecRegistry codecRegistry;

    public MappedUDTCodec(UserType cqlUserType, Class<T> udtClass, Map<String, ColumnMapper<T>> columnMappers, MappingManager mappingManager) {
        super(cqlUserType, udtClass);
        this.cqlUserType = cqlUserType;
        this.udtClass = udtClass;
        this.columnMappers = columnMappers;
        this.codecRegistry = mappingManager.getSession().getCluster().getConfiguration().getCodecRegistry();
    }

    @Override
    protected T newInstance() {
        try {
            return udtClass.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Error creating instance of @UDT-annotated class " + udtClass, e);
        }
    }

    @Override
    protected ByteBuffer serializeField(T source, String fieldName, ProtocolVersion protocolVersion) {
        // The parent class passes lowercase names unquoted, but in our internal map of mappers they are always quoted
        if (!fieldName.startsWith("\""))
            fieldName = Metadata.quote(fieldName);

        ColumnMapper<T> columnMapper = columnMappers.get(fieldName);

        if (columnMapper == null)
            return null;

        Object value = columnMapper.getValue(source);

        TypeCodec<Object> codec = columnMapper.getCustomCodec();
        if (codec == null)
            codec = codecRegistry.codecFor(cqlUserType.getFieldType(columnMapper.getColumnName()), columnMapper.getJavaType());

        return codec.serialize(value, protocolVersion);
    }

    @Override
    protected T deserializeAndSetField(ByteBuffer input, T target, String fieldName, ProtocolVersion protocolVersion) {
        if (!fieldName.startsWith("\""))
            fieldName = Metadata.quote(fieldName);

        ColumnMapper<T> columnMapper = columnMappers.get(fieldName);
        if (columnMapper != null) {
            TypeCodec<Object> codec = columnMapper.getCustomCodec();
            if (codec == null)
                codec = codecRegistry.codecFor(cqlUserType.getFieldType(columnMapper.getColumnName()), columnMapper.getJavaType());
            columnMapper.setValue(target, codec.deserialize(input, protocolVersion));
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
