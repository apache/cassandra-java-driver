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

import com.google.common.collect.Lists;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Serializes a class annotated with {@code @UDT} to the corresponding CQL user-defined type.
 */
class MappedUDTCodec<T> extends TypeCodec<T> {
    private final UserType cqlUserType;
    private final Class<T> udtClass;
    private final List<ColumnMapper<T>> columnMappers;
    private final CodecRegistry codecRegistry;

    public MappedUDTCodec(UserType cqlUserType, Class<T> udtClass, List<ColumnMapper<T>> columnMappers, MappingManager mappingManager) {
        super(cqlUserType, udtClass);
        this.cqlUserType = cqlUserType;
        this.udtClass = udtClass;
        this.columnMappers = columnMappers;
        this.codecRegistry = mappingManager.getSession().getCluster().getConfiguration().getCodecRegistry();
    }

    @Override
    public ByteBuffer serialize(T sourceObject, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (sourceObject == null)
            return null;

        int size = 0;

        List<ByteBuffer> serializedFields = Lists.newArrayList();
        for (ColumnMapper<T> cm : columnMappers) {
            Object value = cm.getValue(sourceObject);
            TypeCodec<Object> codec = cm.getCustomCodec();
            if (codec == null)
                codec = codecRegistry.codecFor(cqlUserType.getFieldType(cm.getColumnName()), cm.getJavaType());
            ByteBuffer serializedField = codec.serialize(value, protocolVersion);
            size += 4 + (serializedField == null ? 0 : serializedField.remaining());
            serializedFields.add(serializedField);
        }

        ByteBuffer result = ByteBuffer.allocate(size);
        for (ByteBuffer bb : serializedFields) {
            if (bb == null) {
                result.putInt(-1);
            } else {
                result.putInt(bb.remaining());
                result.put(bb.duplicate());
            }
        }
        return (ByteBuffer)result.flip();
    }

    @Override
    public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        ByteBuffer input = bytes.duplicate();

        T targetObject = newInstance();

        for (ColumnMapper<T> cm : columnMappers) {
            int size = input.getInt();
            ByteBuffer serialized = (size < 0) ? null : CodecUtils.readBytes(input, size);
            TypeCodec<Object> codec = cm.getCustomCodec();
            if (codec == null)
                codec = codecRegistry.codecFor(cqlUserType.getFieldType(cm.getColumnName()), cm.getJavaType());
            cm.setValue(targetObject, codec.deserialize(serialized, protocolVersion));
        }

        return targetObject;
    }

    private T newInstance() {
        try {
            return udtClass.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Error creating instance of @UDT-annotated class " + udtClass, e);
        }
    }

    @Override
    public T parse(String value) throws InvalidTypeException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String format(T value) throws InvalidTypeException {
        throw new UnsupportedOperationException();
    }
}
