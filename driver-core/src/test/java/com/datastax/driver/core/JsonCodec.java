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
package com.datastax.driver.core;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * A simple Json codec for TypeCodec tests.
 */
public class JsonCodec<T> extends TypeCodec.StringParsingCodec<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonCodec(Class<T> javaType) {
        super(javaType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T fromString(String value) {
        if (value == null)
            return null;
        try {
            return (T) objectMapper.readValue(value, getJavaType().getRawType());
        } catch (IOException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
    }

    @Override
    public String toString(T value) {
        if (value == null)
            return null;
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
    }
}
