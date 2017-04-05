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
package com.datastax.driver.extras.codecs.enums;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.ParsingCodec;

/**
 * A codec that serializes {@link Enum} instances as CQL {@code varchar}s
 * representing their programmatic names as returned by {@link Enum#name()}.
 * <p/>
 * <strong>Note that this codec relies on the enum constant names;
 * it is therefore vital that enum names never change.</strong>
 *
 * @param <E> The Enum class this codec serializes from and deserializes to.
 */
public class EnumNameCodec<E extends Enum<E>> extends ParsingCodec<E> {

    private final Class<E> enumClass;

    public EnumNameCodec(Class<E> enumClass) {
        this(TypeCodec.varchar(), enumClass);
    }

    public EnumNameCodec(TypeCodec<String> innerCodec, Class<E> enumClass) {
        super(innerCodec, enumClass);
        this.enumClass = enumClass;
    }

    @Override
    protected String toString(E value) {
        return value == null ? null : value.name();
    }

    @Override
    protected E fromString(String value) {
        return value == null ? null : Enum.valueOf(enumClass, value);
    }

}
