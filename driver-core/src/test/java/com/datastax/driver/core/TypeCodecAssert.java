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
package com.datastax.driver.core;

import com.google.common.reflect.TypeToken;
import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

@SuppressWarnings("unused")
public class TypeCodecAssert<T> extends AbstractAssert<TypeCodecAssert<T>, TypeCodec<T>> {

    private ProtocolVersion version = ProtocolVersion.NEWEST_SUPPORTED;

    protected TypeCodecAssert(TypeCodec<T> actual) {
        super(actual, TypeCodecAssert.class);
    }

    public TypeCodecAssert<T> accepts(TypeToken<?> javaType) {
        assertThat(actual.accepts(javaType)).as("Codec %s should accept %s but it does not", actual, javaType).isTrue();
        return this;
    }

    public TypeCodecAssert<T> doesNotAccept(TypeToken<?> javaType) {
        assertThat(actual.accepts(javaType)).as("Codec %s should not accept %s but it does", actual, javaType).isFalse();
        return this;
    }

    public TypeCodecAssert<T> accepts(Class<?> javaType) {
        assertThat(actual.accepts(javaType)).as("Codec %s should accept %s but it does not", actual, javaType).isTrue();
        return this;
    }

    public TypeCodecAssert<T> doesNotAccept(Class<?> javaType) {
        assertThat(actual.accepts(javaType)).as("Codec %s should not accept %s but it does", actual, javaType).isFalse();
        return this;
    }

    public TypeCodecAssert<T> accepts(Object value) {
        assertThat(actual.accepts(value)).as("Codec %s should accept %s but it does not", actual, value).isTrue();
        return this;
    }

    public TypeCodecAssert<T> doesNotAccept(Object value) {
        assertThat(actual.accepts(value)).as("Codec %s should not accept %s but it does", actual, value).isFalse();
        return this;
    }

    public TypeCodecAssert<T> accepts(DataType cqlType) {
        assertThat(actual.accepts(cqlType)).as("Codec %s should accept %s but it does not", actual, cqlType).isTrue();
        return this;
    }

    public TypeCodecAssert<T> doesNotAccept(DataType cqlType) {
        assertThat(actual.accepts(cqlType)).as("Codec %s should not accept %s but it does", actual, cqlType).isFalse();
        return this;
    }

    public TypeCodecAssert<T> withProtocolVersion(ProtocolVersion version) {
        if (version == null) fail("ProtocolVersion cannot be null");
        this.version = version;
        return this;
    }

    @SuppressWarnings("unchecked")
    public TypeCodecAssert<T> canSerialize(Object value) {
        if (version == null) fail("ProtocolVersion cannot be null");
        try {
            assertThat(actual.deserialize(actual.serialize((T) value, version), version)).isEqualTo(value);
        } catch (Exception e) {
            fail(String.format("Codec is supposed to serialize this value but it actually doesn't: %s", value), e);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    public TypeCodecAssert<T> cannotSerialize(Object value) {
        if (version == null) fail("ProtocolVersion cannot be null");
        try {
            actual.serialize((T) value, version);
            fail("Should not have been able to serialize " + value + " with " + actual);
        } catch (Exception e) {
            //ok
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    public TypeCodecAssert<T> cannotFormat(Object value) {
        try {
            actual.format((T) value);
            fail("Should not have been able to format " + value + " with " + actual);
        } catch (Exception e) {
            // ok
        }
        return this;
    }
}
