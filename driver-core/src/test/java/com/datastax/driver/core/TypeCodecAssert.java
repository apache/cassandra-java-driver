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

import com.google.common.reflect.TypeToken;
import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/**
 */
@SuppressWarnings("unused")
public class TypeCodecAssert extends AbstractAssert<TypeCodecAssert, TypeCodec> {

    private ProtocolVersion version = TestUtils.getDesiredProtocolVersion();

    protected TypeCodecAssert(TypeCodec<?> actual) {
        super(actual, TypeCodecAssert.class);
    }

    public TypeCodecAssert accepts(Class<?> clazz) {
        return accepts(TypeToken.of(clazz));
    }

    public TypeCodecAssert accepts(TypeToken<?> javaType) {
        assertThat(actual.accepts(javaType)).isTrue();
        return this;
    }

    public TypeCodecAssert doesNotAccept(TypeToken<?> javaType) {
        assertThat(actual.accepts(javaType)).isFalse();
        return this;
    }

    public TypeCodecAssert accepts(Object value) {
        assertThat(actual.accepts(value)).as("Codec %s should accept %s but it does not", actual, value).isTrue();
        return this;
    }

    public TypeCodecAssert doesNotAccept(Object value) {
        assertThat(actual.accepts(value)).isFalse();
        return this;
    }

    public TypeCodecAssert accepts(DataType cqlType) {
        assertThat(actual.accepts(cqlType)).isTrue();
        return this;
    }

    public TypeCodecAssert doesNotAccept(DataType cqlType) {
        assertThat(actual.accepts(cqlType)).isFalse();
        return this;
    }

    public TypeCodecAssert withProtocolVersion(ProtocolVersion version) {
        if (version == null) fail("ProtocolVersion cannot be null");
        this.version = version;
        return this;
    }

    @SuppressWarnings("unchecked")
    public TypeCodecAssert canSerialize(Object value) {
        if (version == null) fail("ProtocolVersion cannot be null");
        try {
            assertThat(actual.deserialize(actual.serialize(value, version), version)).isEqualTo(value);
        } catch (Exception e) {
            fail(String.format("Codec is supposed to serialize this value but it actually doesn't: %s", value), e);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    public TypeCodecAssert cannotSerialize(Object value) {
        if (version == null) fail("ProtocolVersion cannot be null");
        try {
            actual.serialize(value, version);
            fail("Should not have been able to serialize " + value + " with " + actual);
        } catch (Exception e) {
            //ok
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    public TypeCodecAssert cannotFormat(Object value) {
        try {
            actual.format(value);
            fail("Should not have been able to format " + value + " with " + actual);
        } catch (Exception e) {
            // ok
        }
        return this;
    }
}
