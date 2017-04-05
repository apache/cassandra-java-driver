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
package com.datastax.driver.extras.codecs.jdk8;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

import static com.datastax.driver.core.ParseUtils.quote;

/**
 * {@link TypeCodec} that maps {@link java.time.LocalTime} to CQL {@code time} (long representing nanoseconds since midnight).
 */
@IgnoreJDK6Requirement
@SuppressWarnings("Since15")
public class LocalTimeCodec extends TypeCodec<java.time.LocalTime> {

    public static final LocalTimeCodec instance = new LocalTimeCodec();

    private static final java.time.format.DateTimeFormatter FORMATTER = java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private LocalTimeCodec() {
        super(DataType.time(), java.time.LocalTime.class);
    }

    @Override
    public ByteBuffer serialize(java.time.LocalTime value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null)
            return null;
        return bigint().serializeNoBoxing(value.toNanoOfDay(), protocolVersion);
    }

    @Override
    public java.time.LocalTime deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        long nanosOfDay = bigint().deserializeNoBoxing(bytes, protocolVersion);
        return java.time.LocalTime.ofNanoOfDay(nanosOfDay);
    }

    @Override
    public String format(java.time.LocalTime value) {
        if (value == null)
            return "NULL";
        return quote(FORMATTER.format(value));
    }

    @Override
    public java.time.LocalTime parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        // enclosing single quotes required, even for long literals
        if (!ParseUtils.isQuoted(value))
            throw new InvalidTypeException("time values must be enclosed by single quotes");
        value = value.substring(1, value.length() - 1);

        if (ParseUtils.isLongLiteral(value)) {
            long nanosOfDay;
            try {
                nanosOfDay = Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse time value from \"%s\"", value), e);
            }
            return java.time.LocalTime.ofNanoOfDay(nanosOfDay);
        }

        try {
            return java.time.LocalTime.parse(value);
        } catch (RuntimeException e) {
            throw new InvalidTypeException(String.format("Cannot parse time value from \"%s\"", value), e);
        }
    }

}
