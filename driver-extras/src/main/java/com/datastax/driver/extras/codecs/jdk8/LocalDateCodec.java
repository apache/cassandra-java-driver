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
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

import static com.datastax.driver.core.CodecUtils.*;
import static com.datastax.driver.core.ParseUtils.*;
import static java.lang.Long.parseLong;

/**
 * {@link TypeCodec} that maps
 * {@link java.time.LocalDate} to CQL {@code date}.
 */
@IgnoreJDK6Requirement
@SuppressWarnings("Since15")
public class LocalDateCodec extends TypeCodec<java.time.LocalDate> {

    public static final LocalDateCodec instance = new LocalDateCodec();

    private static final java.time.LocalDate EPOCH = java.time.LocalDate.of(1970, 1, 1);

    private LocalDateCodec() {
        super(DataType.date(), java.time.LocalDate.class);
    }

    @Override
    public ByteBuffer serialize(java.time.LocalDate value, ProtocolVersion protocolVersion) {
        if (value == null)
            return null;
        long days = java.time.temporal.ChronoUnit.DAYS.between(EPOCH, value);
        int unsigned = fromSignedToUnsignedInt((int) days);
        return cint().serializeNoBoxing(unsigned, protocolVersion);
    }

    @Override
    public java.time.LocalDate deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        int unsigned = cint().deserializeNoBoxing(bytes, protocolVersion);
        int signed = fromUnsignedToSignedInt(unsigned);
        return EPOCH.plusDays(signed);
    }

    @Override
    public String format(java.time.LocalDate value) {
        if (value == null)
            return "NULL";
        return quote(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE.format(value));
    }

    @Override
    public java.time.LocalDate parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        // single quotes are optional for long literals, mandatory for date patterns
        // strip enclosing single quotes, if any
        if (isQuoted(value))
            value = unquote(value);

        if (isLongLiteral(value)) {
            long raw;
            try {
                raw = parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
            }
            int days;
            try {
                days = fromCqlDateToDaysSinceEpoch(raw);
            } catch (IllegalArgumentException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
            }
            return EPOCH.plusDays(days);
        }

        try {
            return java.time.LocalDate.parse(value, java.time.format.DateTimeFormatter.ISO_LOCAL_DATE);
        } catch (RuntimeException e) {
            throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
        }
    }

}
