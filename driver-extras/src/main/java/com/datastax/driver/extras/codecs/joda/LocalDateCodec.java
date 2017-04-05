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
package com.datastax.driver.extras.codecs.joda;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.nio.ByteBuffer;

import static com.datastax.driver.core.CodecUtils.*;
import static com.datastax.driver.core.ParseUtils.*;
import static java.lang.Long.parseLong;
import static org.joda.time.Days.daysBetween;

/**
 * {@link TypeCodec} that maps
 * {@link org.joda.time.LocalDate} to CQL {@code date}.
 */
public class LocalDateCodec extends TypeCodec<LocalDate> {

    public static final LocalDateCodec instance = new LocalDateCodec();

    private static final LocalDate EPOCH = new LocalDate(1970, 1, 1);

    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC();

    private LocalDateCodec() {
        super(DataType.date(), LocalDate.class);
    }

    @Override
    public ByteBuffer serialize(LocalDate value, ProtocolVersion protocolVersion) {
        if (value == null)
            return null;
        Days days = daysBetween(EPOCH, value);
        int unsigned = fromSignedToUnsignedInt(days.getDays());
        return cint().serializeNoBoxing(unsigned, protocolVersion);
    }

    @Override
    public LocalDate deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        int unsigned = cint().deserializeNoBoxing(bytes, protocolVersion);
        int signed = fromUnsignedToSignedInt(unsigned);
        return EPOCH.plusDays(signed);
    }

    @Override
    public String format(LocalDate value) {
        if (value == null)
            return "NULL";
        return quote(FORMATTER.print(value));
    }

    @Override
    public LocalDate parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        // single quotes are optional for long literals, mandatory for date patterns
        // strip enclosing single quotes, if any
        if (isQuoted(value)) {
            value = unquote(value);
        }

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
            return LocalDate.parse(value, FORMATTER);
        } catch (RuntimeException e) {
            throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
        }
    }

}
