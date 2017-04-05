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
package com.datastax.driver.extras.codecs.date;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;
import java.text.ParseException;

import static com.datastax.driver.core.ParseUtils.*;
import static java.lang.Long.parseLong;

/**
 * A {@link TypeCodec} that maps CQL timestamps to Java primitive longs,
 * representing the number of milliseconds since the Epoch.
 * <p/>
 * This codec can serve as a replacement for the driver's built-in
 * {@link TypeCodec#timestamp() timestamp} codec,
 * when application code prefers to deal with raw milliseconds than with
 * {@link java.util.Date} instances.
 */
public class SimpleTimestampCodec extends TypeCodec.PrimitiveLongCodec {

    public static final SimpleTimestampCodec instance = new SimpleTimestampCodec();

    public SimpleTimestampCodec() {
        super(DataType.timestamp());
    }

    @Override
    public ByteBuffer serializeNoBoxing(long value, ProtocolVersion protocolVersion) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(0, value);
        return bb;
    }

    @Override
    public long deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0)
            return 0;
        if (bytes.remaining() != 8)
            throw new InvalidTypeException("Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());
        return bytes.getLong(bytes.position());
    }

    @Override
    public Long parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        // single quotes are optional for long literals, mandatory for date patterns
        // strip enclosing single quotes, if any
        if (isQuoted(value))
            value = unquote(value);

        if (isLongLiteral(value)) {
            try {
                return parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value), e);
            }
        }

        try {
            return parseDate(value).getTime();
        } catch (ParseException e) {
            throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value), e);
        }
    }

    @Override
    public String format(Long value) {
        if (value == null)
            return "NULL";
        return quote(Long.toString(value));
    }

}
