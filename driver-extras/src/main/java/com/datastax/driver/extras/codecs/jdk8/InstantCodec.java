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
package com.datastax.driver.extras.codecs.jdk8;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;

import static com.datastax.driver.core.ParseUtils.*;
import static java.lang.Long.parseLong;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.*;

/**
 * {@link TypeCodec} that maps {@link Instant} to CQL {@code timestamp}
 * allowing the setting and retrieval of {@code timestamp}
 * columns as {@link Instant} instances.
 * <p/>
 * Since C* <code>timestamp</code> columns do not preserve timezones
 * any attached timezone information will be lost.
 * <p/>
 * <strong>IMPORTANT</strong>: this codec's {@link #format(Instant) format} method formats
 * timestamps using an ISO-8601 format that includes milliseconds.
 * <strong>This format is incompatible with Cassandra versions < 2.0.9.</strong>
 *
 * @see ZonedDateTimeCodec
 * @see <a href="https://cassandra.apache.org/doc/cql3/CQL-2.2.html#usingtimestamps">Working with timestamps</a>
 * section of CQL specification.
 */
public class InstantCodec extends TypeCodec<Instant> {

    public static final InstantCodec instance = new InstantCodec();

    /**
     * A {@link DateTimeFormatter} that parses (most) of
     * the ISO formats accepted in CQL.
     */
    private static final DateTimeFormatter PARSER = new DateTimeFormatterBuilder()
            .parseCaseSensitive()
            .parseStrict()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalEnd()
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalEnd()
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendZoneId()
            .optionalEnd()
            .toFormatter()
            .withZone(UTC);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxx")
            .withZone(UTC);


    private InstantCodec() {
        super(DataType.timestamp(), Instant.class);
    }

    @Override
    public ByteBuffer serialize(Instant value, ProtocolVersion protocolVersion) {
        return value == null ? null : bigint().serializeNoBoxing(value.toEpochMilli(), protocolVersion);
    }

    @Override
    public Instant deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null || bytes.remaining() == 0 ? null : ofEpochMilli(bigint().deserializeNoBoxing(bytes, protocolVersion));
    }

    @Override
    public String format(Instant value) {
        if (value == null)
            return "NULL";
        return quote(FORMATTER.format(value));
    }

    @Override
    public Instant parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;
        // strip enclosing single quotes, if any
        if (isQuoted(value))
            value = unquote(value);
        if (isLongLiteral(value)) {
            try {
                return ofEpochMilli(parseLong(value));
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }
        try {
            return Instant.from(PARSER.parse(value));
        } catch (DateTimeParseException e) {
            throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
        }
    }

}
