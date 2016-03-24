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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;
import java.util.List;

import static com.datastax.driver.core.ParseUtils.isLongLiteral;
import static com.datastax.driver.core.ParseUtils.quote;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@link TypeCodec} that maps
 * {@link java.time.ZonedDateTime} to CQL {@code tuple<timestamp,varchar>},
 * providing a pattern for maintaining timezone information in
 * Cassandra.
 * <p/>
 * Since Cassandra's <code>timestamp</code> type preserves only
 * milliseconds since epoch, any timezone information
 * would normally be lost. By using a
 * <code>tuple&lt;timestamp,varchar&gt;</code> a timezone ID can be
 * persisted in the <code>varchar</code> field such that when the
 * value is deserialized the timezone is
 * preserved.
 * <p/>
 * <strong>IMPORTANT</strong>: this codec's {@link #format(Object) format} method formats
 * timestamps using an ISO-8601 format that includes milliseconds.
 * <strong>This format is incompatible with Cassandra versions < 2.0.9.</strong>
 *
 * @see <a href="https://cassandra.apache.org/doc/cql3/CQL-2.2.html#usingtimestamps">'Working with timestamps' section of CQL specification</a>
 */
@IgnoreJRERequirement
@SuppressWarnings("Since15")
public class ZonedDateTimeCodec extends TypeCodec.AbstractTupleCodec<java.time.ZonedDateTime> {

    /**
     * A {@link java.time.format.DateTimeFormatter} that parses (most) of
     * the ISO formats accepted in CQL.
     */
    private static final java.time.format.DateTimeFormatter FORMATTER = new java.time.format.DateTimeFormatterBuilder()
            .parseCaseSensitive()
            .parseStrict()
            .append(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral('T')
            .appendValue(java.time.temporal.ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(java.time.temporal.ChronoField.MINUTE_OF_HOUR, 2)
            .optionalEnd()
            .optionalStart()
            .appendLiteral(':')
            .appendValue(java.time.temporal.ChronoField.SECOND_OF_MINUTE, 2)
            .optionalEnd()
            .optionalStart()
            .appendFraction(java.time.temporal.ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .toFormatter()
            .withZone(java.time.ZoneOffset.UTC);

    private static final java.time.format.DateTimeFormatter ZONE_FORMATTER = java.time.format.DateTimeFormatter.ofPattern("xxx");

    public ZonedDateTimeCodec(TupleType tupleType) {
        super(tupleType, java.time.ZonedDateTime.class);
        List<DataType> types = tupleType.getComponentTypes();
        checkArgument(
                types.size() == 2 && types.get(0).equals(DataType.timestamp()) && types.get(1).equals(DataType.varchar()),
                "Expected tuple<timestamp,varchar>, got %s",
                tupleType);
    }

    @Override
    protected java.time.ZonedDateTime newInstance() {
        return null;
    }

    @Override
    protected ByteBuffer serializeField(java.time.ZonedDateTime source, int index, ProtocolVersion protocolVersion) {
        if (index == 0) {
            long millis = source.toInstant().toEpochMilli();
            return bigint().serializeNoBoxing(millis, protocolVersion);
        }
        if (index == 1) {
            return varchar().serialize(ZONE_FORMATTER.format(source.getOffset()), protocolVersion);
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. " + index);
    }

    @Override
    protected java.time.ZonedDateTime deserializeAndSetField(ByteBuffer input, java.time.ZonedDateTime target, int index, ProtocolVersion protocolVersion) {
        if (index == 0) {
            long millis = bigint().deserializeNoBoxing(input, protocolVersion);
            return java.time.Instant.ofEpochMilli(millis).atZone(java.time.ZoneOffset.UTC);
        }
        if (index == 1) {
            String zoneId = varchar().deserialize(input, protocolVersion);
            return target.withZoneSameInstant(java.time.ZoneId.of(zoneId));
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. " + index);
    }

    @Override
    protected String formatField(java.time.ZonedDateTime value, int index) {
        if (index == 0) {
            return quote(FORMATTER.format(value));
        }
        if (index == 1) {
            return quote(ZONE_FORMATTER.format(value.getOffset()));
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. " + index);
    }

    @Override
    protected java.time.ZonedDateTime parseAndSetField(String input, java.time.ZonedDateTime target, int index) {
        if (index == 0) {
            // strip enclosing single quotes, if any
            if (ParseUtils.isQuoted(input))
                input = ParseUtils.unquote(input);
            if (isLongLiteral(input)) {
                try {
                    long millis = Long.parseLong(input);
                    return java.time.ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(millis), java.time.ZoneOffset.UTC);
                } catch (NumberFormatException e) {
                    throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", input));
                }
            }
            try {
                return java.time.ZonedDateTime.from(FORMATTER.parse(input));
            } catch (java.time.format.DateTimeParseException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", target));
            }
        }
        if (index == 1) {
            String zoneId = varchar().parse(input);
            return target.withZoneSameInstant(java.time.ZoneId.of(zoneId));
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. " + index);
    }
}
