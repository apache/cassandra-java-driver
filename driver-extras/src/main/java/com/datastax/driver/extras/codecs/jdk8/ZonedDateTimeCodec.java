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
 * <code>tuple&lt;timestamp,varchar&gt;</code> a timezone can be
 * persisted in the <code>varchar</code> field such that when the
 * value is deserialized the timezone is preserved.
 * <p>
 * <strong>IMPORTANT</strong>
 * <p>
 * 1) The default timestamp formatter used by this codec produces CQL literals
 * that may include milliseconds.
 * <strong>This literal format is incompatible with Cassandra < 2.0.9.</strong>
 * <p>
 * 2) Even if the ISO-8601 standard accepts timestamps with nanosecond precision,
 * Cassandra timestamps have millisecond precision; therefore, any sub-millisecond
 * value set on a {@link java.time.ZonedDateTime} will be lost when persisted to Cassandra.
 *
 * @see <a href="https://cassandra.apache.org/doc/cql3/CQL-2.2.html#usingtimestamps">'Working with timestamps' section of CQL specification</a>
 */
@IgnoreJDK6Requirement
@SuppressWarnings("Since15")
public class ZonedDateTimeCodec extends TypeCodec.AbstractTupleCodec<java.time.ZonedDateTime> {

    /**
     * The default {@link java.time.format.DateTimeFormatter} that parses (most) of
     * the ISO formats accepted in CQL.
     */
    private static final java.time.format.DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = java.time.format.DateTimeFormatter.ISO_DATE_TIME.withZone(java.time.ZoneOffset.UTC);

    /**
     * The default {@link java.time.format.DateTimeFormatter} to parse and format zones.
     * It will use a time-zone ID, such as {@code Europe/Paris}, or an offset, such as {@code +02:00},
     * depending on the best available information.
     */
    private static final java.time.format.DateTimeFormatter DEFAULT_ZONE_FORMATTER = new java.time.format.DateTimeFormatterBuilder()
            .appendZoneOrOffsetId()
            .toFormatter();

    private final java.time.format.DateTimeFormatter dateTimeFormatter;

    private final java.time.format.DateTimeFormatter zoneFormatter;

    /**
     * Creates a new {@link ZonedDateTimeCodec} for the given tuple
     * and with default {@link java.time.format.DateTimeFormatter formatters} for
     * both the timestamp and the zone components.
     * <p>
     * The default formatters produce and parse CQL timestamp literals of the following form:
     * <ol>
     * <li>Timestamp component: an ISO-8601 full date and time pattern, including at least: year,
     * month, day, hour and minutes, and optionally, seconds and milliseconds,
     * followed by the zone ID {@code Z} (UTC),
     * e.g. {@code 2010-06-30T02:01Z} or {@code 2010-06-30T01:20:47.999Z};
     * note that timestamp components are always expressed in UTC time, hence the zone ID {@code Z}.</li>
     * <li>Zone component: a zone offset such as {@code -07:00}, or a zone ID such as {@code UTC} or {@code Europe/Paris},
     * depending on what information is available.</li>
     * </ol>
     *
     * @param tupleType The tuple type this codec should handle.
     *                  It must be a {@code tuple<timestamp,varchar>}.
     * @throws IllegalArgumentException if the provided tuple type is not a {@code tuple<timestamp,varchar>}.
     */
    public ZonedDateTimeCodec(TupleType tupleType) {
        this(tupleType, DEFAULT_DATE_TIME_FORMATTER, DEFAULT_ZONE_FORMATTER);
    }

    /**
     * Creates a new {@link ZonedDateTimeCodec} for the given tuple
     * and with the provided {@link java.time.format.DateTimeFormatter formatters} for
     * the timestamp and the zone components of the tuple.
     * <p>
     * Use this constructor if you intend to customize the way the codec
     * parses and formats timestamps and zones. Beware that Cassandra only accepts
     * timestamp literals in some of the most common ISO-8601 formats;
     * attempting to use non-standard formats could result in invalid CQL literals.
     *
     * @param tupleType         The tuple type this codec should handle.
     *                          It must be a {@code tuple<timestamp,varchar>}.
     * @param dateTimeFormatter The {@link java.time.format.DateTimeFormatter DateTimeFormatter} to use
     *                          to parse and format the timestamp component of the tuple.
     *                          As a parser, it should be lenient enough to accept most of the ISO-8601 formats
     *                          accepted by Cassandra as valid CQL literals.
     *                          As a formatter, it should be configured to always format timestamps in UTC
     *                          (see {@link java.time.format.DateTimeFormatter#withZone(java.time.ZoneId)}.
     * @param zoneFormatter     The {@link java.time.format.DateTimeFormatter DateTimeFormatter} to use
     *                          to parse and format the zone component of the tuple.
     * @throws IllegalArgumentException if the provided tuple type is not a {@code tuple<timestamp,varchar>}.
     */
    public ZonedDateTimeCodec(TupleType tupleType, java.time.format.DateTimeFormatter dateTimeFormatter, java.time.format.DateTimeFormatter zoneFormatter) {
        super(tupleType, java.time.ZonedDateTime.class);
        this.dateTimeFormatter = dateTimeFormatter;
        this.zoneFormatter = zoneFormatter;
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
            return varchar().serialize(zoneFormatter.format(source), protocolVersion);
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
            return target.withZoneSameInstant(zoneFormatter.parse(zoneId, java.time.temporal.TemporalQueries.zone()));
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. " + index);
    }

    @Override
    protected String formatField(java.time.ZonedDateTime value, int index) {
        if (index == 0) {
            return quote(dateTimeFormatter.format(value));
        }
        if (index == 1) {
            return quote(zoneFormatter.format(value));
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
                return java.time.ZonedDateTime.from(dateTimeFormatter.parse(input));
            } catch (java.time.format.DateTimeParseException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", target));
            }
        }
        if (index == 1) {
            String zoneId = varchar().parse(input);
            return target.withZoneSameInstant(zoneFormatter.parse(zoneId, java.time.temporal.TemporalQueries.zone()));
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. " + index);
    }
}
