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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.nio.ByteBuffer;
import java.util.List;

import static com.datastax.driver.core.ParseUtils.isLongLiteral;
import static com.datastax.driver.core.ParseUtils.quote;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@link TypeCodec} that maps
 * {@link DateTime} to CQL {@code tuple<timestamp,varchar>},
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
 * timestamps as CQL literal strings using an ISO-8601 format that includes milliseconds.
 * <strong>This format is incompatible with Cassandra versions < 2.0.9.</strong>
 *
 * @see <a href="https://cassandra.apache.org/doc/cql3/CQL-2.2.html#usingtimestamps">'Working with timestamps' section of CQL specification</a>
 */
public class DateTimeCodec extends TypeCodec.AbstractTupleCodec<DateTime> {

    /**
     * A {@link DateTimeFormatter} that parses (most) of
     * the ISO-8601 formats accepted in CQL.
     */
    private static final DateTimeFormatter DEFAULT_PARSER = ISODateTimeFormat.dateOptionalTimeParser();

    /**
     * A {@link DateTimeFormatter} that prints timestamps
     * with a full ISO-8601 date and time format, including the time zone (Z).
     */
    private static final DateTimeFormatter DEFFAULT_PRINTER = ISODateTimeFormat.dateTime().withZoneUTC();

    private final DateTimeFormatter parser;

    private final DateTimeFormatter printer;

    /**
     * Creates a new {@link DateTimeCodec} for the given tuple,
     * using a default parser and a default printer to handle
     * the timestamp component of the tuple.
     * <p>
     * The default formatter and printer produce and parse CQL timestamp literals of the following form:
     * <ol>
     * <li>The printer will always produce a full ISO-8601 date and time pattern, including year,
     * month, day, hour, minutes, seconds and milliseconds,
     * followed by the zone ID {@code Z} (UTC), e.g. {@code 2010-06-30T01:20:47.999Z};
     * note that timestamp components are always printed in UTC time, hence the zone ID {@code Z}.</li>
     * <li>The parser accepts most ISO-8601 date and time patterns, the time part (minutes, seconds, milliseconds) being optional.</li>
     * </ol>
     * <p>
     * Note that it is not possible to customize the parsing and printing of
     * the zone component of the tuple. This codec prints either a zone offset such as {@code -07:00},
     * or a zone ID such as {@code UTC} or {@code Europe/Paris},
     * depending on what is the best information is available.
     *
     * @param tupleType The tuple type this codec should handle.
     *                  It must be a {@code tuple<timestamp,varchar>}.
     * @throws IllegalArgumentException if the provided tuple type is not a {@code tuple<timestamp,varchar>}.
     */
    public DateTimeCodec(TupleType tupleType) {
        this(tupleType, DEFAULT_PARSER, DEFFAULT_PRINTER);
    }

    /**
     * Creates a new {@link DateTimeCodec} for the given tuple,
     * using the provided {@link DateTimeFormatter parser} and {@link DateTimeFormatter printer}
     * to format and print the timestamp component of the tuple.
     * <p>
     * Use this constructor if you intend to customize the way the codec
     * parses and formats timestamps. Beware that Cassandra only accepts
     * timestamp literals in some of the most common ISO-8601 formats;
     * attempting to use non-standard formats could result in invalid CQL literals.
     * <p>
     * Note that it is not possible to customize the parsing and printing of
     * the zone component of the tuple. This codec prints either a zone offset such as {@code -07:00},
     * or a zone ID such as {@code UTC} or {@code Europe/Paris},
     * depending on what information is available.
     *
     * @param tupleType The tuple type this codec should handle.
     *                  It must be a {@code tuple<timestamp,varchar>}.
     * @param parser    The {@link DateTimeFormatter parser} to use
     *                  to parse the timestamp component of the tuple.
     *                  It should be lenient enough to accept most of the ISO-8601 formats
     *                  accepted by Cassandra as valid CQL literals.
     * @param printer   The {@link DateTimeFormatter printer} to use
     *                  to format the timestamp component of the tuple.
     *                  This printer should be configured to always format timestamps in UTC
     *                  (see {@link DateTimeFormatter#withZoneUTC()}.
     * @throws IllegalArgumentException if the provided tuple type is not a {@code tuple<timestamp,varchar>}.
     */
    public DateTimeCodec(TupleType tupleType, DateTimeFormatter parser, DateTimeFormatter printer) {
        super(tupleType, DateTime.class);
        this.parser = parser;
        this.printer = printer;
        List<DataType> types = tupleType.getComponentTypes();
        checkArgument(
                types.size() == 2 && types.get(0).equals(DataType.timestamp()) && types.get(1).equals(DataType.varchar()),
                "Expected tuple<timestamp,varchar>, got %s",
                tupleType);
    }

    @Override
    protected DateTime newInstance() {
        return null;
    }

    @Override
    protected ByteBuffer serializeField(DateTime source, int index, ProtocolVersion protocolVersion) {
        if (index == 0) {
            long millis = source.getMillis();
            return bigint().serializeNoBoxing(millis, protocolVersion);
        }
        if (index == 1) {
            return varchar().serialize(source.getZone().getID(), protocolVersion);
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. " + index);
    }

    @Override
    protected DateTime deserializeAndSetField(ByteBuffer input, DateTime target, int index, ProtocolVersion protocolVersion) {
        if (index == 0) {
            long millis = bigint().deserializeNoBoxing(input, protocolVersion);
            return new DateTime(millis);
        }
        if (index == 1) {
            String zoneId = varchar().deserialize(input, protocolVersion);
            return target.withZone(DateTimeZone.forID(zoneId));
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. " + index);
    }

    @Override
    protected String formatField(DateTime value, int index) {
        if (index == 0) {
            return quote(printer.print(value));
        }
        if (index == 1) {
            return quote(value.getZone().getID());
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. " + index);
    }

    @Override
    protected DateTime parseAndSetField(String input, DateTime target, int index) {
        if (index == 0) {
            // strip enclosing single quotes, if any
            if (ParseUtils.isQuoted(input))
                input = ParseUtils.unquote(input);
            if (isLongLiteral(input)) {
                try {
                    long millis = Long.parseLong(input);
                    return new DateTime(millis);
                } catch (NumberFormatException e) {
                    throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", input));
                }
            }
            try {
                return parser.parseDateTime(input);
            } catch (RuntimeException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", target));
            }
        }
        if (index == 1) {
            String zoneId = varchar().parse(input);
            // Joda time does not recognize "Z"
            if ("Z".equals(zoneId))
                return target.withZone(DateTimeZone.UTC);
            return target.withZone(DateTimeZone.forID(zoneId));
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. " + index);
    }

}
