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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;

/**
 * A collection of convenience {@link TypeCodec} instances useful for
 * serializing between CQL types and Joda types like {@link DateTime}.
 *
 * These codecs may either be registered individually or they may all
 * be registered via {@link JodaCodecs#withCodecs(CodecRegistry)}.
 */
public class JodaCodecs {

    /**
     * <p>
     *     Registers all defined {@link TypeCodec} instances in this class
     *     (with exception to {@link TimeZonePreservingDateTimeCodec}) with
     *     the given {@link CodecRegistry}.
     * </p>
     * @param registry registry to add Codecs to.
     * @return updated registry with codecs.
     */
    public static CodecRegistry withCodecs(CodecRegistry registry) {
        return registry.register(LocalTimeCodec.instance, LocalDateCodec.instance, DateTimeCodec.instance);
    }

    /**
     * {@link TypeCodec} that maps {@link LocalTime} <-> Long representing nanoseconds since midnight
     * allowing the setting and retrieval of <code>time</code> columns as {@link LocalTime}
     * instances.
     *
     * IMPORTANT: {@link LocalTime} as millisecond precision; nanoseconds below one millisecond will be lost
     * during deserialization.
     */
    public static class LocalTimeCodec extends MappingCodec<LocalTime, Long> {

        public static final LocalTimeCodec instance = new LocalTimeCodec();

        private LocalTimeCodec() {
            super(TypeCodec.timeCodec(), LocalTime.class);
        }

        @Override
        protected LocalTime deserialize(Long value) {
            if (value == null)
                return null;
            return LocalTime.fromMillisOfDay(TimeUnit.NANOSECONDS.toMillis(value));
        }

        @Override
        protected Long serialize(LocalTime value) {
            if (value == null)
                return null;
            return TimeUnit.MILLISECONDS.toNanos(value.getMillisOfDay());
        }
    }

    /**
     * {@link TypeCodec} that maps
     * {@link org.joda.time.LocalDate} <-> {@link LocalDate} allowing the
     * setting and retrieval of <code>date</code> columns as
     * {@link org.joda.time.LocalDate} instances.
     */
    public static class LocalDateCodec extends MappingCodec<org.joda.time.LocalDate, LocalDate> {

        public static final LocalDateCodec instance = new LocalDateCodec();

        private LocalDateCodec() {
            super(TypeCodec.dateCodec(), org.joda.time.LocalDate.class);
        }

        @Override
        protected org.joda.time.LocalDate deserialize(LocalDate value) {
            return value == null ? null : new org.joda.time.LocalDate(value.getYear(), value.getMonth(), value.getDay());
        }

        @Override
        protected LocalDate serialize(org.joda.time.LocalDate value) {
            return value == null ? null : LocalDate.fromYearMonthDay(value.getYear(), value.getMonthOfYear(), value.getDayOfMonth());
        }
    }

    /**
     * <p>
     *     {@link TypeCodec} that maps {@link DateTime} <-> {@link Date}
     *     allowing the setting and retrieval of <code>timestamp</code>
     *     columns as {@link DateTime} instances.
     * </p>
     *
     * <p>
     *     Since C* <code>timestamp</code> columns do not preserve timezones
     *     any attached timezone information will be lost.
     * </p>
     *
     * @see TimeZonePreservingDateTimeCodec
     */
    public static class DateTimeCodec extends MappingCodec<DateTime, Date> {

        public static final DateTimeCodec instance = new DateTimeCodec();

        private DateTimeCodec() {
            super(TypeCodec.timestampCodec(), DateTime.class);
        }

        @Override
        protected DateTime deserialize(Date value) {
            return new DateTime(value);
        }

        @Override
        protected Date serialize(DateTime value) {
            return value.toDate();
        }
    }

    /**
     * <p>
     *     {@link TypeCodec} that maps
     *     {@link LocalTime} <-> <code>tuple&lt;timestamp,varchar&gt;</code>
     *     providing a pattern for maintaining timezone information in
     *     cassandra.
     * </p>
     *
     * <p>
     *     Since cassandra's <code>timestamp</code> type preserves only
     *     milliseconds since epoch, any timezone information passed in with
     *     a {@link Date} instance would normally be lost.  By using a
     *     <code>tuple&lt;timestamp,varchar&gt;</code> a timezone ID can be
     *     persisted in the <code>varchar</code> field such that when the
     *     value is deserialized into a {@link DateTime} the timezone is
     *     preserved.
     * </p>
     *
     */
    public static class TimeZonePreservingDateTimeCodec extends MappingCodec<DateTime, TupleValue> {

        private final TupleType tupleType;

        public TimeZonePreservingDateTimeCodec(TypeCodec<TupleValue> innerCodec) {
            super(innerCodec, DateTime.class);
            tupleType = (TupleType)innerCodec.getCqlType();
            List<DataType> types = tupleType.getComponentTypes();
            checkArgument(
                types.size() == 2 && types.get(0).equals(timestamp()) && types.get(1).equals(varchar()),
                "Expected tuple<timestamp,varchar>, got %s",
                tupleType);
        }

        @Override
        protected DateTime deserialize(TupleValue value) {
            Date date = value.getTimestamp(0);
            String zoneID = value.getString(1);
            return new DateTime(date).withZone(DateTimeZone.forID(zoneID));
        }

        @Override
        protected TupleValue serialize(DateTime value) {
            TupleValue tupleValue = new TupleValue(tupleType);
            tupleValue.setTimestamp(0, value.toDate());
            tupleValue.setString(1, value.getZone().getID());
            return tupleValue;
        }
    }

}
