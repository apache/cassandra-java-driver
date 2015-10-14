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

import com.datastax.driver.core.utils.CassandraVersion;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

import static com.datastax.driver.core.DataType.*;

@CassandraVersion(major=2.2)
public class TypeCodecJodaIntegrationTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS foo (c1 text PRIMARY KEY, " +
                        "cd date, ctime time, " +
                        "ctimestamp timestamp, " +
                        "ctuple tuple<timestamp,varchar>)");
    }

    @Override
    protected Cluster.Builder configure(Cluster.Builder builder) {
        return builder.withCodecRegistry(JodaCodecs.withCodecs(new CodecRegistry()));
    }

    /**
     * <p>
     * Validates that a <code>time</code> column can be mapped to a {@link LocalTime} by using
     * {@link com.datastax.driver.core.JodaCodecs.LocalTimeCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-721
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_time_to_localtime() {
        long lTime =
            TimeUnit.HOURS.toNanos(12) +
            TimeUnit.MINUTES.toNanos(16) +
            TimeUnit.SECONDS.toNanos(34) +
            TimeUnit.MILLISECONDS.toNanos(999);
        LocalTime time = new LocalTime(12, 16, 34, 999);

        session.execute("insert into foo (c1, ctime) values (?, ?)", "should_map_time_to_localtime", time);

        ResultSet result = session.execute("select ctime from foo where c1=?", "should_map_time_to_localtime");

        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);

        Row row = result.one();
        assertThat(row.get("ctime", LocalTime.class)).isEqualTo(time);
        assertThat(row.getTime("ctime")).isEqualTo(lTime);
    }

    /**
     * <p>
     * Validates that a <code>date</code> column can be mapped to a {@link org.joda.time.LocalDate} by using
     * {@link com.datastax.driver.core.JodaCodecs.LocalDateCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-721
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_date_to_localdate() {
        org.joda.time.LocalDate date = new org.joda.time.LocalDate(2015, 1, 1);
        LocalDate lDate = LocalDate.fromYearMonthDay(2015, 1, 1);

        session.execute("insert into foo (c1, cd) values (?, ?)", "should_map_date_to_localdate", date);

        ResultSet result = session.execute("select cd from foo where c1=?", "should_map_date_to_localdate");

        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);

        Row row = result.one();
        assertThat(row.get("cd", org.joda.time.LocalDate.class)).isEqualTo(date);
        assertThat(row.getDate("cd")).isEqualTo(lDate);
    }


    /**
     * <p>
     * Validates that a <code>timestamp</code> column can be mapped to a {@link DateTime} by using
     * {@link com.datastax.driver.core.JodaCodecs.DateTimeCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-721
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_timestamp_to_datetime() {
        DateTime dateTime = DateTime.parse("2010-06-30T01:20+05:00");
        Date date = dateTime.toDate();
        DateTimeZone timeZone = dateTime.getZone();

        session.execute("insert into foo (c1, ctimestamp) values (?, ?)", "should_map_timestamp_to_datetime", dateTime);

        ResultSet result = session.execute("select ctimestamp from foo where c1=?", "should_map_timestamp_to_datetime");

        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);

        Row row = result.one();
        // Since timezone is not preserved in timestamp type, set it.
        DateTime rDateTime = row.get("ctimestamp", DateTime.class).withZone(timeZone);
        assertThat(rDateTime).isEqualTo(dateTime);
        assertThat(row.getTimestamp("ctimestamp")).isEqualTo(date);
    }


    /**
     * <p>
     * Validates that a <code>tuple&lt;timestamp,text&gt;</code> column can be mapped to a {@link DateTime} by using
     * {@link com.datastax.driver.core.JodaCodecs.TimeZonePreservingDateTimeCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-721
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_tuple_to_datetime() {
        // Register codec that maps DateTime <-> tuple<timestamp,varchar>
        TupleType dateWithTimeZoneType = cluster.getMetadata().newTupleType(timestamp(), varchar());
        JodaCodecs.TimeZonePreservingDateTimeCodec dateTimeCodec = new JodaCodecs.TimeZonePreservingDateTimeCodec(TypeCodec.tuple(dateWithTimeZoneType));
        cluster.getConfiguration().getCodecRegistry().register(dateTimeCodec);

        DateTime dateTime = DateTime.parse("2010-06-30T01:20+05:30");
        DateTimeZone timeZone = dateTime.getZone();

        PreparedStatement insertStmt = session.prepare("insert into foo (c1, ctuple) values (?, ?)");
        session.execute(insertStmt.bind("should_map_tuple_to_datetime", dateTime));

        ResultSet result = session.execute("select ctuple from foo where c1=?", "should_map_tuple_to_datetime");

        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);

        Row row = result.one();
        DateTime rDateTime = row.get("ctuple", DateTime.class);
        // Since timezone is preserved in the tuple, the date times should match perfectly.
        assertThat(rDateTime).isEqualTo(dateTime);
        // Ensure the timezones match as well.
        // (This is just a safety check as the previous assert would have failed otherwise).
        assertThat(rDateTime.getZone()).isEqualTo(timeZone);
    }
}
