/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.internal.core.type.codec;

import static java.time.ZoneOffset.ofHours;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class TimestampCodecTest extends CodecTestBase<Instant> {

  public TimestampCodecTest() {
    // force a given timezone for reproducible results in should_format
    codec = new TimestampCodec(ZoneOffset.UTC);
  }

  @Test
  public void should_encode() {
    assertThat(encode(Instant.EPOCH)).isEqualTo("0x0000000000000000");
    assertThat(encode(Instant.ofEpochMilli(128))).isEqualTo("0x0000000000000080");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x0000000000000000").toEpochMilli()).isEqualTo(0);
    assertThat(decode("0x0000000000000080").toEpochMilli()).isEqualTo(128);
    assertThat(decode(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_not_enough_bytes() {
    decode("0x0000");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_too_many_bytes() {
    decode("0x0000000000000000" + "0000");
  }

  @Test
  public void should_format() {
    // No need to test various values because the codec delegates directly to SimpleDateFormat,
    // which we assume does its job correctly.
    assertThat(format(Instant.EPOCH)).isEqualTo("'1970-01-01T00:00:00.000Z'");
    assertThat(format(Instant.parse("2018-08-16T15:59:34.123Z")))
        .isEqualTo("'2018-08-16T15:59:34.123Z'");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @DataProvider
  public static Iterable<ZoneId> timeZones() {
    return Lists.newArrayList(
        ZoneId.systemDefault(),
        ZoneOffset.UTC,
        ZoneOffset.ofHoursMinutes(3, 30),
        ZoneId.of("Europe/Paris"),
        ZoneId.of("GMT+7"));
  }

  @Test
  @UseDataProvider("timeZones")
  public void should_parse(ZoneId defaultTimeZone) {
    TimestampCodec codec = new TimestampCodec(defaultTimeZone);

    // Raw numbers
    assertThat(codec.parse("'0'")).isEqualTo(Instant.EPOCH);
    assertThat(codec.parse("'-1'")).isEqualTo(Instant.EPOCH.minusMillis(1));
    assertThat(codec.parse("1534463100000")).isEqualTo(Instant.ofEpochMilli(1534463100000L));

    // Date formats
    Instant expected;

    // date without time, without time zone
    expected = LocalDate.parse("2017-01-01").atStartOfDay().atZone(defaultTimeZone).toInstant();
    assertThat(codec.parse("'2017-01-01'")).isEqualTo(expected);

    // date without time, with time zone
    expected = LocalDate.parse("2018-08-16").atStartOfDay().atZone(ofHours(2)).toInstant();
    assertThat(codec.parse("'2018-08-16+02'")).isEqualTo(expected);
    assertThat(codec.parse("'2018-08-16+0200'")).isEqualTo(expected);
    assertThat(codec.parse("'2018-08-16+02:00'")).isEqualTo(expected);
    assertThat(codec.parse("'2018-08-16 CEST'")).isEqualTo(expected);

    // date with time, without time zone
    expected = LocalDateTime.parse("2018-08-16T23:45").atZone(defaultTimeZone).toInstant();
    assertThat(codec.parse("'2018-08-16T23:45'")).isEqualTo(expected);
    assertThat(codec.parse("'2018-08-16 23:45'")).isEqualTo(expected);

    // date with time + seconds, without time zone
    expected = LocalDateTime.parse("2019-12-31T16:08:38").atZone(defaultTimeZone).toInstant();
    assertThat(codec.parse("'2019-12-31T16:08:38'")).isEqualTo(expected);
    assertThat(codec.parse("'2019-12-31 16:08:38'")).isEqualTo(expected);

    // date with time + seconds + milliseconds, without time zone
    expected = LocalDateTime.parse("1950-02-28T12:00:59.230").atZone(defaultTimeZone).toInstant();
    assertThat(codec.parse("'1950-02-28T12:00:59.230'")).isEqualTo(expected);
    assertThat(codec.parse("'1950-02-28 12:00:59.230'")).isEqualTo(expected);

    // date with time, with time zone
    expected = ZonedDateTime.parse("1973-06-23T23:59:00.000+01:00").toInstant();
    assertThat(codec.parse("'1973-06-23T23:59+01'")).isEqualTo(expected);
    assertThat(codec.parse("'1973-06-23T23:59+0100'")).isEqualTo(expected);
    assertThat(codec.parse("'1973-06-23T23:59+01:00'")).isEqualTo(expected);
    assertThat(codec.parse("'1973-06-23T23:59 CET'")).isEqualTo(expected);
    assertThat(codec.parse("'1973-06-23 23:59+01'")).isEqualTo(expected);
    assertThat(codec.parse("'1973-06-23 23:59+0100'")).isEqualTo(expected);
    assertThat(codec.parse("'1973-06-23 23:59+01:00'")).isEqualTo(expected);
    assertThat(codec.parse("'1973-06-23 23:59 CET'")).isEqualTo(expected);

    // date with time + seconds, with time zone
    expected = ZonedDateTime.parse("1980-01-01T23:59:59.000-08:00").toInstant();
    assertThat(codec.parse("'1980-01-01T23:59:59-08'")).isEqualTo(expected);
    assertThat(codec.parse("'1980-01-01T23:59:59-0800'")).isEqualTo(expected);
    assertThat(codec.parse("'1980-01-01T23:59:59-08:00'")).isEqualTo(expected);
    assertThat(codec.parse("'1980-01-01T23:59:59 PST'")).isEqualTo(expected);
    assertThat(codec.parse("'1980-01-01 23:59:59-08'")).isEqualTo(expected);
    assertThat(codec.parse("'1980-01-01 23:59:59-0800'")).isEqualTo(expected);
    assertThat(codec.parse("'1980-01-01 23:59:59-08:00'")).isEqualTo(expected);
    assertThat(codec.parse("'1980-01-01 23:59:59 PST'")).isEqualTo(expected);

    // date with time + seconds + milliseconds, with time zone
    expected = ZonedDateTime.parse("1999-12-31T23:59:59.999+00:00").toInstant();
    assertThat(codec.parse("'1999-12-31T23:59:59.999+00'")).isEqualTo(expected);
    assertThat(codec.parse("'1999-12-31T23:59:59.999+0000'")).isEqualTo(expected);
    assertThat(codec.parse("'1999-12-31T23:59:59.999+00:00'")).isEqualTo(expected);
    assertThat(codec.parse("'1999-12-31T23:59:59.999 UTC'")).isEqualTo(expected);
    assertThat(codec.parse("'1999-12-31 23:59:59.999+00'")).isEqualTo(expected);
    assertThat(codec.parse("'1999-12-31 23:59:59.999+0000'")).isEqualTo(expected);
    assertThat(codec.parse("'1999-12-31 23:59:59.999+00:00'")).isEqualTo(expected);
    assertThat(codec.parse("'1999-12-31 23:59:59.999 UTC'")).isEqualTo(expected);

    assertThat(codec.parse("NULL")).isNull();
    assertThat(codec.parse("null")).isNull();
    assertThat(codec.parse("")).isNull();
    assertThat(codec.parse(null)).isNull();
  }

  @Test
  public void should_fail_to_parse_invalid_input() {
    assertThatThrownBy(() -> parse("not a timestamp"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Alphanumeric timestamp literal must be quoted: \"not a timestamp\"");
    assertThatThrownBy(() -> parse("'not a timestamp'"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse timestamp value from \"'not a timestamp'\"");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(Instant.class))).isTrue();
    assertThat(codec.accepts(GenericType.of(Integer.class))).isFalse();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(Instant.class)).isTrue();
    assertThat(codec.accepts(Integer.class)).isFalse();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(Instant.EPOCH)).isTrue();
    assertThat(codec.accepts(Integer.MIN_VALUE)).isFalse();
  }
}
