/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.type.codec.extras.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CodecTestBase;
import com.datastax.oss.driver.internal.core.type.codec.TimestampCodecTest;
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
public class LocalTimestampCodecTest extends CodecTestBase<LocalDateTime> {

  @Test
  public void should_encode() {
    codec = ExtraTypeCodecs.LOCAL_TIMESTAMP_UTC;
    assertThat(encode(Instant.EPOCH.atZone(ZoneOffset.UTC).toLocalDateTime()))
        .isEqualTo("0x0000000000000000");
    assertThat(encode(Instant.ofEpochMilli(128).atZone(ZoneOffset.UTC).toLocalDateTime()))
        .isEqualTo("0x0000000000000080");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    codec = ExtraTypeCodecs.LOCAL_TIMESTAMP_UTC;
    assertThat(decode("0x0000000000000000"))
        .isEqualTo(Instant.EPOCH.atZone(ZoneOffset.UTC).toLocalDateTime());
    assertThat(decode("0x0000000000000080"))
        .isEqualTo(Instant.ofEpochMilli(128).atZone(ZoneOffset.UTC).toLocalDateTime());
    assertThat(decode(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_not_enough_bytes() {
    codec = ExtraTypeCodecs.LOCAL_TIMESTAMP_SYSTEM;
    decode("0x0000");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_too_many_bytes() {
    codec = ExtraTypeCodecs.LOCAL_TIMESTAMP_SYSTEM;
    decode("0x0000000000000000" + "0000");
  }

  @Test
  public void should_format() {
    codec = ExtraTypeCodecs.localTimestampAt(ZoneOffset.ofHours(2));
    // No need to test various values because the codec delegates directly to SimpleDateFormat,
    // which we assume does its job correctly.
    assertThat(format(LocalDateTime.parse("2018-08-16T16:59:34.123")))
        .isEqualTo("'2018-08-16T16:59:34.123+02:00'");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  @UseDataProvider(value = "timeZones", location = TimestampCodecTest.class)
  public void should_parse(ZoneId defaultTimeZone) {
    codec = ExtraTypeCodecs.localTimestampAt(defaultTimeZone);

    // Raw numbers
    assertThat(parse("'0'")).isEqualTo(Instant.EPOCH.atZone(defaultTimeZone).toLocalDateTime());
    assertThat(parse("'-1'"))
        .isEqualTo(Instant.EPOCH.minusMillis(1).atZone(defaultTimeZone).toLocalDateTime());
    assertThat(parse("1534463100000"))
        .isEqualTo(Instant.ofEpochMilli(1534463100000L).atZone(defaultTimeZone).toLocalDateTime());

    // Date formats
    LocalDateTime expected;

    // date without time, without time zone
    expected = LocalDate.parse("2017-01-01").atStartOfDay();
    assertThat(parse("'2017-01-01'")).isEqualTo(expected);

    // date without time, with time zone
    expected =
        ZonedDateTime.parse("2018-08-16T00:00:00+02:00")
            .withZoneSameInstant(defaultTimeZone)
            .toLocalDateTime();
    assertThat(parse("'2018-08-16+02'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16+0200'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16+02:00'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 CEST'")).isEqualTo(expected);

    // date with time, without time zone
    expected = LocalDateTime.parse("2018-08-16T23:45");
    assertThat(parse("'2018-08-16T23:45'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 23:45'")).isEqualTo(expected);

    // date with time + seconds, without time zone
    expected = LocalDateTime.parse("2019-12-31T16:08:38");
    assertThat(parse("'2019-12-31T16:08:38'")).isEqualTo(expected);
    assertThat(parse("'2019-12-31 16:08:38'")).isEqualTo(expected);

    // date with time + seconds + milliseconds, without time zone
    expected = LocalDateTime.parse("1950-02-28T12:00:59.230");
    assertThat(parse("'1950-02-28T12:00:59.230'")).isEqualTo(expected);
    assertThat(parse("'1950-02-28 12:00:59.230'")).isEqualTo(expected);

    // date with time, with time zone
    expected =
        ZonedDateTime.parse("1973-06-23T23:59:00.000+01:00")
            .withZoneSameInstant(defaultTimeZone)
            .toLocalDateTime();
    assertThat(parse("'1973-06-23T23:59+01'")).isEqualTo(expected);
    assertThat(parse("'1973-06-23T23:59+0100'")).isEqualTo(expected);
    assertThat(parse("'1973-06-23T23:59+01:00'")).isEqualTo(expected);
    assertThat(parse("'1973-06-23T23:59 CET'")).isEqualTo(expected);
    assertThat(parse("'1973-06-23 23:59+01'")).isEqualTo(expected);
    assertThat(parse("'1973-06-23 23:59+0100'")).isEqualTo(expected);
    assertThat(parse("'1973-06-23 23:59+01:00'")).isEqualTo(expected);
    assertThat(parse("'1973-06-23 23:59 CET'")).isEqualTo(expected);

    // date with time + seconds, with time zone
    expected =
        ZonedDateTime.parse("1980-01-01T23:59:59.000-08:00")
            .withZoneSameInstant(defaultTimeZone)
            .toLocalDateTime();
    assertThat(parse("'1980-01-01T23:59:59-08'")).isEqualTo(expected);
    assertThat(parse("'1980-01-01T23:59:59-0800'")).isEqualTo(expected);
    assertThat(parse("'1980-01-01T23:59:59-08:00'")).isEqualTo(expected);
    assertThat(parse("'1980-01-01T23:59:59 PST'")).isEqualTo(expected);
    assertThat(parse("'1980-01-01 23:59:59-08'")).isEqualTo(expected);
    assertThat(parse("'1980-01-01 23:59:59-0800'")).isEqualTo(expected);
    assertThat(parse("'1980-01-01 23:59:59-08:00'")).isEqualTo(expected);
    assertThat(parse("'1980-01-01 23:59:59 PST'")).isEqualTo(expected);

    // date with time + seconds + milliseconds, with time zone
    expected =
        ZonedDateTime.parse("1999-12-31T23:59:59.999+00:00")
            .withZoneSameInstant(defaultTimeZone)
            .toLocalDateTime();
    assertThat(parse("'1999-12-31T23:59:59.999+00'")).isEqualTo(expected);
    assertThat(parse("'1999-12-31T23:59:59.999+0000'")).isEqualTo(expected);
    assertThat(parse("'1999-12-31T23:59:59.999+00:00'")).isEqualTo(expected);
    assertThat(parse("'1999-12-31T23:59:59.999 UTC'")).isEqualTo(expected);
    assertThat(parse("'1999-12-31 23:59:59.999+00'")).isEqualTo(expected);
    assertThat(parse("'1999-12-31 23:59:59.999+0000'")).isEqualTo(expected);
    assertThat(parse("'1999-12-31 23:59:59.999+00:00'")).isEqualTo(expected);
    assertThat(parse("'1999-12-31 23:59:59.999 UTC'")).isEqualTo(expected);

    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test
  public void should_fail_to_parse_invalid_input() {
    codec = ExtraTypeCodecs.LOCAL_TIMESTAMP_SYSTEM;
    assertThatThrownBy(() -> parse("not a timestamp"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Alphanumeric timestamp literal must be quoted: \"not a timestamp\"");
    assertThatThrownBy(() -> parse("'not a timestamp'"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse timestamp value from \"'not a timestamp'\"");
  }

  @Test
  public void should_accept_generic_type() {
    codec = ExtraTypeCodecs.LOCAL_TIMESTAMP_SYSTEM;
    assertThat(codec.accepts(GenericType.LOCAL_DATE_TIME)).isTrue();
    assertThat(codec.accepts(GenericType.INSTANT)).isFalse();
  }

  @Test
  public void should_accept_raw_type() {
    codec = ExtraTypeCodecs.LOCAL_TIMESTAMP_SYSTEM;
    assertThat(codec.accepts(LocalDateTime.class)).isTrue();
    assertThat(codec.accepts(Instant.class)).isFalse();
  }

  @Test
  public void should_accept_object() {
    codec = ExtraTypeCodecs.LOCAL_TIMESTAMP_SYSTEM;
    assertThat(codec.accepts(LocalDateTime.now(ZoneId.systemDefault()))).isTrue();
    assertThat(codec.accepts(Instant.EPOCH)).isFalse();
  }
}
