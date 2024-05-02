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
package com.datastax.dse.driver.internal.core.type.codec.time;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.dse.driver.api.core.type.codec.DseTypeCodecs;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.shaded.guava.common.base.MoreObjects;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.nio.ByteBuffer;
import java.text.ParseException;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DateRangeCodecTest {

  @Test
  @UseDataProvider("dateRanges")
  public void should_encode_and_decode(DateRange dateRange) {
    TypeCodec<DateRange> codec = DseTypeCodecs.DATE_RANGE;
    DateRange decoded =
        codec.decode(codec.encode(dateRange, ProtocolVersion.DEFAULT), ProtocolVersion.DEFAULT);
    assertThat(decoded).isEqualTo(dateRange);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_encode_unknown_date_range_type() {
    DseTypeCodecs.DATE_RANGE.decode(ByteBuffer.wrap(new byte[] {127}), ProtocolVersion.DEFAULT);
  }

  @Test
  @UseDataProvider("dateRangeStrings")
  public void should_format_and_parse(String dateRangeString) {
    TypeCodec<DateRange> codec = DseTypeCodecs.DATE_RANGE;
    String formatted = codec.format(codec.parse(dateRangeString));
    assertThat(formatted).isEqualTo(MoreObjects.firstNonNull(dateRangeString, "NULL"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_string() {
    DseTypeCodecs.DATE_RANGE.parse("foo");
  }

  @DataProvider
  public static Object[][] dateRanges() throws ParseException {
    return new Object[][] {
      {null},
      {DateRange.parse("[2011-01 TO 2015]")},
      {DateRange.parse("[2010-01-02 TO 2015-05-05T13]")},
      {DateRange.parse("[1973-06-30T13:57:28.123Z TO 1999-05-05T14:14:59]")},
      {DateRange.parse("[2010-01-01T15 TO 2016-02]")},
      {DateRange.parse("[1500 TO 1501]")},
      {DateRange.parse("[0001-01-01 TO 0001-01-01]")},
      {DateRange.parse("[0001-01-01 TO 0001-01-02]")},
      {DateRange.parse("[0000-01-01 TO 0000-01-01]")},
      {DateRange.parse("[0000-01-01 TO 0000-01-02]")},
      {DateRange.parse("[-0001-01-01 TO -0001-01-01]")},
      {DateRange.parse("[-0001-01-01 TO -0001-01-02]")},
      {DateRange.parse("[* TO 2014-12-01]")},
      {DateRange.parse("[1999 TO *]")},
      {DateRange.parse("[* TO *]")},
      {DateRange.parse("-0009")},
      {DateRange.parse("2000-11")},
      {DateRange.parse("*")}
    };
  }

  @DataProvider
  public static Object[][] dateRangeStrings() {
    return new Object[][] {
      {null},
      {"NULL"},
      {"'[2011-01 TO 2015]'"},
      {"'[2010-01-02 TO 2015-05-05T13]'"},
      {"'[1973-06-30T13:57:28.123Z TO 1999-05-05T14:14:59]'"},
      {"'[2010-01-01T15 TO 2016-02]'"},
      {"'[1500 TO 1501]'"},
      {"'[0001-01-01 TO 0001-01-01]'"},
      {"'[0001-01-01 TO 0001-01-02]'"},
      {"'[0000-01-01 TO 0000-01-01]'"},
      {"'[0000-01-01 TO 0000-01-02]'"},
      {"'[-0001-01-01 TO -0001-01-01]'"},
      {"'[-0001-01-01 TO -0001-01-02]'"},
      {"'[* TO 2014-12-01]'"},
      {"'[1999 TO *]'"},
      {"'[* TO *]'"},
      {"'-0009'"},
      {"'2000-11'"},
      {"'*'"}
    };
  }
}
