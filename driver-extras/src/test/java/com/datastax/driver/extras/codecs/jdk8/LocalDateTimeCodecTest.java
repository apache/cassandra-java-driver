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
package com.datastax.driver.extras.codecs.jdk8;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static java.time.LocalDateTime.parse;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Assertions;
import java.time.LocalDateTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@SuppressWarnings("Since15")
public class LocalDateTimeCodecTest {

  @DataProvider(name = "LocalDateTimeCodecTest.parse")
  public Object[][] parseParameters() {
    return new Object[][] {
      {null, null},
      {"", null},
      {"NULL", null},
      // timestamps as milliseconds since the Epoch
      {"0", parse("1970-01-01T00:00:00.000")},
      {"1277860847999", parse("2010-06-30T01:20:47.999")},
      // timestamps as valid CQL literals with different precisions
      {"'2010-06-30T01:20'", parse("2010-06-30T01:20:00.000")},
      {"'2010-06-30T01:20:47'", parse("2010-06-30T01:20:47.000")},
      {"'2010-06-30T01:20:47.999'", parse("2010-06-30T01:20:47.999")}
    };
  }

  @DataProvider(name = "LocalDateTimeCodecTest.format")
  public Object[][] formatParameters() {
    return new Object[][] {
      {null, "NULL"},
      {parse("1970-01-01T00:00"), "'1970-01-01T00:00:00'"},
      {parse("1970-01-01T00:00:00"), "'1970-01-01T00:00:00'"},
      {parse("1970-01-01T00:00:00.000"), "'1970-01-01T00:00:00'"},
      {parse("2010-06-30T01:20"), "'2010-06-30T01:20:00'"},
      {parse("2010-06-30T01:20:47"), "'2010-06-30T01:20:47'"}
    };
  }

  @Test(groups = "unit", dataProvider = "LocalDateTimeCodecTest.parse")
  public void should_parse_valid_formats(String input, LocalDateTime expected) {
    // when
    LocalDateTime actual = LocalDateTimeCodec.instance.parse(input);
    // then
    assertThat(actual).isEqualTo(expected);
  }

  @Test(groups = "unit", dataProvider = "LocalDateTimeCodecTest.format")
  public void should_serialize_and_format_valid_object(LocalDateTime input, String expected) {
    // given
    LocalDateTimeCodec codec = LocalDateTimeCodec.instance;
    // when
    String actual = codec.format(input);
    System.out.println(actual);
    // then
    Assertions.assertThat(codec).withProtocolVersion(V4).canSerialize(input);
    assertThat(actual).isEqualTo(expected);
  }
}
