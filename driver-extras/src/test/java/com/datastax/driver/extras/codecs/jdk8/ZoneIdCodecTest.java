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
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Assertions;
import java.time.ZoneId;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ZoneIdCodecTest {

  @DataProvider(name = "ZoneIdCodecTest.parse")
  public Object[][] parseParameters() {
    return new Object[][] {
      {null, null},
      {"", null},
      {"NULL", null},
      // simple IDs
      {"'Z'", ZoneId.of("Z")},
      {"'+9'", ZoneId.of("+9")},
      {"'-11:00'", ZoneId.of("-11:00")},
      // offset-style IDs
      {"'GMT+07:00'", ZoneId.of("GMT+07:00")},
      {"'UT-1'", ZoneId.of("UT-1")},
      {"'UTC-12:00'", ZoneId.of("UTC-12:00")},
      // region-based IDs
      {"'Asia/Rangoon'", ZoneId.of("Asia/Rangoon")},
      {"'Australia/Darwin'", ZoneId.of("Australia/Darwin")},
      {"'Brazil/West'", ZoneId.of("Brazil/West")},
      {"'Etc/GMT+9'", ZoneId.of("Etc/GMT+9")},
      {"'Europe/Andorra'", ZoneId.of("Europe/Andorra")},
      {"'Pacific/Norfolk'", ZoneId.of("Pacific/Norfolk")}
    };
  }

  @DataProvider(name = "ZoneIdCodecTest.format")
  public Object[][] formatParameters() {
    return new Object[][] {
      {null, "NULL"},
      // simple IDs
      {ZoneId.of("Z"), "'Z'"},
      {
        ZoneId.of("+9"), "'+09:00'",
      },
      {ZoneId.of("-01:00"), "'-01:00'"},
      // offset-style IDs
      {ZoneId.of("GMT+07:00"), "'GMT+07:00'"},
      {
        ZoneId.of("UT+3"), "'UT+03:00'",
      },
      {ZoneId.of("UTC+9"), "'UTC+09:00'"},
      // region-based IDs
      {ZoneId.of("America/Kentucky/Monticello"), "'America/Kentucky/Monticello'"},
      {ZoneId.of("Australia/North"), "'Australia/North'"},
      {ZoneId.of("Etc/GMT+8"), "'Etc/GMT+8'"},
      {ZoneId.of("Europe/Paris"), "'Europe/Paris'"},
      {ZoneId.of("Indian/Christmas"), "'Indian/Christmas'"},
      {ZoneId.of("Pacific/Port_Moresby"), "'Pacific/Port_Moresby'"}
    };
  }

  @Test(groups = "unit", dataProvider = "ZoneIdCodecTest.parse")
  public void should_parse_valid_formats(String input, ZoneId expected) {
    // when
    ZoneId actual = ZoneIdCodec.instance.parse(input);
    // then
    assertThat(actual).isEqualTo(expected);
  }

  @Test(groups = "unit", dataProvider = "ZoneIdCodecTest.format")
  public void should_serialize_and_format_valid_object(ZoneId input, String expected) {
    // when
    String actual = ZoneIdCodec.instance.format(input);
    // then
    Assertions.assertThat(ZoneIdCodec.instance).withProtocolVersion(V4).canSerialize(input);
    assertThat(actual).isEqualTo(expected);
  }
}
