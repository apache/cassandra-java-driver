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
package com.datastax.oss.driver.internal.core.type.codec.extras.time;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CodecTestBase;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.Before;
import org.junit.Test;

public class PersistentZonedTimestampCodecTest extends CodecTestBase<ZonedDateTime> {

  private static final ZonedDateTime EPOCH_UTC = Instant.EPOCH.atZone(ZoneOffset.UTC);

  private static final ZonedDateTime EPOCH_MILLIS_CET =
      Instant.ofEpochMilli(128).atZone(ZoneId.of("CET"));

  private static final ZonedDateTime EPOCH_MILLIS_OFFSET =
      Instant.ofEpochMilli(128).atZone(ZoneOffset.ofHours(2));

  private static final ZonedDateTime EPOCH_MILLIS_EUROPE_PARIS =
      Instant.ofEpochMilli(-128).atZone(ZoneId.of("Europe/Paris"));

  private static final String EPOCH_UTC_ENCODED =
      "0x"
          + ("00000008" + "0000000000000000") // size and contents of timestamp
          + ("00000001" + "5a"); // size and contents of zone ID

  private static final String EPOCH_MILLIS_CET_ENCODED =
      "0x"
          + ("00000008" + "0000000000000080") // size and contents of timestamp
          + ("00000003" + "434554"); // size and contents of zone ID

  private static final String EPOCH_MILLIS_OFFSET_ENCODED =
      "0x"
          + ("00000008" + "0000000000000080") // size and contents of timestamp
          + ("00000006" + "2b30323a3030"); // size and contents of zone ID

  private static final String EPOCH_MILLIS_EUROPE_PARIS_ENCODED =
      "0x"
          + ("00000008" + "ffffffffffffff80") // size and contents of timestamp
          + ("0000000c" + "4575726f70652f5061726973"); // size and contents of zone ID

  private static final String EPOCH_UTC_FORMATTED = "('1970-01-01T00:00:00.000Z','Z')";

  private static final String EPOCH_MILLIS_CET_FORMATTED = "('1970-01-01T00:00:00.128Z','CET')";

  private static final String EPOCH_MILLIS_OFFSET_FORMATTED =
      "('1970-01-01T00:00:00.128Z','+02:00')";

  private static final String EPOCH_MILLIS_EUROPE_PARIS_FORMATTED =
      "('1969-12-31T23:59:59.872Z','Europe/Paris')";

  @Before
  public void setup() {
    codec = ExtraTypeCodecs.ZONED_TIMESTAMP_PERSISTED;
  }

  @Test
  public void should_encode() {
    assertThat(encode(EPOCH_UTC)).isEqualTo(EPOCH_UTC_ENCODED);
    assertThat(encode(EPOCH_MILLIS_CET)).isEqualTo(EPOCH_MILLIS_CET_ENCODED);
    assertThat(encode(EPOCH_MILLIS_OFFSET)).isEqualTo(EPOCH_MILLIS_OFFSET_ENCODED);
    assertThat(encode(EPOCH_MILLIS_EUROPE_PARIS)).isEqualTo(EPOCH_MILLIS_EUROPE_PARIS_ENCODED);
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode(EPOCH_UTC_ENCODED)).isEqualTo(EPOCH_UTC);
    assertThat(decode(EPOCH_MILLIS_CET_ENCODED)).isEqualTo(EPOCH_MILLIS_CET);
    assertThat(decode(EPOCH_MILLIS_OFFSET_ENCODED)).isEqualTo(EPOCH_MILLIS_OFFSET);
    assertThat(decode(EPOCH_MILLIS_EUROPE_PARIS_ENCODED)).isEqualTo(EPOCH_MILLIS_EUROPE_PARIS);
    assertThat(decode(null)).isNull();
  }

  @Test
  public void should_format() {
    assertThat(format(EPOCH_UTC)).isEqualTo(EPOCH_UTC_FORMATTED);
    assertThat(format(EPOCH_MILLIS_CET)).isEqualTo(EPOCH_MILLIS_CET_FORMATTED);
    assertThat(format(EPOCH_MILLIS_OFFSET)).isEqualTo(EPOCH_MILLIS_OFFSET_FORMATTED);
    assertThat(format(EPOCH_MILLIS_EUROPE_PARIS)).isEqualTo(EPOCH_MILLIS_EUROPE_PARIS_FORMATTED);
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse(EPOCH_UTC_FORMATTED)).isEqualTo(EPOCH_UTC);
    assertThat(parse(EPOCH_MILLIS_CET_FORMATTED)).isEqualTo(EPOCH_MILLIS_CET);
    assertThat(parse(EPOCH_MILLIS_OFFSET_FORMATTED)).isEqualTo(EPOCH_MILLIS_OFFSET);
    assertThat(parse(EPOCH_MILLIS_EUROPE_PARIS_FORMATTED)).isEqualTo(EPOCH_MILLIS_EUROPE_PARIS);
    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(ZonedDateTime.class))).isTrue();
    assertThat(codec.accepts(GenericType.of(Integer.class))).isFalse();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(ZonedDateTime.class)).isTrue();
    assertThat(codec.accepts(Integer.class)).isFalse();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(ZonedDateTime.now(ZoneOffset.systemDefault()))).isTrue();
    assertThat(codec.accepts(Integer.MIN_VALUE)).isFalse();
  }
}
