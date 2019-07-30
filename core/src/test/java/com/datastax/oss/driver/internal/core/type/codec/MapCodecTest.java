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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MapCodecTest extends CodecTestBase<Map<String, Integer>> {

  @Mock private TypeCodec<String> keyCodec;
  @Mock private TypeCodec<Integer> valueCodec;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(keyCodec.getCqlType()).thenReturn(DataTypes.TEXT);
    when(keyCodec.getJavaType()).thenReturn(GenericType.STRING);

    when(valueCodec.getCqlType()).thenReturn(DataTypes.INT);
    when(valueCodec.getJavaType()).thenReturn(GenericType.INTEGER);
    codec = TypeCodecs.mapOf(keyCodec, valueCodec);
  }

  @Test
  public void should_encode_null() {
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_encode_empty_map() {
    assertThat(encode(new LinkedHashMap<>())).isEqualTo("0x00000000");
  }

  @Test
  public void should_encode_non_empty_map() {
    when(keyCodec.encode("a", ProtocolVersion.DEFAULT)).thenReturn(Bytes.fromHexString("0x10"));
    when(keyCodec.encode("b", ProtocolVersion.DEFAULT)).thenReturn(Bytes.fromHexString("0x2000"));
    when(keyCodec.encode("c", ProtocolVersion.DEFAULT)).thenReturn(Bytes.fromHexString("0x300000"));

    when(valueCodec.encode(1, ProtocolVersion.DEFAULT)).thenReturn(Bytes.fromHexString("0x01"));
    when(valueCodec.encode(2, ProtocolVersion.DEFAULT)).thenReturn(Bytes.fromHexString("0x0002"));
    when(valueCodec.encode(3, ProtocolVersion.DEFAULT)).thenReturn(Bytes.fromHexString("0x000003"));

    assertThat(encode(ImmutableMap.of("a", 1, "b", 2, "c", 3)))
        .isEqualTo(
            "0x"
                + "00000003" // number of key-value pairs
                + "0000000110" // size + contents of key 1
                + "0000000101" // size + contents of value 1
                + "000000022000" // size + contents of key 2
                + "000000020002" // size + contents of value 2
                + "00000003300000" // size + contents of key 3
                + "00000003000003" // size + contents of value 3
            );
  }

  @Test
  public void should_decode_null_as_empty_map() {
    assertThat(decode(null)).isEmpty();
  }

  @Test
  public void should_decode_empty_map() {
    assertThat(decode("0x00000000")).isEmpty();
  }

  @Test
  public void should_decode_non_empty_map() {
    when(keyCodec.decode(Bytes.fromHexString("0x10"), ProtocolVersion.DEFAULT)).thenReturn("a");
    when(keyCodec.decode(Bytes.fromHexString("0x2000"), ProtocolVersion.DEFAULT)).thenReturn("b");
    when(keyCodec.decode(Bytes.fromHexString("0x300000"), ProtocolVersion.DEFAULT)).thenReturn("c");

    when(valueCodec.decode(Bytes.fromHexString("0x01"), ProtocolVersion.DEFAULT)).thenReturn(1);
    when(valueCodec.decode(Bytes.fromHexString("0x0002"), ProtocolVersion.DEFAULT)).thenReturn(2);
    when(valueCodec.decode(Bytes.fromHexString("0x000003"), ProtocolVersion.DEFAULT)).thenReturn(3);

    assertThat(
            decode(
                "0x"
                    + "00000003"
                    + "0000000110"
                    + "0000000101"
                    + "000000022000"
                    + "000000020002"
                    + "00000003300000"
                    + "00000003000003"))
        .containsOnlyKeys("a", "b", "c")
        .containsEntry("a", 1)
        .containsEntry("b", 2)
        .containsEntry("c", 3);
  }

  @Test
  public void should_decode_map_with_null_elements() {
    when(keyCodec.decode(Bytes.fromHexString("0x10"), ProtocolVersion.DEFAULT)).thenReturn("a");
    when(valueCodec.decode(Bytes.fromHexString("0x0002"), ProtocolVersion.DEFAULT)).thenReturn(2);
    assertThat(decode("0x" + "00000002" + "0000000110" + "FFFFFFFF" + "FFFFFFFF" + "000000020002"))
        .containsOnlyKeys("a", null)
        .containsEntry("a", null)
        .containsEntry(null, 2);
  }

  @Test
  public void should_format_null_map() {
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_format_empty_map() {
    assertThat(format(new LinkedHashMap<>())).isEqualTo("{}");
  }

  @Test
  public void should_format_non_empty_map() {
    when(keyCodec.format("a")).thenReturn("foo");
    when(keyCodec.format("b")).thenReturn("bar");
    when(keyCodec.format("c")).thenReturn("baz");

    when(valueCodec.format(1)).thenReturn("qux");
    when(valueCodec.format(2)).thenReturn("quux");
    when(valueCodec.format(3)).thenReturn("quuz");

    assertThat(format(ImmutableMap.of("a", 1, "b", 2, "c", 3)))
        .isEqualTo("{foo:qux,bar:quux,baz:quuz}");
  }

  @Test
  public void should_parse_null_or_empty_string() {
    assertThat(parse(null)).isNull();
    assertThat(parse("")).isNull();
  }

  @Test
  public void should_parse_empty_map() {
    assertThat(parse("{}")).isEmpty();
  }

  @Test
  public void should_parse_non_empty_map() {
    when(keyCodec.parse("foo")).thenReturn("a");
    when(keyCodec.parse("bar")).thenReturn("b");
    when(keyCodec.parse("baz")).thenReturn("c");

    when(valueCodec.parse("qux")).thenReturn(1);
    when(valueCodec.parse("quux")).thenReturn(2);
    when(valueCodec.parse("quuz")).thenReturn(3);

    assertThat(parse("{foo:qux,bar:quux,baz:quuz}"))
        .containsOnlyKeys("a", "b", "c")
        .containsEntry("a", 1)
        .containsEntry("b", 2)
        .containsEntry("c", 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_malformed_map() {
    parse("not a map");
  }
}
