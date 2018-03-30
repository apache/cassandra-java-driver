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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ListCodecTest extends CodecTestBase<List<Integer>> {

  @Mock private TypeCodec<Integer> elementCodec;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    Mockito.when(elementCodec.getCqlType()).thenReturn(DataTypes.INT);
    Mockito.when(elementCodec.getJavaType()).thenReturn(GenericType.INTEGER);
    codec = TypeCodecs.listOf(elementCodec);
  }

  @Test
  public void should_encode_null() {
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_encode_empty_list() {
    assertThat(encode(new ArrayList<>())).isEqualTo("0x00000000");
  }

  @Test
  public void should_encode_non_empty_list() {
    Mockito.when(elementCodec.encode(1, ProtocolVersion.DEFAULT))
        .thenReturn(Bytes.fromHexString("0x01"));
    Mockito.when(elementCodec.encode(2, ProtocolVersion.DEFAULT))
        .thenReturn(Bytes.fromHexString("0x0002"));
    Mockito.when(elementCodec.encode(3, ProtocolVersion.DEFAULT))
        .thenReturn(Bytes.fromHexString("0x000003"));

    assertThat(encode(ImmutableList.of(1, 2, 3)))
        .isEqualTo(
            "0x"
                + "00000003" // number of elements
                + "0000000101" // size + contents of element 1
                + "000000020002" // size + contents of element 2
                + "00000003000003" // size + contents of element 3
            );
  }

  @Test
  public void should_decode_null_as_empty_list() {
    assertThat(decode(null)).isEmpty();
  }

  @Test
  public void should_decode_empty_list() {
    assertThat(decode("0x00000000")).isEmpty();
  }

  @Test
  public void should_decode_non_empty_list() {
    Mockito.when(elementCodec.decode(Bytes.fromHexString("0x01"), ProtocolVersion.DEFAULT))
        .thenReturn(1);
    Mockito.when(elementCodec.decode(Bytes.fromHexString("0x0002"), ProtocolVersion.DEFAULT))
        .thenReturn(2);
    Mockito.when(elementCodec.decode(Bytes.fromHexString("0x000003"), ProtocolVersion.DEFAULT))
        .thenReturn(3);

    assertThat(decode("0x" + "00000003" + "0000000101" + "000000020002" + "00000003000003"))
        .containsExactly(1, 2, 3);
  }

  @Test
  public void should_format_null_list() {
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_format_empty_list() {
    assertThat(format(new ArrayList<>())).isEqualTo("[]");
  }

  @Test
  public void should_format_non_empty_list() {
    Mockito.when(elementCodec.format(1)).thenReturn("a");
    Mockito.when(elementCodec.format(2)).thenReturn("b");
    Mockito.when(elementCodec.format(3)).thenReturn("c");

    assertThat(format(ImmutableList.of(1, 2, 3))).isEqualTo("[a,b,c]");
  }

  @Test
  public void should_parse_null_or_empty_string() {
    assertThat(parse(null)).isNull();
    assertThat(parse("")).isNull();
  }

  @Test
  public void should_parse_empty_list() {
    assertThat(parse("[]")).isEmpty();
  }

  @Test
  public void should_parse_non_empty_list() {
    Mockito.when(elementCodec.parse("a")).thenReturn(1);
    Mockito.when(elementCodec.parse("b")).thenReturn(2);
    Mockito.when(elementCodec.parse("c")).thenReturn(3);

    assertThat(parse("[a,b,c]")).containsExactly(1, 2, 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_malformed_list() {
    parse("not a list");
  }
}
