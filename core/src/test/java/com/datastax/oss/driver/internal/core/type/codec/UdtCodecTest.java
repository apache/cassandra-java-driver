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
package com.datastax.oss.driver.internal.core.type.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveIntCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.data.DefaultUdtValue;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class UdtCodecTest extends CodecTestBase<UdtValue> {

  @Mock private AttachmentPoint attachmentPoint;
  @Mock private CodecRegistry codecRegistry;
  private PrimitiveIntCodec intCodec;
  private TypeCodec<Double> doubleCodec;
  private TypeCodec<String> textCodec;

  private UserDefinedType userType;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(attachmentPoint.getCodecRegistry()).thenReturn(codecRegistry);
    when(attachmentPoint.getProtocolVersion()).thenReturn(ProtocolVersion.DEFAULT);

    intCodec = spy(TypeCodecs.INT);
    doubleCodec = spy(TypeCodecs.DOUBLE);
    textCodec = spy(TypeCodecs.TEXT);

    // Called by the getters/setters
    when(codecRegistry.codecFor(DataTypes.INT, Integer.class)).thenAnswer(i -> intCodec);
    when(codecRegistry.codecFor(DataTypes.DOUBLE, Double.class)).thenAnswer(i -> doubleCodec);
    when(codecRegistry.codecFor(DataTypes.TEXT, String.class)).thenAnswer(i -> textCodec);

    // Called by format/parse
    when(codecRegistry.codecFor(DataTypes.INT)).thenAnswer(i -> intCodec);
    when(codecRegistry.codecFor(DataTypes.DOUBLE)).thenAnswer(i -> doubleCodec);
    when(codecRegistry.codecFor(DataTypes.TEXT)).thenAnswer(i -> textCodec);

    userType =
        new DefaultUserDefinedType(
            CqlIdentifier.fromInternal("ks"),
            CqlIdentifier.fromInternal("type"),
            false,
            ImmutableList.of(
                CqlIdentifier.fromInternal("field1"),
                CqlIdentifier.fromInternal("field2"),
                CqlIdentifier.fromInternal("field3")),
            ImmutableList.of(DataTypes.INT, DataTypes.DOUBLE, DataTypes.TEXT),
            attachmentPoint);

    codec = TypeCodecs.udtOf(userType);
  }

  @Test
  public void should_encode_null_udt() {
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_encode_udt() {
    UdtValue udt = userType.newValue();
    udt = udt.setInt("field1", 1);
    udt = udt.setToNull("field2");
    udt = udt.setString("field3", "a");

    assertThat(encode(udt))
        .isEqualTo(
            "0x"
                + ("00000004" + "00000001") // size and contents of field 0
                + "ffffffff" // null field 1
                + ("00000001" + "61") // size and contents of field 2
            );

    verify(intCodec).encodePrimitive(1, ProtocolVersion.DEFAULT);
    // null values are handled directly in the udt codec, without calling the child codec:
    verifyZeroInteractions(doubleCodec);
    verify(textCodec).encode("a", ProtocolVersion.DEFAULT);
  }

  @Test
  public void should_decode_null_udt() {
    assertThat(decode(null)).isNull();
  }

  @Test
  public void should_decode_udt() {
    UdtValue udt = decode("0x" + ("00000004" + "00000001") + "ffffffff" + ("00000001" + "61"));

    assertThat(udt.getInt(0)).isEqualTo(1);
    assertThat(udt.isNull(1)).isTrue();
    assertThat(udt.getString(2)).isEqualTo("a");

    verify(intCodec).decodePrimitive(Bytes.fromHexString("0x00000001"), ProtocolVersion.DEFAULT);
    verifyZeroInteractions(doubleCodec);
    verify(textCodec).decode(Bytes.fromHexString("0x61"), ProtocolVersion.DEFAULT);
  }

  @Test
  public void should_decode_udt_when_too_many_fields() {
    UdtValue udt =
        decode(
            "0x"
                + ("00000004" + "00000001")
                + "ffffffff"
                + ("00000001" + "61")
                // extra contents
                + "ffffffff");
    assertThat(udt.getInt(0)).isEqualTo(1);
    assertThat(udt.isNull(1)).isTrue();
    assertThat(udt.getString(2)).isEqualTo("a");
  }

  /** Test for JAVA-2557. Ensures that the codec can decode null fields with any negative length. */
  @Test
  public void should_decode_negative_element_length_as_null_field() {
    UdtValue udt =
        decode(
            "0x"
                + "ffffffff" // field1 has length -1
                + "fffffffe" // field2 has length -2
                + "80000000" // field3 has length Integer.MIN_VALUE (-2147483648)
            );

    assertThat(udt.isNull(0)).isTrue();
    assertThat(udt.isNull(1)).isTrue();
    assertThat(udt.isNull(2)).isTrue();

    verifyZeroInteractions(intCodec);
    verifyZeroInteractions(doubleCodec);
    verifyZeroInteractions(textCodec);
  }

  @Test
  public void should_decode_absent_element_as_null_field() {
    UdtValue udt = decode("0x");

    assertThat(udt.isNull(0)).isTrue();
    assertThat(udt.isNull(1)).isTrue();
    assertThat(udt.isNull(2)).isTrue();

    verifyZeroInteractions(intCodec);
    verifyZeroInteractions(doubleCodec);
    verifyZeroInteractions(textCodec);
  }

  @Test
  public void should_format_null_udt() {
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_format_udt() {
    UdtValue udt = userType.newValue();
    udt = udt.setInt(0, 1);
    udt = udt.setToNull(1);
    udt = udt.setString(2, "a");

    assertThat(format(udt)).isEqualTo("{field1:1,field2:NULL,field3:'a'}");

    verify(intCodec).format(1);
    verify(doubleCodec).format(null);
    verify(textCodec).format("a");
  }

  @Test
  public void should_parse_null_udt() {
    assertThat(parse(null)).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("NULL")).isNull();
  }

  @Test
  public void should_parse_empty_udt() {
    UdtValue udt = parse("{}");

    assertThat(udt.isNull(0)).isTrue();
    assertThat(udt.isNull(1)).isTrue();
    assertThat(udt.isNull(2)).isTrue();

    verifyNoMoreInteractions(intCodec);
    verifyNoMoreInteractions(doubleCodec);
    verifyNoMoreInteractions(textCodec);
  }

  @Test
  public void should_parse_partial_udt() {
    UdtValue udt = parse("{field1:1,field2:NULL}");

    assertThat(udt.getInt(0)).isEqualTo(1);
    assertThat(udt.isNull(1)).isTrue();
    assertThat(udt.isNull(2)).isTrue();

    verify(intCodec).parse("1");
    verify(doubleCodec).parse("NULL");
    verifyNoMoreInteractions(textCodec);
  }

  @Test
  public void should_parse_full_udt() {
    UdtValue udt = parse("{field1:1,field2:NULL,field3:'a'}");

    assertThat(udt.getInt(0)).isEqualTo(1);
    assertThat(udt.isNull(1)).isTrue();
    assertThat(udt.getString(2)).isEqualTo("a");

    verify(intCodec).parse("1");
    verify(doubleCodec).parse("NULL");
    verify(textCodec).parse("'a'");
  }

  @Test
  public void should_parse_udt_with_extra_whitespace() {
    UdtValue udt = parse(" { field1 : 1 , field2 : NULL , field3 : 'a' } ");

    assertThat(udt.getInt(0)).isEqualTo(1);
    assertThat(udt.isNull(1)).isTrue();
    assertThat(udt.getString(2)).isEqualTo("a");

    verify(intCodec).parse("1");
    verify(doubleCodec).parse("NULL");
    verify(textCodec).parse("'a'");
  }

  @Test
  public void should_fail_to_parse_invalid_input() {
    // general UDT structure invalid
    assertThatThrownBy(() -> parse("not a udt"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"not a udt\" at character 0: expecting '{' but got 'n'");
    assertThatThrownBy(() -> parse(" { "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \" { \" at character 3: expecting CQL identifier or '}', got EOF");
    assertThatThrownBy(() -> parse("{ [ "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{ [ \", cannot parse a CQL identifier at character 2");
    assertThatThrownBy(() -> parse("{ field1 "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{ field1 \", at field field1 (character 9) expecting ':', but got EOF");
    assertThatThrownBy(() -> parse("{ field1 ,"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{ field1 ,\", at field field1 (character 9) expecting ':', but got ','");
    assertThatThrownBy(() -> parse("{nonExistentField:NULL}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{nonExistentField:NULL}\", unknown CQL identifier at character 17: \"nonExistentField\"");
    assertThatThrownBy(() -> parse("{ field1 : "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{ field1 : \", invalid CQL value at field field1 (character 11)");
    assertThatThrownBy(() -> parse("{ field1 : ["))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{ field1 : [\", invalid CQL value at field field1 (character 11)");
    assertThatThrownBy(() -> parse("{ field1 : 1 , "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{ field1 : 1 , \" at field field1 (character 15): expecting CQL identifier or '}', got EOF");
    assertThatThrownBy(() -> parse("{ field1 : 1 field2 "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{ field1 : 1 field2 \", at field field1 (character 13) expecting ',' but got 'f'");
    assertThatThrownBy(() -> parse("{field1:1,field2:12.34,field3:'a'"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{field1:1,field2:12.34,field3:'a'\", at field field3 (character 33) expecting ',' or '}', but got EOF");
    assertThatThrownBy(() -> parse("{field1:1,field2:12.34,field3:'a'}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{field1:1,field2:12.34,field3:'a'}}\", at character 34 expecting EOF or blank, but got \"}\"");
    assertThatThrownBy(() -> parse("{field1:1,field2:12.34,field3:'a'} extra"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{field1:1,field2:12.34,field3:'a'} extra\", at character 35 expecting EOF or blank, but got \"extra\"");
    // element syntax invalid
    assertThatThrownBy(() -> parse("{field1:not a valid int,field2:NULL,field3:'a'}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{field1:not a valid int,field2:NULL,field3:'a'}\", "
                + "invalid CQL value at field field1 (character 8): "
                + "Cannot parse 32-bits int value from \"not\"")
        .hasRootCauseInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parse("{field1:1,field2:not a valid double,field3:'a'}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{field1:1,field2:not a valid double,field3:'a'}\", "
                + "invalid CQL value at field field2 (character 17): "
                + "Cannot parse 64-bits double value from \"not\"")
        .hasRootCauseInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parse("{field1:1,field2:NULL,field3:not a valid text}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse UDT value from \"{field1:1,field2:NULL,field3:not a valid text}\", "
                + "invalid CQL value at field field3 (character 29): "
                + "text or varchar values must be enclosed by single quotes")
        .hasRootCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(UdtValue.class))).isTrue();
    assertThat(codec.accepts(GenericType.of(DefaultUdtValue.class)))
        .isFalse(); // covariance not allowed
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(UdtValue.class)).isTrue();
    assertThat(codec.accepts(DefaultUdtValue.class)).isFalse(); // covariance not allowed
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(userType.newValue())).isTrue();
    assertThat(codec.accepts(new DefaultUdtValue(userType))).isTrue(); // covariance allowed
    assertThat(codec.accepts("not a udt")).isFalse();
  }
}
