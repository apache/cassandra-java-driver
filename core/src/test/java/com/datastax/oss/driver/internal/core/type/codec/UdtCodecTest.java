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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
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
  public void should_parse_udt() {
    UdtValue udt = parse("{field1:1,field2:NULL,field3:'a'}");

    assertThat(udt.getInt(0)).isEqualTo(1);
    assertThat(udt.isNull(1)).isTrue();
    assertThat(udt.getString(2)).isEqualTo("a");

    verify(intCodec).parse("1");
    verify(doubleCodec).parse("NULL");
    verify(textCodec).parse("'a'");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a udt");
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
