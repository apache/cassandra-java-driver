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
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveIntCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.data.DefaultTupleValue;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class TupleCodecTest extends CodecTestBase<TupleValue> {

  @Mock private AttachmentPoint attachmentPoint;
  @Mock private CodecRegistry codecRegistry;
  private PrimitiveIntCodec intCodec;
  private TypeCodec<Double> doubleCodec;
  private TypeCodec<String> textCodec;

  private TupleType tupleType;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    Mockito.when(attachmentPoint.getCodecRegistry()).thenReturn(codecRegistry);
    Mockito.when(attachmentPoint.getProtocolVersion()).thenReturn(ProtocolVersion.DEFAULT);

    intCodec = Mockito.spy(TypeCodecs.INT);
    doubleCodec = Mockito.spy(TypeCodecs.DOUBLE);
    textCodec = Mockito.spy(TypeCodecs.TEXT);

    // Called by the getters/setters
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, Integer.class)).thenAnswer(i -> intCodec);
    Mockito.when(codecRegistry.codecFor(DataTypes.DOUBLE, Double.class))
        .thenAnswer(i -> doubleCodec);
    Mockito.when(codecRegistry.codecFor(DataTypes.TEXT, String.class)).thenAnswer(i -> textCodec);

    // Called by format/parse
    Mockito.when(codecRegistry.codecFor(DataTypes.INT)).thenAnswer(i -> intCodec);
    Mockito.when(codecRegistry.codecFor(DataTypes.DOUBLE)).thenAnswer(i -> doubleCodec);
    Mockito.when(codecRegistry.codecFor(DataTypes.TEXT)).thenAnswer(i -> textCodec);

    tupleType =
        new DefaultTupleType(
            ImmutableList.of(DataTypes.INT, DataTypes.DOUBLE, DataTypes.TEXT), attachmentPoint);

    codec = TypeCodecs.tupleOf(tupleType);
  }

  @Test
  public void should_encode_null_tuple() {
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_encode_tuple() {
    TupleValue tuple = tupleType.newValue();
    tuple.setInt(0, 1);
    tuple.setToNull(1);
    tuple.setString(2, "a");

    assertThat(encode(tuple))
        .isEqualTo(
            "0x"
                + ("00000004" + "00000001") // size and contents of field 0
                + "ffffffff" // null field 1
                + ("00000001" + "61") // size and contents of field 2
            );

    Mockito.verify(intCodec).encodePrimitive(1, ProtocolVersion.DEFAULT);
    // null values are handled directly in the tuple codec, without calling the child codec:
    Mockito.verifyZeroInteractions(doubleCodec);
    Mockito.verify(textCodec).encode("a", ProtocolVersion.DEFAULT);
  }

  @Test
  public void should_decode_null_tuple() {
    assertThat(decode(null)).isNull();
  }

  @Test
  public void should_decode_tuple() {
    TupleValue tuple = decode("0x" + ("00000004" + "00000001") + "ffffffff" + ("00000001" + "61"));

    assertThat(tuple.getInt(0)).isEqualTo(1);
    assertThat(tuple.isNull(1)).isTrue();
    assertThat(tuple.getString(2)).isEqualTo("a");

    Mockito.verify(intCodec)
        .decodePrimitive(Bytes.fromHexString("0x00000001"), ProtocolVersion.DEFAULT);
    Mockito.verifyZeroInteractions(doubleCodec);
    Mockito.verify(textCodec).decode(Bytes.fromHexString("0x61"), ProtocolVersion.DEFAULT);
  }

  @Test
  public void should_format_null_tuple() {
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_format_tuple() {
    TupleValue tuple = tupleType.newValue();
    tuple.setInt(0, 1);
    tuple.setToNull(1);
    tuple.setString(2, "a");

    assertThat(format(tuple)).isEqualTo("(1,NULL,'a')");

    Mockito.verify(intCodec).format(1);
    Mockito.verify(doubleCodec).format(null);
    Mockito.verify(textCodec).format("a");
  }

  @Test
  public void should_parse_null_tuple() {
    assertThat(parse(null)).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("NULL")).isNull();
  }

  @Test
  public void should_parse_tuple() {
    TupleValue tuple = parse("(1,NULL,'a')");

    assertThat(tuple.getInt(0)).isEqualTo(1);
    assertThat(tuple.isNull(1)).isTrue();
    assertThat(tuple.getString(2)).isEqualTo("a");

    Mockito.verify(intCodec).parse("1");
    Mockito.verify(doubleCodec).parse("NULL");
    Mockito.verify(textCodec).parse("'a'");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a tuple");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(TupleValue.class))).isTrue();
    assertThat(codec.accepts(GenericType.of(DefaultTupleValue.class)))
        .isFalse(); // covariance not allowed
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(TupleValue.class)).isTrue();
    assertThat(codec.accepts(DefaultTupleValue.class)).isFalse(); // covariance not allowed
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(tupleType.newValue())).isTrue();
    assertThat(codec.accepts(new DefaultTupleValue(tupleType))).isTrue(); // covariance allowed
    assertThat(codec.accepts("not a tuple")).isFalse();
  }
}
