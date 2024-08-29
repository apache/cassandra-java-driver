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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.util.HashMap;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class VectorCodecTest extends CodecTestBase<CqlVector<Float>> {

  private static final Float[] VECTOR_ARGS = {1.0f, 2.5f};

  private static final CqlVector<Float> VECTOR = CqlVector.newInstance(VECTOR_ARGS);

  private static final String VECTOR_HEX_STRING = "0x" + "3f800000" + "40200000";

  private static final String FORMATTED_VECTOR = "[1.0, 2.5]";

  public VectorCodecTest() {
    VectorType vectorType = DataTypes.vectorOf(DataTypes.FLOAT, 2);
    this.codec = TypeCodecs.vectorOf(vectorType, TypeCodecs.FLOAT);
  }

  @DataProvider
  public static Object[][] dataProvider() {
    HashMap<Integer, String> map1 = new HashMap<>();
    map1.put(1, "a");
    HashMap<Integer, String> map2 = new HashMap<>();
    map2.put(2, "b");
    // For every row, data type, array of 2 values, formatted string, encoded bytes
    return new Object[][] {
      {
        DataTypes.FLOAT,
        new Float[] {1.0f, 2.5f},
        "[1.0, 2.5]",
        Bytes.fromHexString("0x3f80000040200000")
      },
      {
        DataTypes.ASCII,
        new String[] {"ab", "cde"},
        "['ab', 'cde']",
        Bytes.fromHexString("0x02616203636465")
      },
      {
        DataTypes.BIGINT,
        new Long[] {1L, 2L},
        "[1, 2]",
        Bytes.fromHexString("0x00000000000000010000000000000002")
      },
      {
        DataTypes.BLOB,
        new ByteBuffer[] {Bytes.fromHexString("0xCAFE"), Bytes.fromHexString("0xABCD")},
        "[0xcafe, 0xabcd]",
        Bytes.fromHexString("0x02cafe02abcd")
      },
      {
        DataTypes.BOOLEAN,
        new Boolean[] {true, false},
        "[true, false]",
        Bytes.fromHexString("0x0100")
      },
      {
        DataTypes.TIME,
        new LocalTime[] {LocalTime.ofNanoOfDay(1), LocalTime.ofNanoOfDay(2)},
        "['00:00:00.000000001', '00:00:00.000000002']",
        Bytes.fromHexString("0x080000000000000001080000000000000002")
      },
      {
        DataTypes.mapOf(DataTypes.INT, DataTypes.ASCII),
        new HashMap[] {map1, map2},
        "[{1:'a'}, {2:'b'}]",
        Bytes.fromHexString(
            "0x110000000100000004000000010000000161110000000100000004000000020000000162")
      },
      {
        DataTypes.vectorOf(DataTypes.INT, 1),
        new CqlVector[] {CqlVector.newInstance(1), CqlVector.newInstance(2)},
        "[[1], [2]]",
        Bytes.fromHexString("0x0000000100000002")
      },
      {
        DataTypes.vectorOf(DataTypes.TEXT, 1),
        new CqlVector[] {CqlVector.newInstance("ab"), CqlVector.newInstance("cdef")},
        "[['ab'], ['cdef']]",
        Bytes.fromHexString("0x03026162050463646566")
      },
      {
        DataTypes.vectorOf(DataTypes.vectorOf(DataTypes.FLOAT, 2), 1),
        new CqlVector[] {
          CqlVector.newInstance(CqlVector.newInstance(1.0f, 2.5f)),
          CqlVector.newInstance(CqlVector.newInstance(3.0f, 4.5f))
        },
        "[[[1.0, 2.5]], [[3.0, 4.5]]]",
        Bytes.fromHexString("0x3f800000402000004040000040900000")
      },
    };
  }

  @UseDataProvider("dataProvider")
  @Test
  public void should_encode(
      DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
    TypeCodec<CqlVector<Object>> codec = getCodec(dataType);
    CqlVector<Object> vector = CqlVector.newInstance(values);
    assertThat(codec.encode(vector, ProtocolVersion.DEFAULT)).isEqualTo(bytes);

    //    assertThat(encode(null)).isNull();
  }

  /** Too few elements will cause an exception, extra elements will be silently ignored */
  @Test
  @UseDataProvider("dataProvider")
  public void should_throw_on_encode_with_too_few_elements(
      DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
    TypeCodec<CqlVector<Object>> codec = getCodec(dataType);
    assertThatThrownBy(
            () -> codec.encode(CqlVector.newInstance(values[0]), ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void should_throw_on_encode_with_empty_list() {
    assertThatThrownBy(() -> encode(CqlVector.newInstance()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_throw_on_encode_with_too_many_elements(
      DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
    Object[] doubled = ArrayUtils.addAll(values, values);
    TypeCodec<CqlVector<Object>> codec = getCodec(dataType);
    assertThatThrownBy(() -> codec.encode(CqlVector.newInstance(doubled), ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_decode(
      DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
    TypeCodec<CqlVector<Object>> codec = getCodec(dataType);
    assertThat(codec.decode(bytes, ProtocolVersion.DEFAULT))
        .isEqualTo(CqlVector.newInstance(values));
    //    assertThat(decode("0x")).isNull();
    //    assertThat(decode(null)).isNull();
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_throw_on_decode_if_too_few_bytes(
      DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
    TypeCodec<CqlVector<Object>> codec = getCodec(dataType);
    int lastIndex = bytes.remaining() - 1;
    assertThatThrownBy(
            () ->
                codec.decode(
                    (ByteBuffer) bytes.duplicate().limit(lastIndex), ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_throw_on_decode_if_too_many_bytes(
      DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
    ByteBuffer doubled = ByteBuffer.allocate(bytes.remaining() * 2);
    doubled.put(bytes.duplicate()).put(bytes.duplicate()).flip();
    TypeCodec<CqlVector<Object>> codec = getCodec(dataType);
    assertThatThrownBy(() -> codec.decode(doubled, ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_format(
      DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
    TypeCodec<CqlVector<Object>> codec = getCodec(dataType);
    CqlVector<Object> vector = CqlVector.newInstance(values);
    assertThat(codec.format(vector)).isEqualTo(formatted);
    //    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_parse(DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
    TypeCodec<CqlVector<Object>> codec = getCodec(dataType);
    assertThat(codec.parse(formatted)).isEqualTo(CqlVector.newInstance(values));
    //    assertThat(parse("NULL")).isNull();
    //    assertThat(parse("null")).isNull();
    //    assertThat(parse("")).isNull();
    //    assertThat(parse(null)).isNull();
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_accept_data_type(
      DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
    TypeCodec<CqlVector<Object>> codec = getCodec(dataType);
    assertThat(codec.accepts(new DefaultVectorType(dataType, 2))).isTrue();
    assertThat(codec.accepts(new DefaultVectorType(DataTypes.custom("non-existent"), 2))).isFalse();
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_accept_vector_type_correct_dimension_only(
      DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
    TypeCodec<CqlVector<Object>> codec = getCodec(dataType);
    assertThat(codec.accepts(new DefaultVectorType(dataType, 0))).isFalse();
    assertThat(codec.accepts(new DefaultVectorType(dataType, 1))).isFalse();
    assertThat(codec.accepts(new DefaultVectorType(dataType, 3))).isFalse();
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_accept_generic_type(
      DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
    TypeCodec<CqlVector<Object>> codec = getCodec(dataType);
    assertThat(codec.accepts(codec.getJavaType())).isTrue();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(CqlVector.class)).isTrue();
    assertThat(codec.accepts(Integer.class)).isFalse();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(VECTOR)).isTrue();
    assertThat(codec.accepts(Integer.MIN_VALUE)).isFalse();
  }

  private static TypeCodec<CqlVector<Object>> getCodec(DataType dataType) {
    return TypeCodecs.vectorOf(
        DataTypes.vectorOf(dataType, 2), CodecRegistry.DEFAULT.codecFor(dataType));
  }
}
