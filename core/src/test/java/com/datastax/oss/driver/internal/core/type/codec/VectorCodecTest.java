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
public class VectorCodecTest {

  @DataProvider
  public static Object[] dataProvider() {
    HashMap<Integer, String> map1 = new HashMap<>();
    map1.put(1, "a");
    HashMap<Integer, String> map2 = new HashMap<>();
    map2.put(2, "b");
    return new TestDataContainer[] {
      new TestDataContainer(
          DataTypes.FLOAT,
          new Float[] {1.0f, 2.5f},
          "[1.0, 2.5]",
          Bytes.fromHexString("0x3f80000040200000")),
      new TestDataContainer(
          DataTypes.ASCII,
          new String[] {"ab", "cde"},
          "['ab', 'cde']",
          Bytes.fromHexString("0x02616203636465")),
      new TestDataContainer(
          DataTypes.BIGINT,
          new Long[] {1L, 2L},
          "[1, 2]",
          Bytes.fromHexString("0x00000000000000010000000000000002")),
      new TestDataContainer(
          DataTypes.BLOB,
          new ByteBuffer[] {Bytes.fromHexString("0xCAFE"), Bytes.fromHexString("0xABCD")},
          "[0xcafe, 0xabcd]",
          Bytes.fromHexString("0x02cafe02abcd")),
      new TestDataContainer(
          DataTypes.BOOLEAN,
          new Boolean[] {true, false},
          "[true, false]",
          Bytes.fromHexString("0x0100")),
      new TestDataContainer(
          DataTypes.TIME,
          new LocalTime[] {LocalTime.ofNanoOfDay(1), LocalTime.ofNanoOfDay(2)},
          "['00:00:00.000000001', '00:00:00.000000002']",
          Bytes.fromHexString("0x080000000000000001080000000000000002")),
      new TestDataContainer(
          DataTypes.mapOf(DataTypes.INT, DataTypes.ASCII),
          new HashMap[] {map1, map2},
          "[{1:'a'}, {2:'b'}]",
          Bytes.fromHexString(
              "0x110000000100000004000000010000000161110000000100000004000000020000000162")),
      new TestDataContainer(
          DataTypes.vectorOf(DataTypes.INT, 1),
          new CqlVector[] {CqlVector.newInstance(1), CqlVector.newInstance(2)},
          "[[1], [2]]",
          Bytes.fromHexString("0x0000000100000002")),
      new TestDataContainer(
          DataTypes.vectorOf(DataTypes.TEXT, 1),
          new CqlVector[] {CqlVector.newInstance("ab"), CqlVector.newInstance("cdef")},
          "[['ab'], ['cdef']]",
          Bytes.fromHexString("0x03026162050463646566")),
      new TestDataContainer(
          DataTypes.vectorOf(DataTypes.vectorOf(DataTypes.FLOAT, 2), 1),
          new CqlVector[] {
            CqlVector.newInstance(CqlVector.newInstance(1.0f, 2.5f)),
            CqlVector.newInstance(CqlVector.newInstance(3.0f, 4.5f))
          },
          "[[[1.0, 2.5]], [[3.0, 4.5]]]",
          Bytes.fromHexString("0x3f800000402000004040000040900000"))
    };
  }

  @UseDataProvider("dataProvider")
  @Test
  public void should_encode(TestDataContainer testData) {
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    CqlVector<Object> vector = CqlVector.newInstance(testData.getValues());
    assertThat(codec.encode(vector, ProtocolVersion.DEFAULT)).isEqualTo(testData.getBytes());
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_throw_on_encode_with_too_few_elements(TestDataContainer testData) {
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    assertThatThrownBy(
            () ->
                codec.encode(
                    CqlVector.newInstance(testData.getValues()[0]), ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_throw_on_encode_with_too_many_elements(TestDataContainer testData) {
    Object[] doubled = ArrayUtils.addAll(testData.getValues(), testData.getValues());
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    assertThatThrownBy(() -> codec.encode(CqlVector.newInstance(doubled), ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_decode(TestDataContainer testData) {
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    assertThat(codec.decode(testData.getBytes(), ProtocolVersion.DEFAULT))
        .isEqualTo(CqlVector.newInstance(testData.getValues()));
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_throw_on_decode_if_too_few_bytes(TestDataContainer testData) {
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    int lastIndex = testData.getBytes().remaining() - 1;
    assertThatThrownBy(
            () ->
                codec.decode(
                    (ByteBuffer) testData.getBytes().duplicate().limit(lastIndex),
                    ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_throw_on_decode_if_too_many_bytes(TestDataContainer testData) {
    ByteBuffer doubled = ByteBuffer.allocate(testData.getBytes().remaining() * 2);
    doubled.put(testData.getBytes().duplicate()).put(testData.getBytes().duplicate()).flip();
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    assertThatThrownBy(() -> codec.decode(doubled, ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_format(TestDataContainer testData) {
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    CqlVector<Object> vector = CqlVector.newInstance(testData.getValues());
    assertThat(codec.format(vector)).isEqualTo(testData.getFormatted());
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_parse(TestDataContainer testData) {
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    assertThat(codec.parse(testData.getFormatted()))
        .isEqualTo(CqlVector.newInstance(testData.getValues()));
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_accept_data_type(TestDataContainer testData) {
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    assertThat(codec.accepts(new DefaultVectorType(testData.getDataType(), 2))).isTrue();
    assertThat(codec.accepts(new DefaultVectorType(DataTypes.custom("non-existent"), 2))).isFalse();
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_accept_vector_type_correct_dimension_only(TestDataContainer testData) {
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    assertThat(codec.accepts(new DefaultVectorType(testData.getDataType(), 0))).isFalse();
    assertThat(codec.accepts(new DefaultVectorType(testData.getDataType(), 1))).isFalse();
    assertThat(codec.accepts(new DefaultVectorType(testData.getDataType(), 3))).isFalse();
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_accept_generic_type(TestDataContainer testData) {
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    assertThat(codec.accepts(codec.getJavaType())).isTrue();
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_accept_raw_type(TestDataContainer testData) {
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    assertThat(codec.accepts(CqlVector.class)).isTrue();
    assertThat(codec.accepts(Integer.class)).isFalse();
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_accept_object(TestDataContainer testData) {
    TypeCodec<CqlVector<Object>> codec = getCodec(testData.getDataType());
    CqlVector<?> vector = CqlVector.newInstance(testData.getValues());
    assertThat(codec.accepts(vector)).isTrue();
    assertThat(codec.accepts(Integer.MIN_VALUE)).isFalse();
  }

  @Test
  public void should_handle_null_and_empty() {
    TypeCodec<CqlVector<Object>> codec = getCodec(DataTypes.FLOAT);
    assertThat(codec.encode(null, ProtocolVersion.DEFAULT)).isNull();
    assertThat(codec.decode(Bytes.fromHexString("0x"), ProtocolVersion.DEFAULT)).isNull();
    assertThat(codec.format(null)).isEqualTo("NULL");
    assertThat(codec.parse("NULL")).isNull();
    assertThat(codec.parse("null")).isNull();
    assertThat(codec.parse("")).isNull();
    assertThat(codec.parse(null)).isNull();
    assertThatThrownBy(() -> codec.encode(CqlVector.newInstance(), ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static TypeCodec<CqlVector<Object>> getCodec(DataType dataType) {
    return TypeCodecs.vectorOf(
        DataTypes.vectorOf(dataType, 2), CodecRegistry.DEFAULT.codecFor(dataType));
  }

  private static class TestDataContainer {
    private final DataType dataType;
    private final Object[] values;
    private final String formatted;
    private final ByteBuffer bytes;

    public TestDataContainer(
        DataType dataType, Object[] values, String formatted, ByteBuffer bytes) {
      this.dataType = dataType;
      this.values = values;
      this.formatted = formatted;
      this.bytes = bytes;
    }

    public DataType getDataType() {
      return dataType;
    }

    public Object[] getValues() {
      return values;
    }

    public String getFormatted() {
      return formatted;
    }

    public ByteBuffer getBytes() {
      return bytes;
    }
  }
}
