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
package com.datastax.oss.driver.internal.core.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.data.SettableByIndex;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveIntCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CqlIntToStringCodec;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public abstract class AccessibleByIndexTestBase<T extends GettableByIndex & SettableByIndex<T>> {

  protected abstract T newInstance(List<DataType> dataTypes, AttachmentPoint attachmentPoint);

  protected abstract T newInstance(
      List<DataType> dataTypes, List<Object> values, AttachmentPoint attachmentPoint);

  @Mock protected AttachmentPoint attachmentPoint;
  @Mock protected AttachmentPoint v3AttachmentPoint;
  @Mock protected CodecRegistry codecRegistry;
  protected PrimitiveIntCodec intCodec;
  protected TypeCodec<Double> doubleCodec;
  protected TypeCodec<String> textCodec;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(attachmentPoint.getCodecRegistry()).thenReturn(codecRegistry);
    when(attachmentPoint.getProtocolVersion()).thenReturn(ProtocolVersion.DEFAULT);

    when(v3AttachmentPoint.getCodecRegistry()).thenReturn(codecRegistry);
    when(v3AttachmentPoint.getProtocolVersion()).thenReturn(DefaultProtocolVersion.V3);

    intCodec = spy(TypeCodecs.INT);
    doubleCodec = spy(TypeCodecs.DOUBLE);
    textCodec = spy(TypeCodecs.TEXT);

    when(codecRegistry.codecFor(DataTypes.INT, Integer.class)).thenAnswer(i -> intCodec);
    when(codecRegistry.codecFor(DataTypes.DOUBLE, Double.class)).thenAnswer(i -> doubleCodec);
    when(codecRegistry.codecFor(DataTypes.TEXT, String.class)).thenAnswer(i -> textCodec);

    when(codecRegistry.codecFor(DataTypes.INT)).thenAnswer(i -> intCodec);
    when(codecRegistry.codecFor(DataTypes.TEXT)).thenAnswer(t -> textCodec);
    when(codecRegistry.codecFor(DataTypes.DOUBLE)).thenAnswer(d -> doubleCodec);
  }

  @Test
  public void should_set_primitive_value_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.setInt(0, 1);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    verify(intCodec).encodePrimitive(1, ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_object_value_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);

    // When
    t = t.setString(0, "a");

    // Then
    verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    verify(textCodec).encode("a", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x61"));
  }

  @Test
  public void should_set_bytes_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_to_null_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    t = t.setToNull(0);

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(0)).isNull();
  }

  @Test
  public void should_set_with_explicit_class_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, String.class)).thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.set(0, "1", String.class);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_type_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.set(0, "1", GenericType.STRING);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_codec_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.set(0, "1", intToStringCodec);

    // Then
    verifyZeroInteractions(codecRegistry);
    verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_values_in_bulk() {
    // Given
    when(codecRegistry.codecFor(DataTypes.TEXT, "foo")).thenReturn(TypeCodecs.TEXT);
    when(codecRegistry.codecFor(DataTypes.INT, 1)).thenReturn(TypeCodecs.INT);

    // When
    T t =
        newInstance(
            ImmutableList.of(DataTypes.TEXT, DataTypes.INT),
            ImmutableList.of("foo", 1),
            attachmentPoint);

    // Then
    assertThat(t.getString(0)).isEqualTo("foo");
    assertThat(t.getInt(1)).isEqualTo(1);
    verify(codecRegistry).codecFor(DataTypes.TEXT, "foo");
    verify(codecRegistry).codecFor(DataTypes.INT, 1);
  }

  @Test
  public void should_set_values_in_bulk_when_not_enough_values() {
    // Given
    when(codecRegistry.codecFor(DataTypes.TEXT, "foo")).thenReturn(TypeCodecs.TEXT);

    // When
    T t =
        newInstance(
            ImmutableList.of(DataTypes.TEXT, DataTypes.INT),
            ImmutableList.of("foo"),
            attachmentPoint);

    // Then
    assertThat(t.getString(0)).isEqualTo("foo");
    assertThat(t.isNull(1)).isTrue();
    verify(codecRegistry).codecFor(DataTypes.TEXT, "foo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_set_values_in_bulk_when_too_many_values() {
    newInstance(
        ImmutableList.of(DataTypes.TEXT, DataTypes.INT),
        ImmutableList.of("foo", 1, "bar"),
        attachmentPoint);
  }

  @Test
  public void should_get_primitive_value_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    int i = t.getInt(0);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    verify(intCodec).decodePrimitive(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(i).isEqualTo(1);
  }

  @Test
  public void should_get_object_value_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);
    t = t.setBytesUnsafe(0, Bytes.fromHexString("0x61"));

    // When
    String s = t.getString(0);

    // Then
    verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    verify(textCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("a");
  }

  @Test
  public void should_get_bytes_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    ByteBuffer bytes = t.getBytesUnsafe(0);

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(bytes).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_test_if_null_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(0, null);

    // When
    boolean isNull = t.isNull(0);

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(isNull).isTrue();
  }

  @Test
  public void should_get_with_explicit_class_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, String.class)).thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(0, String.class);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_type_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(0, GenericType.STRING);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_codec_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(0, intToStringCodec);

    // Then
    verifyZeroInteractions(codecRegistry);
    verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }
}
