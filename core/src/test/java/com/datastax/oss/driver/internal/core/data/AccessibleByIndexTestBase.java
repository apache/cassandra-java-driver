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
package com.datastax.oss.driver.internal.core.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

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
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public abstract class AccessibleByIndexTestBase<T extends GettableByIndex & SettableByIndex<T>> {

  protected abstract T newInstance(List<DataType> dataTypes, AttachmentPoint attachmentPoint);

  protected abstract T newInstance(
      List<DataType> dataTypes, List<Object> values, AttachmentPoint attachmentPoint);

  @Mock protected AttachmentPoint attachmentPoint;
  @Mock protected CodecRegistry codecRegistry;
  protected PrimitiveIntCodec intCodec;
  protected TypeCodec<Double> doubleCodec;
  protected TypeCodec<String> textCodec;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    Mockito.when(attachmentPoint.codecRegistry()).thenReturn(codecRegistry);
    Mockito.when(attachmentPoint.protocolVersion()).thenReturn(ProtocolVersion.DEFAULT);

    intCodec = Mockito.spy(TypeCodecs.INT);
    doubleCodec = Mockito.spy(TypeCodecs.DOUBLE);
    textCodec = Mockito.spy(TypeCodecs.TEXT);

    Mockito.when(codecRegistry.codecFor(DataTypes.INT, Integer.class)).thenAnswer(i -> intCodec);
    Mockito.when(codecRegistry.codecFor(DataTypes.DOUBLE, Double.class))
        .thenAnswer(i -> doubleCodec);
    Mockito.when(codecRegistry.codecFor(DataTypes.TEXT, String.class)).thenAnswer(i -> textCodec);
  }

  @Test
  public void should_set_primitive_value_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.setInt(0, 1);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    Mockito.verify(intCodec).encodePrimitive(1, ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_object_value_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);

    // When
    t.setString(0, "a");

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    Mockito.verify(textCodec).encode("a", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x61"));
  }

  @Test
  public void should_set_bytes_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_to_null_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    t.setToNull(0);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(0)).isNull();
  }

  @Test
  public void should_set_with_explicit_class_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, String.class))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.set(0, "1", String.class);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    Mockito.verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_type_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.set(0, "1", GenericType.STRING);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    Mockito.verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_codec_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.set(0, "1", intToStringCodec);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    Mockito.verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(0)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_values_in_bulk() {
    // Given
    Mockito.when(codecRegistry.codecFor(DataTypes.TEXT, "foo")).thenReturn(TypeCodecs.TEXT);
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, 1)).thenReturn(TypeCodecs.INT);

    // When
    T t =
        newInstance(
            ImmutableList.of(DataTypes.TEXT, DataTypes.INT),
            ImmutableList.of("foo", 1),
            attachmentPoint);

    // Then
    assertThat(t.getString(0)).isEqualTo("foo");
    assertThat(t.getInt(1)).isEqualTo(1);
    Mockito.verify(codecRegistry).codecFor(DataTypes.TEXT, "foo");
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, 1);
  }

  @Test
  public void should_set_values_in_bulk_when_not_enough_values() {
    // Given
    Mockito.when(codecRegistry.codecFor(DataTypes.TEXT, "foo")).thenReturn(TypeCodecs.TEXT);

    // When
    T t =
        newInstance(
            ImmutableList.of(DataTypes.TEXT, DataTypes.INT),
            ImmutableList.of("foo"),
            attachmentPoint);

    // Then
    assertThat(t.getString(0)).isEqualTo("foo");
    assertThat(t.isNull(1)).isTrue();
    Mockito.verify(codecRegistry).codecFor(DataTypes.TEXT, "foo");
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
    t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    int i = t.getInt(0);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    Mockito.verify(intCodec).decodePrimitive(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(i).isEqualTo(1);
  }

  @Test
  public void should_get_object_value_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);
    t.setBytesUnsafe(0, Bytes.fromHexString("0x61"));

    // When
    String s = t.getString(0);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    Mockito.verify(textCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("a");
  }

  @Test
  public void should_get_bytes_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    ByteBuffer bytes = t.getBytesUnsafe(0);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(bytes).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_test_if_null_by_index() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(0, null);

    // When
    boolean isNull = t.isNull(0);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(isNull).isTrue();
  }

  @Test
  public void should_get_with_explicit_class_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, String.class))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(0, String.class);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    Mockito.verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_type_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(0, GenericType.STRING);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    Mockito.verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_codec_by_index() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(0, intToStringCodec);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    Mockito.verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }
}
