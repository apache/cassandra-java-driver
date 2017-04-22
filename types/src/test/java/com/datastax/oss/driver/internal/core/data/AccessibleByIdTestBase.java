/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.GettableById;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableById;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.type.DataTypes;
import com.datastax.oss.driver.api.type.reflect.GenericType;
import com.datastax.oss.driver.internal.type.codec.CqlIntToStringCodec;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public abstract class AccessibleByIdTestBase<
        T extends GettableById & SettableById<T> & GettableByName & SettableByName<T>>
    extends AccessibleByIndexTestBase<T> {

  private static final CqlIdentifier FIELD0_ID = CqlIdentifier.fromInternal("field0");
  private static final String FIELD0_NAME = "field0";

  @Test
  public void should_set_primitive_value_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.setInt(FIELD0_ID, 1);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    Mockito.verify(intCodec).encodePrimitive(1, ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_object_value_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);

    // When
    t.setString(FIELD0_ID, "a");

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    Mockito.verify(textCodec).encode("a", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x61"));
  }

  @Test
  public void should_set_bytes_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_to_null_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    t.setToNull(FIELD0_ID);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isNull();
  }

  @Test
  public void should_set_with_explicit_class_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, String.class))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.set(FIELD0_ID, "1", String.class);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    Mockito.verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_type_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.set(FIELD0_ID, "1", GenericType.STRING);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    Mockito.verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_codec_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.set(FIELD0_ID, "1", intToStringCodec);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    Mockito.verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_get_primitive_value_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    int i = t.getInt(FIELD0_ID);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    Mockito.verify(intCodec).decodePrimitive(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(i).isEqualTo(1);
  }

  @Test
  public void should_get_object_value_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x61"));

    // When
    String s = t.getString(FIELD0_ID);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    Mockito.verify(textCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("a");
  }

  @Test
  public void should_get_bytes_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    ByteBuffer bytes = t.getBytesUnsafe(FIELD0_ID);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(bytes).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_test_if_null_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_ID, null);

    // When
    boolean isNull = t.isNull(FIELD0_ID);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(isNull).isTrue();
  }

  @Test
  public void should_get_with_explicit_class_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, String.class))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_ID, String.class);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    Mockito.verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_type_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_ID, GenericType.STRING);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    Mockito.verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_codec_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_ID, intToStringCodec);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    Mockito.verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_set_primitive_value_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.setInt(FIELD0_NAME, 1);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    Mockito.verify(intCodec).encodePrimitive(1, ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_object_value_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);

    // When
    t.setString(FIELD0_NAME, "a");

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    Mockito.verify(textCodec).encode("a", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x61"));
  }

  @Test
  public void should_set_bytes_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_to_null_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    t.setToNull(FIELD0_NAME);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isNull();
  }

  @Test
  public void should_set_with_explicit_class_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, String.class))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.set(FIELD0_NAME, "1", String.class);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    Mockito.verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_type_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.set(FIELD0_NAME, "1", GenericType.STRING);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    Mockito.verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_codec_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.set(FIELD0_NAME, "1", intToStringCodec);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    Mockito.verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_get_primitive_value_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    int i = t.getInt(FIELD0_NAME);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    Mockito.verify(intCodec).decodePrimitive(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(i).isEqualTo(1);
  }

  @Test
  public void should_get_object_value_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x61"));

    // When
    String s = t.getString(FIELD0_NAME);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    Mockito.verify(textCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("a");
  }

  @Test
  public void should_get_bytes_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    ByteBuffer bytes = t.getBytesUnsafe(FIELD0_NAME);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(bytes).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_test_if_null_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_NAME, null);

    // When
    boolean isNull = t.isNull(FIELD0_NAME);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    assertThat(isNull).isTrue();
  }

  @Test
  public void should_get_with_explicit_class_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, String.class))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_NAME, String.class);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    Mockito.verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_type_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    Mockito.when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_NAME, GenericType.STRING);

    // Then
    Mockito.verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    Mockito.verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_codec_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = Mockito.spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_NAME, intToStringCodec);

    // Then
    Mockito.verifyZeroInteractions(codecRegistry);
    Mockito.verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }
}
