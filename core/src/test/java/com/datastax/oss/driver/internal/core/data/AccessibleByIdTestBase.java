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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.GettableById;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableById;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CqlIntToStringCodec;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import org.junit.Test;

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
    t = t.setInt(FIELD0_ID, 1);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    verify(intCodec).encodePrimitive(1, ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_object_value_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);

    // When
    t = t.setString(FIELD0_ID, "a");

    // Then
    verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    verify(textCodec).encode("a", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x61"));
  }

  @Test
  public void should_set_bytes_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_to_null_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    t = t.setToNull(FIELD0_ID);

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isNull();
  }

  @Test
  public void should_set_with_explicit_class_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, String.class)).thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.set(FIELD0_ID, "1", String.class);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_type_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.set(FIELD0_ID, "1", GenericType.STRING);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_codec_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.set(FIELD0_ID, "1", intToStringCodec);

    // Then
    verifyZeroInteractions(codecRegistry);
    verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_ID)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_get_primitive_value_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    int i = t.getInt(FIELD0_ID);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    verify(intCodec).decodePrimitive(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(i).isEqualTo(1);
  }

  @Test
  public void should_get_object_value_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x61"));

    // When
    String s = t.getString(FIELD0_ID);

    // Then
    verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    verify(textCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("a");
  }

  @Test
  public void should_get_bytes_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    ByteBuffer bytes = t.getBytesUnsafe(FIELD0_ID);

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(bytes).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_test_if_null_by_id() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_ID, null);

    // When
    boolean isNull = t.isNull(FIELD0_ID);

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(isNull).isTrue();
  }

  @Test
  public void should_get_with_explicit_class_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, String.class)).thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_ID, String.class);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_type_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_ID, GenericType.STRING);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_codec_by_id() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_ID, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_ID, intToStringCodec);

    // Then
    verifyZeroInteractions(codecRegistry);
    verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_set_primitive_value_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.setInt(FIELD0_NAME, 1);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    verify(intCodec).encodePrimitive(1, ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_object_value_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);

    // When
    t = t.setString(FIELD0_NAME, "a");

    // Then
    verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    verify(textCodec).encode("a", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x61"));
  }

  @Test
  public void should_set_bytes_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_to_null_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    t = t.setToNull(FIELD0_NAME);

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isNull();
  }

  @Test
  public void should_set_with_explicit_class_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, String.class)).thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.set(FIELD0_NAME, "1", String.class);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_type_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.set(FIELD0_NAME, "1", GenericType.STRING);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_set_with_explicit_codec_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t = t.set(FIELD0_NAME, "1", intToStringCodec);

    // Then
    verifyZeroInteractions(codecRegistry);
    verify(intToStringCodec).encode("1", ProtocolVersion.DEFAULT);
    assertThat(t.getBytesUnsafe(FIELD0_NAME)).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_get_primitive_value_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    int i = t.getInt(FIELD0_NAME);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, Integer.class);
    verify(intCodec).decodePrimitive(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(i).isEqualTo(1);
  }

  @Test
  public void should_get_object_value_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.TEXT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x61"));

    // When
    String s = t.getString(FIELD0_NAME);

    // Then
    verify(codecRegistry).codecFor(DataTypes.TEXT, String.class);
    verify(textCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("a");
  }

  @Test
  public void should_get_bytes_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    ByteBuffer bytes = t.getBytesUnsafe(FIELD0_NAME);

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(bytes).isEqualTo(Bytes.fromHexString("0x00000001"));
  }

  @Test
  public void should_test_if_null_by_name() {
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_NAME, null);

    // When
    boolean isNull = t.isNull(FIELD0_NAME);

    // Then
    verifyZeroInteractions(codecRegistry);
    assertThat(isNull).isTrue();
  }

  @Test
  public void should_get_with_explicit_class_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, String.class)).thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_NAME, String.class);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, String.class);
    verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_type_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    when(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .thenAnswer(i -> intToStringCodec);
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_NAME, GenericType.STRING);

    // Then
    verify(codecRegistry).codecFor(DataTypes.INT, GenericType.STRING);
    verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test
  public void should_get_with_explicit_codec_by_name() {
    // Given
    CqlIntToStringCodec intToStringCodec = spy(new CqlIntToStringCodec());
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);
    t = t.setBytesUnsafe(FIELD0_NAME, Bytes.fromHexString("0x00000001"));

    // When
    String s = t.get(FIELD0_NAME, intToStringCodec);

    // Then
    verifyZeroInteractions(codecRegistry);
    verify(intToStringCodec).decode(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
    assertThat(s).isEqualTo("1");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("CheckReturnValue")
  public void should_fail_when_id_does_not_exists() {
    final CqlIdentifier invalidField = CqlIdentifier.fromInternal("invalidField");
    // Given
    T t = newInstance(ImmutableList.of(DataTypes.INT), attachmentPoint);

    // When
    t.setInt(invalidField, 1);

    // Then the method will throw IllegalArgumentException up to the client.
  }
}
