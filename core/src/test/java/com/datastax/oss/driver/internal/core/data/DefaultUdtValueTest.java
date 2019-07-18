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
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.SerializationHelper;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.io.UnsupportedEncodingException;
import java.util.List;
import org.junit.Test;

public class DefaultUdtValueTest extends AccessibleByIdTestBase<UdtValue> {

  @Override
  protected UdtValue newInstance(List<DataType> dataTypes, AttachmentPoint attachmentPoint) {
    UserDefinedTypeBuilder builder =
        new UserDefinedTypeBuilder(
            CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"));
    for (int i = 0; i < dataTypes.size(); i++) {
      builder.withField(CqlIdentifier.fromInternal("field" + i), dataTypes.get(i));
    }
    UserDefinedType userDefinedType = builder.build();
    userDefinedType.attach(attachmentPoint);
    return userDefinedType.newValue();
  }

  @Override
  protected UdtValue newInstance(
      List<DataType> dataTypes, List<Object> values, AttachmentPoint attachmentPoint) {
    UserDefinedTypeBuilder builder =
        new UserDefinedTypeBuilder(
            CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"));
    for (int i = 0; i < dataTypes.size(); i++) {
      builder.withField(CqlIdentifier.fromInternal("field" + i), dataTypes.get(i));
    }
    UserDefinedType userDefinedType = builder.build();
    userDefinedType.attach(attachmentPoint);
    return userDefinedType.newValue(values.toArray());
  }

  @Test
  public void should_serialize_and_deserialize() {
    UserDefinedType type =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.TEXT)
            .build();
    UdtValue in = type.newValue();
    in = in.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));
    in = in.setBytesUnsafe(1, Bytes.fromHexString("0x61"));

    UdtValue out = SerializationHelper.serializeAndDeserialize(in);

    assertThat(out.getType()).isEqualTo(in.getType());
    assertThat(out.getType().isDetached()).isTrue();
    assertThat(Bytes.toHexString(out.getBytesUnsafe(0))).isEqualTo("0x00000001");
    assertThat(Bytes.toHexString(out.getBytesUnsafe(1))).isEqualTo("0x61");
  }

  @Test
  public void should_support_null_items_when_setting_in_bulk() throws UnsupportedEncodingException {
    UserDefinedType type =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.TEXT)
            .build();
    when(codecRegistry.<Integer>codecFor(DataTypes.INT)).thenReturn(TypeCodecs.INT);
    when(codecRegistry.codecFor(DataTypes.TEXT, "foo")).thenReturn(TypeCodecs.TEXT);
    UdtValue value = type.newValue(null, "foo");

    assertThat(value.isNull(0)).isTrue();
    assertThat(value.getString(1)).isEqualTo("foo");
  }

  @Test
  public void should_equate_instances_with_same_values_but_different_binary_representations() {
    UserDefinedType type =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("f"), DataTypes.VARINT)
            .build();

    UdtValue udt1 = type.newValue().setBytesUnsafe(0, Bytes.fromHexString("0x01"));
    UdtValue udt2 = type.newValue().setBytesUnsafe(0, Bytes.fromHexString("0x0001"));

    assertThat(udt1).isEqualTo(udt2);
  }

  @Test
  public void should_format_to_string() {
    UserDefinedType type =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("t"), DataTypes.TEXT)
            .withField(CqlIdentifier.fromInternal("i"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("d"), DataTypes.DOUBLE)
            .build();

    UdtValue udt = type.newValue().setString("t", "foobar").setDouble("d", 3.14);

    assertThat(udt.getFormattedContents()).isEqualTo("{t:'foobar',i:NULL,d:3.14}");
  }

  @Test
  public void should_equate_instances_with_different_protocol_versions() {

    UserDefinedType type1 =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("t"), DataTypes.TEXT)
            .withField(CqlIdentifier.fromInternal("i"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("d"), DataTypes.DOUBLE)
            .build();
    type1.attach(attachmentPoint);

    // create an idential type, but with a different attachment point
    UserDefinedType type2 =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("t"), DataTypes.TEXT)
            .withField(CqlIdentifier.fromInternal("i"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("d"), DataTypes.DOUBLE)
            .build();
    type2.attach(v3AttachmentPoint);
    UdtValue udt1 =
        type1.newValue().setString("t", "some text string").setInt("i", 42).setDouble("d", 3.14);
    UdtValue udt2 =
        type2.newValue().setString("t", "some text string").setInt("i", 42).setDouble("d", 3.14);
    assertThat(udt1).isEqualTo(udt2);
  }
}
