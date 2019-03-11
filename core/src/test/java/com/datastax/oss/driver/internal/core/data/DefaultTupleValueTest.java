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

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.SerializationHelper;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.util.List;
import org.junit.Test;

public class DefaultTupleValueTest extends AccessibleByIndexTestBase<TupleValue> {

  @Override
  protected TupleValue newInstance(List<DataType> dataTypes, AttachmentPoint attachmentPoint) {
    DefaultTupleType type = new DefaultTupleType(dataTypes, attachmentPoint);
    return type.newValue();
  }

  @Override
  protected TupleValue newInstance(
      List<DataType> dataTypes, List<Object> values, AttachmentPoint attachmentPoint) {
    DefaultTupleType type = new DefaultTupleType(dataTypes, attachmentPoint);
    return type.newValue(values.toArray());
  }

  @Test
  public void should_serialize_and_deserialize() {
    DefaultTupleType type =
        new DefaultTupleType(ImmutableList.of(DataTypes.INT, DataTypes.TEXT), attachmentPoint);
    TupleValue in = type.newValue();
    in = in.setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));
    in = in.setBytesUnsafe(1, Bytes.fromHexString("0x61"));

    TupleValue out = SerializationHelper.serializeAndDeserialize(in);

    assertThat(out.getType()).isEqualTo(in.getType());
    assertThat(out.getType().isDetached()).isTrue();
    assertThat(Bytes.toHexString(out.getBytesUnsafe(0))).isEqualTo("0x00000001");
    assertThat(Bytes.toHexString(out.getBytesUnsafe(1))).isEqualTo("0x61");
  }

  @Test
  public void should_support_null_items_when_setting_in_bulk() {
    DefaultTupleType type =
        new DefaultTupleType(ImmutableList.of(DataTypes.INT, DataTypes.TEXT), attachmentPoint);
    when(codecRegistry.<Integer>codecFor(DataTypes.INT)).thenReturn(TypeCodecs.INT);
    when(codecRegistry.codecFor(DataTypes.TEXT, "foo")).thenReturn(TypeCodecs.TEXT);
    TupleValue value = type.newValue(null, "foo");

    assertThat(value.isNull(0)).isTrue();
    assertThat(value.getString(1)).isEqualTo("foo");
  }

  @Test
  public void should_equate_instances_with_same_values_but_different_binary_representations() {
    TupleType tupleType = DataTypes.tupleOf(DataTypes.VARINT);

    TupleValue tuple1 = tupleType.newValue().setBytesUnsafe(0, Bytes.fromHexString("0x01"));
    TupleValue tuple2 = tupleType.newValue().setBytesUnsafe(0, Bytes.fromHexString("0x0001"));

    assertThat(tuple1).isEqualTo(tuple2);
    assertThat(tuple1.hashCode()).isEqualTo(tuple2.hashCode());
  }

  @Test
  public void should_not_equate_instances_with_same_binary_representation_but_different_types() {
    TupleType tupleType1 = DataTypes.tupleOf(DataTypes.INT);
    TupleType tupleType2 = DataTypes.tupleOf(DataTypes.VARINT);

    TupleValue tuple1 = tupleType1.newValue().setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));
    TupleValue tuple2 = tupleType2.newValue().setBytesUnsafe(0, Bytes.fromHexString("0x00000001"));

    assertThat(tuple1).isNotEqualTo(tuple2);
  }

  @Test
  public void should_equate_instances_with_different_protocol_versions() {
    TupleType tupleType1 = DataTypes.tupleOf(DataTypes.TEXT);
    tupleType1.attach(attachmentPoint);

    // use the V3 attachmentPoint for type2
    TupleType tupleType2 = DataTypes.tupleOf(DataTypes.TEXT);
    tupleType2.attach(v3AttachmentPoint);

    TupleValue tuple1 = tupleType1.newValue().setBytesUnsafe(0, Bytes.fromHexString("0x01"));
    TupleValue tuple2 = tupleType2.newValue().setBytesUnsafe(0, Bytes.fromHexString("0x01"));

    assertThat(tuple1).isEqualTo(tuple2);
  }
}
