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
package com.datastax.oss.driver.internal.core.type;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.SerializationHelper;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DataTypeDetachableTest {

  @Mock private AttachmentPoint attachmentPoint;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void simple_types_should_never_be_detached() {
    // Because simple types don't need the codec registry, we consider them as always attached by
    // default
    for (DataType simpleType : ImmutableList.of(DataTypes.INT, DataTypes.custom("some.class"))) {
      assertThat(simpleType.isDetached()).isFalse();
      assertThat(SerializationHelper.serializeAndDeserialize(simpleType).isDetached()).isFalse();
    }
  }

  @Test
  public void manually_created_tuple_should_be_detached() {
    TupleType tuple = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT);
    assertThat(tuple.isDetached()).isTrue();
  }

  @Test
  public void attaching_tuple_should_attach_all_of_its_subtypes() {
    TupleType tuple1 = DataTypes.tupleOf(DataTypes.INT);
    TupleType tuple2 = DataTypes.tupleOf(DataTypes.TEXT, tuple1);

    assertThat(tuple1.isDetached()).isTrue();
    assertThat(tuple2.isDetached()).isTrue();

    tuple2.attach(attachmentPoint);

    assertThat(tuple1.isDetached()).isFalse();
  }

  @Test
  public void manually_created_udt_should_be_detached() {
    UserDefinedType udt =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.TEXT)
            .build();
    assertThat(udt.isDetached()).isTrue();
  }

  @Test
  public void attaching_udt_should_attach_all_of_its_subtypes() {
    TupleType tuple = DataTypes.tupleOf(DataTypes.INT);
    UserDefinedType udt =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), tuple)
            .build();

    assertThat(tuple.isDetached()).isTrue();
    assertThat(udt.isDetached()).isTrue();

    udt.attach(attachmentPoint);

    assertThat(tuple.isDetached()).isFalse();
  }

  @Test
  public void list_should_be_attached_if_its_element_is() {
    TupleType tuple = DataTypes.tupleOf(DataTypes.INT);
    ListType list = DataTypes.listOf(tuple);

    assertThat(tuple.isDetached()).isTrue();
    assertThat(list.isDetached()).isTrue();

    tuple.attach(attachmentPoint);

    assertThat(list.isDetached()).isFalse();
  }

  @Test
  public void attaching_list_should_attach_its_element() {
    TupleType tuple = DataTypes.tupleOf(DataTypes.INT);
    ListType list = DataTypes.listOf(tuple);

    assertThat(tuple.isDetached()).isTrue();
    assertThat(list.isDetached()).isTrue();

    list.attach(attachmentPoint);

    assertThat(tuple.isDetached()).isFalse();
  }

  @Test
  public void set_should_be_attached_if_its_element_is() {
    TupleType tuple = DataTypes.tupleOf(DataTypes.INT);
    SetType set = DataTypes.setOf(tuple);

    assertThat(tuple.isDetached()).isTrue();
    assertThat(set.isDetached()).isTrue();

    tuple.attach(attachmentPoint);

    assertThat(set.isDetached()).isFalse();
  }

  @Test
  public void attaching_set_should_attach_its_element() {
    TupleType tuple = DataTypes.tupleOf(DataTypes.INT);
    SetType set = DataTypes.setOf(tuple);

    assertThat(tuple.isDetached()).isTrue();
    assertThat(set.isDetached()).isTrue();

    set.attach(attachmentPoint);

    assertThat(tuple.isDetached()).isFalse();
  }

  @Test
  public void map_should_be_attached_if_its_elements_are() {
    TupleType tuple1 = DataTypes.tupleOf(DataTypes.INT);
    TupleType tuple2 = DataTypes.tupleOf(DataTypes.TEXT);
    MapType map = DataTypes.mapOf(tuple1, tuple2);

    assertThat(tuple1.isDetached()).isTrue();
    assertThat(tuple2.isDetached()).isTrue();
    assertThat(map.isDetached()).isTrue();

    tuple1.attach(attachmentPoint);
    assertThat(map.isDetached()).isTrue();

    tuple2.attach(attachmentPoint);
    assertThat(map.isDetached()).isFalse();
  }

  @Test
  public void attaching_map_should_attach_all_of_its_subtypes() {
    TupleType tuple1 = DataTypes.tupleOf(DataTypes.INT);
    TupleType tuple2 = DataTypes.tupleOf(DataTypes.TEXT);
    MapType map = DataTypes.mapOf(tuple1, tuple2);

    assertThat(tuple1.isDetached()).isTrue();
    assertThat(tuple2.isDetached()).isTrue();

    map.attach(attachmentPoint);

    assertThat(tuple1.isDetached()).isFalse();
    assertThat(tuple2.isDetached()).isFalse();
  }
}
