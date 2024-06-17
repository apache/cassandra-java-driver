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
package com.datastax.oss.driver.api.core.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.SerializationHelper;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.assertj.core.util.Lists;
import org.junit.Test;

public class CqlVectorTest {

  private static final Float[] VECTOR_ARGS = {1.0f, 2.5f};

  private void validate_built_vector(CqlVector<Float> vec) {

    assertThat(vec.size()).isEqualTo(2);
    assertThat(vec.isEmpty()).isFalse();
    assertThat(vec.get(0)).isEqualTo(VECTOR_ARGS[0]);
    assertThat(vec.get(1)).isEqualTo(VECTOR_ARGS[1]);
  }

  @Test
  public void should_build_vector_from_elements() {

    validate_built_vector(CqlVector.newInstance(VECTOR_ARGS));
  }

  @Test
  public void should_build_vector_from_list() {

    validate_built_vector(CqlVector.newInstance(Lists.newArrayList(VECTOR_ARGS)));
  }

  @Test
  public void should_build_vector_from_tostring_output() {

    CqlVector<Float> vector1 = CqlVector.newInstance(VECTOR_ARGS);
    CqlVector<Float> vector2 = CqlVector.from(vector1.toString(), TypeCodecs.FLOAT);
    assertThat(vector2).isEqualTo(vector1);
  }

  @Test
  public void should_throw_from_null_string() {

    assertThatThrownBy(
            () -> {
              CqlVector.from(null, TypeCodecs.FLOAT);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void should_throw_from_empty_string() {

    assertThatThrownBy(
            () -> {
              CqlVector.from("", TypeCodecs.FLOAT);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void should_throw_when_building_with_nulls() {

    assertThatThrownBy(
            () -> {
              CqlVector.newInstance(1.1f, null, 2.2f);
            })
        .isInstanceOf(IllegalArgumentException.class);

    Float[] theArray = new Float[] {1.1f, null, 2.2f};
    assertThatThrownBy(
            () -> {
              CqlVector.newInstance(theArray);
            })
        .isInstanceOf(IllegalArgumentException.class);

    List<Float> theList = Lists.newArrayList(1.1f, null, 2.2f);
    assertThatThrownBy(
            () -> {
              CqlVector.newInstance(theList);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void should_build_empty_vector() {

    CqlVector<Float> vector = CqlVector.newInstance();
    assertThat(vector.isEmpty()).isTrue();
    assertThat(vector.size()).isEqualTo(0);
  }

  @Test
  public void should_behave_mostly_like_a_list() {

    CqlVector<Float> vector = CqlVector.newInstance(VECTOR_ARGS);
    assertThat(vector.get(0)).isEqualTo(VECTOR_ARGS[0]);
    Float newVal = VECTOR_ARGS[0] * 2;
    vector.set(0, newVal);
    assertThat(vector.get(0)).isEqualTo(newVal);
    assertThat(vector.isEmpty()).isFalse();
    assertThat(vector.size()).isEqualTo(2);
    assertThat(Iterators.toArray(vector.iterator(), Float.class)).isEqualTo(VECTOR_ARGS);
  }

  @Test
  public void should_play_nicely_with_streams() {

    CqlVector<Float> vector = CqlVector.newInstance(VECTOR_ARGS);
    List<Float> results =
        vector.stream()
            .map((f) -> f * 2)
            .collect(Collectors.toCollection(() -> new ArrayList<Float>()));
    for (int i = 0; i < vector.size(); ++i) {
      assertThat(results.get(i)).isEqualTo(vector.get(i) * 2);
    }
  }

  @Test
  public void should_reflect_changes_to_mutable_list() {

    List<Float> theList = Lists.newArrayList(1.1f, 2.2f, 3.3f);
    CqlVector<Float> vector = CqlVector.newInstance(theList);
    assertThat(vector.size()).isEqualTo(3);
    assertThat(vector.get(2)).isEqualTo(3.3f);

    float newVal1 = 4.4f;
    theList.set(2, newVal1);
    assertThat(vector.size()).isEqualTo(3);
    assertThat(vector.get(2)).isEqualTo(newVal1);

    float newVal2 = 5.5f;
    theList.add(newVal2);
    assertThat(vector.size()).isEqualTo(4);
    assertThat(vector.get(3)).isEqualTo(newVal2);
  }

  @Test
  public void should_reflect_changes_to_array() {

    Float[] theArray = new Float[] {1.1f, 2.2f, 3.3f};
    CqlVector<Float> vector = CqlVector.newInstance(theArray);
    assertThat(vector.size()).isEqualTo(3);
    assertThat(vector.get(2)).isEqualTo(3.3f);

    float newVal1 = 4.4f;
    theArray[2] = newVal1;
    assertThat(vector.size()).isEqualTo(3);
    assertThat(vector.get(2)).isEqualTo(newVal1);
  }

  @Test
  public void should_correctly_compare_vectors() {

    Float[] args = VECTOR_ARGS.clone();
    CqlVector<Float> vector1 = CqlVector.newInstance(args);
    CqlVector<Float> vector2 = CqlVector.newInstance(args);
    CqlVector<Float> vector3 = CqlVector.newInstance(Lists.newArrayList(args));
    assertThat(vector1).isNotSameAs(vector2);
    assertThat(vector1).isEqualTo(vector2);
    assertThat(vector1).isNotSameAs(vector3);
    assertThat(vector1).isEqualTo(vector3);

    Float[] differentArgs = args.clone();
    float newVal = differentArgs[0] * 2;
    differentArgs[0] = newVal;
    CqlVector<Float> vector4 = CqlVector.newInstance(differentArgs);
    assertThat(vector1).isNotSameAs(vector4);
    assertThat(vector1).isNotEqualTo(vector4);

    Float[] biggerArgs = Arrays.copyOf(args, args.length + 1);
    biggerArgs[biggerArgs.length - 1] = newVal;
    CqlVector<Float> vector5 = CqlVector.newInstance(biggerArgs);
    assertThat(vector1).isNotSameAs(vector5);
    assertThat(vector1).isNotEqualTo(vector5);
  }

  @Test
  public void should_serialize_and_deserialize() throws Exception {
    CqlVector<Float> initial = CqlVector.newInstance(VECTOR_ARGS);
    CqlVector<Float> deserialized = SerializationHelper.serializeAndDeserialize(initial);
    assertThat(deserialized).isEqualTo(initial);
  }

  @Test
  public void should_serialize_and_deserialize_empty_vector() throws Exception {
    CqlVector<Float> initial = CqlVector.newInstance(Collections.emptyList());
    CqlVector<Float> deserialized = SerializationHelper.serializeAndDeserialize(initial);
    assertThat(deserialized).isEqualTo(initial);
  }

  @Test
  public void should_serialize_and_deserialize_unserializable_list() throws Exception {
    CqlVector<Float> initial =
        CqlVector.newInstance(
            new AbstractList<Float>() {
              @Override
              public Float get(int index) {
                return VECTOR_ARGS[index];
              }

              @Override
              public int size() {
                return VECTOR_ARGS.length;
              }
            });
    CqlVector<Float> deserialized = SerializationHelper.serializeAndDeserialize(initial);
    assertThat(deserialized).isEqualTo(initial);
  }

  @Test
  public void should_not_use_preallocate_serialized_size() throws DecoderException {
    // serialized CqlVector<Float>(1.0f, 2.5f, 3.0f) with size field adjusted to Integer.MAX_VALUE
    byte[] suspiciousBytes =
        Hex.decodeHex(
            "aced000573720042636f6d2e64617461737461782e6f73732e6472697665722e6170692e636f72652e646174612e43716c566563746f722453657269616c697a6174696f6e50726f78790000000000000001030000787077047fffffff7372000f6a6176612e6c616e672e466c6f6174daedc9a2db3cf0ec02000146000576616c7565787200106a6176612e6c616e672e4e756d62657286ac951d0b94e08b02000078703f8000007371007e0002402000007371007e00024040000078"
                .toCharArray());
    try {
      new ObjectInputStream(new ByteArrayInputStream(suspiciousBytes)).readObject();
      fail("Should not be able to deserialize bytes with incorrect size field");
    } catch (Exception e) {
      // check we fail to deserialize, rather than OOM
      assertThat(e).isInstanceOf(ObjectStreamException.class);
    }
  }
}
