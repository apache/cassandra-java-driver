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

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.SerializationHelper;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.time.LocalTime;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class CqlVectorTest {

  private static final Float[] VECTOR_ARGS = {1.0f, 2.5f};

  @DataProvider
  public static Object[][] dataProvider() {
    return new Object[][] {
      {new Float[] {1.0f, 2.5f}},
      {new LocalTime[] {LocalTime.of(1, 2), LocalTime.of(3, 4)}},
      {new List[] {Arrays.asList(1, 2), Arrays.asList(3, 4)}},
      {new CqlVector[] {CqlVector.newInstance("a", "bc"), CqlVector.newInstance("d", "ef")}}
    };
  }

  private void validate_built_vector(CqlVector<?> vec, Object[] expectedVals) {
    assertThat(vec.size()).isEqualTo(2);
    assertThat(vec.isEmpty()).isFalse();
    assertThat(vec.get(0)).isEqualTo(expectedVals[0]);
    assertThat(vec.get(1)).isEqualTo(expectedVals[1]);
  }

  @UseDataProvider("dataProvider")
  @Test
  public void should_build_vector_from_elements(Object[] vals) {
    validate_built_vector(CqlVector.newInstance(vals), vals);
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_build_vector_from_list(Object[] vals) {
    validate_built_vector(CqlVector.newInstance(Lists.newArrayList(vals)), vals);
  }

  @Test
  @UseDataProvider("dataProvider")
  public void should_build_vector_from_tostring_output(Object[] vals) {
    CqlVector<?> vector1 = CqlVector.newInstance(vals);
    TypeCodec<?> codec = CodecRegistry.DEFAULT.codecFor(vals[0]);
    CqlVector<?> vector2 = CqlVector.from(vector1.toString(), codec);
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
  @UseDataProvider("dataProvider")
  public <T> void should_behave_mostly_like_a_list(T[] vals) {
    T[] theArray = Arrays.copyOf(vals, vals.length);
    CqlVector<T> vector = CqlVector.newInstance(theArray);
    assertThat(vector.get(0)).isEqualTo(theArray[0]);
    vector.set(0, theArray[1]);
    assertThat(vector.get(0)).isEqualTo(theArray[1]);
    assertThat(vector.isEmpty()).isFalse();
    assertThat(vector.size()).isEqualTo(2);
    Iterator<?> iterator = vector.iterator();
    assertThat(iterator.next()).isEqualTo(theArray[1]);
    assertThat(iterator.next()).isEqualTo(theArray[1]);
  }

  @Test
  @UseDataProvider("dataProvider")
  public <T> void should_play_nicely_with_streams(T[] vals) {
    CqlVector<T> vector = CqlVector.newInstance(vals);
    List<String> results =
        vector.stream()
            .map(Object::toString)
            .collect(Collectors.toCollection(() -> new ArrayList<String>()));
    for (int i = 0; i < vector.size(); ++i) {
      assertThat(results.get(i)).isEqualTo(vector.get(i).toString());
    }
  }

  @Test
  @UseDataProvider("dataProvider")
  public <T> void should_reflect_changes_to_mutable_list(T[] vals) {
    List<T> theList = Lists.newArrayList(vals);
    CqlVector<T> vector = CqlVector.newInstance(theList);
    assertThat(vector.size()).isEqualTo(2);
    assertThat(vector.get(1)).isEqualTo(vals[1]);

    T newVal = vals[0];
    theList.set(1, newVal);
    assertThat(vector.size()).isEqualTo(2);
    assertThat(vector.get(1)).isEqualTo(newVal);
  }

  @Test
  @UseDataProvider("dataProvider")
  public <T> void should_reflect_changes_to_array(T[] vals) {
    T[] theArray = Arrays.copyOf(vals, vals.length);
    CqlVector<T> vector = CqlVector.newInstance(theArray);
    assertThat(vector.size()).isEqualTo(2);
    assertThat(vector.get(1)).isEqualTo(theArray[1]);

    T newVal = theArray[0];
    theArray[1] = newVal;
    assertThat(vector.size()).isEqualTo(2);
    assertThat(vector.get(1)).isEqualTo(newVal);
  }

  @Test
  @UseDataProvider("dataProvider")
  public <T> void should_correctly_compare_vectors(T[] vals) {
    CqlVector<T> vector1 = CqlVector.newInstance(vals);
    CqlVector<T> vector2 = CqlVector.newInstance(vals);
    CqlVector<T> vector3 = CqlVector.newInstance(Lists.newArrayList(vals));
    assertThat(vector1).isNotSameAs(vector2);
    assertThat(vector1).isEqualTo(vector2);
    assertThat(vector1).isNotSameAs(vector3);
    assertThat(vector1).isEqualTo(vector3);

    T[] differentArgs = Arrays.copyOf(vals, vals.length);
    T newVal = differentArgs[1];
    differentArgs[0] = newVal;
    CqlVector<T> vector4 = CqlVector.newInstance(differentArgs);
    assertThat(vector1).isNotSameAs(vector4);
    assertThat(vector1).isNotEqualTo(vector4);

    T[] biggerArgs = Arrays.copyOf(vals, vals.length + 1);
    biggerArgs[biggerArgs.length - 1] = newVal;
    CqlVector<T> vector5 = CqlVector.newInstance(biggerArgs);
    assertThat(vector1).isNotSameAs(vector5);
    assertThat(vector1).isNotEqualTo(vector5);
  }

  @Test
  @UseDataProvider("dataProvider")
  public <T> void should_serialize_and_deserialize(T[] vals) throws Exception {
    CqlVector<T> initial = CqlVector.newInstance(vals);
    CqlVector<T> deserialized = SerializationHelper.serializeAndDeserialize(initial);
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
