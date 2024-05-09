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
package com.datastax.oss.driver.api.core.type.reflect;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.reflect.TypeToken;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class GenericTypeTest {

  @Test
  public void should_wrap_class() {
    GenericType<String> stringType = GenericType.of(String.class);
    assertThat(stringType.__getToken()).isEqualTo(TypeToken.of(String.class));
  }

  @Test
  public void should_capture_generic_type() {
    GenericType<List<String>> stringListType = new GenericType<List<String>>() {};
    TypeToken<List<String>> stringListToken = new TypeToken<List<String>>() {};
    assertThat(stringListType.__getToken()).isEqualTo(stringListToken);
  }

  @Test
  public void should_wrap_classes_in_collection() {
    GenericType<Map<String, Integer>> mapType = GenericType.mapOf(String.class, Integer.class);
    assertThat(mapType.__getToken()).isEqualTo(new TypeToken<Map<String, Integer>>() {});
  }

  @Test
  public void should_wrap_types_in_collection() {
    GenericType<Map<String, List<Integer>>> mapType =
        GenericType.mapOf(GenericType.of(String.class), GenericType.listOf(Integer.class));
    assertThat(mapType.__getToken()).isEqualTo(new TypeToken<Map<String, List<Integer>>>() {});
  }

  @Test
  public void should_substitute_type_parameters() {
    assertThat(optionalOf(GenericType.listOf(String.class)).__getToken())
        .isEqualTo(new TypeToken<Optional<List<String>>>() {});
    assertThat(mapOf(String.class, Integer.class).__getToken())
        .isEqualTo(new TypeToken<Map<String, Integer>>() {});
  }

  @Test
  public void should_report_supertype() {
    assertThat(GenericType.of(Number.class).isSupertypeOf(GenericType.of(Integer.class))).isTrue();
    assertThat(GenericType.of(Integer.class).isSupertypeOf(GenericType.of(Number.class))).isFalse();
  }

  @Test
  public void should_report_subtype() {
    assertThat(GenericType.of(Number.class).isSubtypeOf(GenericType.of(Integer.class))).isFalse();
    assertThat(GenericType.of(Integer.class).isSubtypeOf(GenericType.of(Number.class))).isTrue();
  }

  @Test
  public void should_wrap_primitive_type() {
    assertThat(GenericType.of(Integer.TYPE).wrap()).isEqualTo(GenericType.of(Integer.class));
    GenericType<String> stringType = GenericType.of(String.class);
    assertThat(stringType.wrap()).isSameAs(stringType);
  }

  @Test
  public void should_unwrap_wrapper_type() {
    assertThat(GenericType.of(Integer.class).unwrap()).isEqualTo(GenericType.of(Integer.TYPE));
    GenericType<String> stringType = GenericType.of(String.class);
    assertThat(stringType.unwrap()).isSameAs(stringType);
  }

  @Test
  public void should_return_raw_type() {
    assertThat(GenericType.INTEGER.getRawType()).isEqualTo(Integer.class);
    assertThat(GenericType.listOf(Integer.class).getRawType()).isEqualTo(List.class);
  }

  @Test
  public void should_return_super_type() {
    GenericType<Iterable<Integer>> expectedType = iterableOf(GenericType.INTEGER);
    assertThat(GenericType.listOf(Integer.class).getSupertype(Iterable.class))
        .isEqualTo(expectedType);
  }

  @Test
  public void should_return_sub_type() {
    GenericType<Iterable<Integer>> superType = iterableOf(GenericType.INTEGER);
    assertThat(superType.getSubtype(List.class)).isEqualTo(GenericType.listOf(GenericType.INTEGER));
  }

  @Test
  public void should_return_type() {
    assertThat(GenericType.INTEGER.getType()).isEqualTo(Integer.class);
  }

  @Test
  public void should_return_component_type() {
    assertThat(GenericType.of(Integer[].class).getComponentType()).isEqualTo(GenericType.INTEGER);
  }

  @Test
  public void should_report_is_array() {
    assertThat(GenericType.INTEGER.isArray()).isFalse();
    assertThat(GenericType.of(Integer[].class).isArray()).isTrue();
  }

  private <T> GenericType<Optional<T>> optionalOf(GenericType<T> elementType) {
    return new GenericType<Optional<T>>() {}.where(new GenericTypeParameter<T>() {}, elementType);
  }

  private <T> GenericType<Iterable<T>> iterableOf(GenericType<T> elementType) {
    return new GenericType<Iterable<T>>() {}.where(new GenericTypeParameter<T>() {}, elementType);
  }

  private <K, V> GenericType<Map<K, V>> mapOf(Class<K> keyClass, Class<V> valueClass) {
    return new GenericType<Map<K, V>>() {}.where(new GenericTypeParameter<K>() {}, keyClass)
        .where(new GenericTypeParameter<V>() {}, valueClass);
  }
}
