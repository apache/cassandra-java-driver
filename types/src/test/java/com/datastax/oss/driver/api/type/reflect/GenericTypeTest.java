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
package com.datastax.oss.driver.api.type.reflect;

import com.google.common.reflect.TypeToken;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
