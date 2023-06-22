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
package com.datastax.oss.driver.api.core.data;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import java.util.Arrays;
import org.junit.Test;

public class CqlVectorTest {

  private final Float[] FLOATS = new Float[] {1.1f, 2.2f, 3.3f, 4.4f, 5.5f};

  @Test
  public void should_generate_string_and_back() {

    CqlVector<Float> v = CqlVector.newInstance(FLOATS);
    assertThat(CqlVector.from(v.toString(), TypeCodecs.FLOAT)).isEqualTo(v);
  }

  @Test
  public void should_equal_equal_vectors() {

    CqlVector<Float> v1 = CqlVector.newInstance(FLOATS);
    CqlVector<Float> v2 = CqlVector.newInstance(FLOATS);
    Float[] floats2 = Arrays.copyOf(FLOATS, FLOATS.length + 1);
    floats2[floats2.length - 1] = 0.0f;
    CqlVector<Float> v3 = CqlVector.newInstance(floats2);

    assertThat(v1).isEqualTo(v2);
    assertThat(v1).isNotEqualTo(v3);
  }

  @Test
  public void should_not_equal_different_size_vectors() {

    CqlVector<Float> v1 = CqlVector.newInstance(FLOATS);
    CqlVector<Float> v2 = CqlVector.newInstance(Arrays.copyOf(FLOATS, (FLOATS.length - 1)));
    Float[] v3Array = Arrays.copyOf(FLOATS, FLOATS.length + 1);
    v3Array[v3Array.length - 1] = v3Array.length * 1.1f;
    CqlVector<Float> v3 = CqlVector.newInstance(v3Array);

    assertThat(v1).isNotEqualTo(v2);
    assertThat(v1).isNotEqualTo(v3);
  }

  @Test
  public void should_create_vectors_with_new_instance() {

    CqlVector<Float> v1 = CqlVector.newInstance(FLOATS);
    ImmutableList<Float> l =
        ImmutableList.<Float>builder().addAll(Iterators.forArray(FLOATS)).build();
    CqlVector<Float> v2 = CqlVector.newInstance(l);

    assertThat(v1).isEqualTo(v2);
  }

  @Test
  public void should_create_vectors_with_builder() {

    CqlVector<Float> v1 = CqlVector.newInstance(FLOATS);

    CqlVector.Builder<Float> b2 = CqlVector.builder();
    for (float f : v1) b2.add(f);
    CqlVector<Float> v2 = b2.build();

    CqlVector.Builder<Float> b3 = CqlVector.builder();
    b3.add(FLOATS);
    CqlVector<Float> v3 = b3.build();

    CqlVector.Builder<Float> b4 = CqlVector.builder();
    b4.addAll(ImmutableList.copyOf(FLOATS));
    CqlVector<Float> v4 = b4.build();

    assertThat(v1).isEqualTo(v2);
    assertThat(v1).isEqualTo(v3);
    assertThat(v1).isEqualTo(v4);
  }
}
