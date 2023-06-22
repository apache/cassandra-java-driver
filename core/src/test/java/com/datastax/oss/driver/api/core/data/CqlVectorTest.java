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
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.apache.commons.collections.bag.TreeBag;
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

  /** Not necessarily intended to be exhaustive; aiming more for good coverage */
  @Test
  public void should_export_any_collection_type() {

    CqlVector<Float> v = CqlVector.newInstance(FLOATS);

    /** =========== Lists =========== */
    List<Float> compareList = Lists.newArrayList(FLOATS);
    for (List l :
        new List[] {Lists.newArrayList(), Lists.newLinkedList(), Lists.newCopyOnWriteArrayList()}) {
      v.export(l);
      assertThat(l).isEqualTo(compareList);
    }

    /** =========== Set =========== */
    Set<Float> compareSet = Sets.newHashSet(Iterators.forArray(FLOATS));
    for (Set s :
        new Set[] {
          Sets.newHashSet(),
          Sets.newLinkedHashSet(),
          new TreeSet<Float>(),
          new ConcurrentSkipListSet<Float>()
        }) {
      v.export(s);
      assertThat(s).isEqualTo(compareSet);
    }

    /** =========== Queue =========== */
    Queue<Float> compareQueue = Queues.newConcurrentLinkedQueue(Lists.newArrayList(FLOATS));
    for (Queue q :
        new Queue[] {
          Queues.newArrayBlockingQueue(FLOATS.length),
          Queues.newArrayDeque(),
          Queues.newConcurrentLinkedQueue(),
          Queues.newLinkedBlockingQueue(),
          Queues.newLinkedBlockingDeque()
        }) {
      v.export(q);
      assertThat(Lists.newArrayList(q)).isEqualTo(Lists.newArrayList(compareQueue));
    }

    /** =========== Random third-party Collection impl =========== */
    Bag compareBag = new HashBag();
    for (Float f : v) compareBag.add(f);
    for (Collection c : new Collection[] {new HashBag(), new TreeBag()}) {
      v.export(c);
      assertThat(c).isEqualTo(compareBag);
    }
  }

  @Test
  public void should_export_list_of_supertype() {

    CqlVector<Float> v = CqlVector.newInstance(FLOATS);
    List<Number> l = Lists.newArrayList();
    v.export(l);

    List<Float> compareList = Lists.newArrayList(FLOATS);
    assertThat(l).isEqualTo(compareList);
  }

  @Test
  public void should_return_list_on_export() {

    CqlVector<Float> v = CqlVector.newInstance(FLOATS);
    Collection<Float> rv = (Collection<Float>) v.export(Lists.newArrayList());

    List<Float> compareList = Lists.newArrayList(FLOATS);
    assertThat(rv).isEqualTo(compareList);
  }
}
