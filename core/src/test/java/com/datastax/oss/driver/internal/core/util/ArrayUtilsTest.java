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
package com.datastax.oss.driver.internal.core.util;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class ArrayUtilsTest {

  @Test
  public void should_swap() {
    String[] array = {"a", "b", "c"};
    ArrayUtils.swap(array, 0, 2);
    assertThat(array).containsExactly("c", "b", "a");
  }

  @Test
  public void should_swap_with_same_index() {
    String[] array = {"a", "b", "c"};
    ArrayUtils.swap(array, 0, 0);
    assertThat(array).containsExactly("a", "b", "c");
  }

  @Test
  public void should_bubble_up() {
    String[] array = {"a", "b", "c", "d", "e"};
    ArrayUtils.bubbleUp(array, 3, 1);
    assertThat(array).containsExactly("a", "d", "b", "c", "e");
  }

  @Test
  public void should_bubble_up_to_same_index() {
    String[] array = {"a", "b", "c", "d", "e"};
    ArrayUtils.bubbleUp(array, 3, 3);
    assertThat(array).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void should_not_bubble_up_when_target_index_higher() {
    String[] array = {"a", "b", "c", "d", "e"};
    ArrayUtils.bubbleUp(array, 3, 5);
    assertThat(array).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void should_shuffle_head() {
    // Testing for randomness is hard, so we call the method a large number of times, and check that
    // we get all permutations with a decent distribution.
    Map<List<String>, Integer> counts = new HashMap<>();
    for (int i = 0; i < 6000; i++) {
      String[] array = {"a", "b", "c", "d", "e"};
      ArrayUtils.shuffleHead(array, 3);

      // Tail elements should not move
      assertThat(array[3]).isEqualTo("d");
      assertThat(array[4]).isEqualTo("e");

      List<String> permutation = ImmutableList.of(array[0], array[1], array[2]);
      counts.merge(permutation, 1, (a, b) -> a + b);
    }

    assertThat(counts).hasSize(6);
    for (Integer count : counts.values()) {
      assertThat(count).isBetween(900, 1100);
    }
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void should_fail_to_shuffle_head_when_count_is_too_high() {
    ArrayUtils.shuffleHead(new String[] {"a", "b", "c"}, 5);
  }
}
