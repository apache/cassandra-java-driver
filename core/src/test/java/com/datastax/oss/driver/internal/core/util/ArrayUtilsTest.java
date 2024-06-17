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
package com.datastax.oss.driver.internal.core.util;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

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
  public void should_bubble_down() {
    String[] array = {"a", "b", "c", "d", "e"};
    ArrayUtils.bubbleDown(array, 1, 3);
    assertThat(array).containsExactly("a", "c", "d", "b", "e");
  }

  @Test
  public void should_bubble_down_to_same_index() {
    String[] array = {"a", "b", "c", "d", "e"};
    ArrayUtils.bubbleDown(array, 3, 3);
    assertThat(array).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void should_not_bubble_down_when_target_index_lower() {
    String[] array = {"a", "b", "c", "d", "e"};
    ArrayUtils.bubbleDown(array, 4, 2);
    assertThat(array).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void should_shuffle_head() {
    String[] array = {"a", "b", "c", "d", "e"};
    ThreadLocalRandom random = mock(ThreadLocalRandom.class);
    when(random.nextInt(anyInt()))
        .thenAnswer(
            (invocation) -> {
              int i = invocation.getArgument(0);
              // shifts elements by 1 to the right
              return i - 2;
            });
    ArrayUtils.shuffleHead(array, 3, random);
    assertThat(array[0]).isEqualTo("c");
    assertThat(array[1]).isEqualTo("a");
    assertThat(array[2]).isEqualTo("b");
    // Tail elements should not move
    assertThat(array[3]).isEqualTo("d");
    assertThat(array[4]).isEqualTo("e");
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void should_fail_to_shuffle_head_when_count_is_too_high() {
    ArrayUtils.shuffleHead(new String[] {"a", "b", "c"}, 5);
  }

  @Test
  public void should_rotate() {
    String[] array = {"a", "b", "c", "d", "e"};

    ArrayUtils.rotate(array, 1, 3, 1);
    assertThat(array).containsExactly("a", "c", "d", "b", "e");

    ArrayUtils.rotate(array, 0, 4, 2);
    assertThat(array).containsExactly("d", "b", "a", "c", "e");

    ArrayUtils.rotate(array, 2, 3, 10);
    assertThat(array).containsExactly("d", "b", "c", "e", "a");
  }

  @Test
  public void should_not_rotate_when_amount_multiple_of_range_size() {
    String[] array = {"a", "b", "c", "d", "e"};

    ArrayUtils.rotate(array, 1, 3, 9);
    assertThat(array).containsExactly("a", "b", "c", "d", "e");
  }

  @Test
  public void should_not_rotate_when_range_is_singleton_or_empty() {
    String[] array = {"a", "b", "c", "d", "e"};

    ArrayUtils.rotate(array, 1, 1, 3);
    assertThat(array).containsExactly("a", "b", "c", "d", "e");

    ArrayUtils.rotate(array, 1, 0, 3);
    assertThat(array).containsExactly("a", "b", "c", "d", "e");
  }
}
