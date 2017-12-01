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

import java.util.concurrent.ThreadLocalRandom;

public class ArrayUtils {

  public static <T> void swap(T[] elements, int i, int j) {
    if (i != j) {
      T tmp = elements[i];
      elements[i] = elements[j];
      elements[j] = tmp;
    }
  }

  /**
   * Moves an element towards the beginning of the array, shifting all the intermediary elements to
   * the right (no-op if targetIndex >= sourceIndex).
   */
  public static <T> void bubbleUp(T[] elements, int sourceIndex, int targetIndex) {
    for (int i = sourceIndex; i > targetIndex; i--) {
      swap(elements, i, i - 1);
    }
  }

  /**
   * Shuffles the first n elements of the array in-place.
   *
   * @see <a
   *     href="https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle#The_modern_algorithm">Modern
   *     Fisher-Yates shuffle</a>
   */
  public static <T> void shuffleHead(T[] elements, int n) {
    if (n > 1) {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      for (int i = n - 1; i > 0; i--) {
        int j = random.nextInt(i + 1);
        swap(elements, i, j);
      }
    }
  }
}
