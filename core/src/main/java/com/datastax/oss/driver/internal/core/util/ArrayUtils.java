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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.ThreadLocalRandom;

public class ArrayUtils {

  public static <ElementT> void swap(@NonNull ElementT[] elements, int i, int j) {
    if (i != j) {
      ElementT tmp = elements[i];
      elements[i] = elements[j];
      elements[j] = tmp;
    }
  }

  /**
   * Moves an element towards the beginning of the array, shifting all the intermediary elements to
   * the right (no-op if {@code targetIndex >= sourceIndex}).
   */
  public static <ElementT> void bubbleUp(
      @NonNull ElementT[] elements, int sourceIndex, int targetIndex) {
    for (int i = sourceIndex; i > targetIndex; i--) {
      swap(elements, i, i - 1);
    }
  }

  /**
   * Moves an element towards the end of the array, shifting all the intermediary elements to the
   * left (no-op if {@code targetIndex <= sourceIndex}).
   */
  public static <ElementT> void bubbleDown(
      @NonNull ElementT[] elements, int sourceIndex, int targetIndex) {
    for (int i = sourceIndex; i < targetIndex; i++) {
      swap(elements, i, i + 1);
    }
  }

  /**
   * Shuffles the first n elements of the array in-place.
   *
   * @param elements the array to shuffle.
   * @param n the number of elements to shuffle; must be {@code <= elements.length}.
   * @see <a
   *     href="https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle#The_modern_algorithm">Modern
   *     Fisher-Yates shuffle</a>
   */
  public static <ElementT> void shuffleHead(@NonNull ElementT[] elements, int n) {
    shuffleHead(elements, n, ThreadLocalRandom.current());
  }

  /**
   * Shuffles the first n elements of the array in-place.
   *
   * @param elements the array to shuffle.
   * @param n the number of elements to shuffle; must be {@code <= elements.length}.
   * @param random the {@link ThreadLocalRandom} instance to use. This is mainly intended to
   *     facilitate tests.
   * @see <a
   *     href="https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle#The_modern_algorithm">Modern
   *     Fisher-Yates shuffle</a>
   */
  public static <ElementT> void shuffleHead(
      @NonNull ElementT[] elements, int n, @NonNull ThreadLocalRandom random) {
    if (n > elements.length) {
      throw new ArrayIndexOutOfBoundsException(
          String.format(
              "Can't shuffle the first %d elements, there are only %d", n, elements.length));
    }
    if (n > 1) {
      for (int i = n - 1; i > 0; i--) {
        int j = random.nextInt(i + 1);
        swap(elements, i, j);
      }
    }
  }

  /** Rotates the elements in the specified range by the specified amount (round-robin). */
  public static <ElementT> void rotate(
      @NonNull ElementT[] elements, int startIndex, int length, int amount) {
    if (length >= 2) {
      amount = amount % length;
      // Repeatedly shift by 1. This is not the most time-efficient but the array will typically be
      // small so we don't care, and this avoids allocating a temporary buffer.
      for (int i = 0; i < amount; i++) {
        bubbleDown(elements, startIndex, startIndex + length - 1);
      }
    }
  }
}
