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
package com.datastax.oss.driver;

import com.tngtech.java.junit.dataprovider.DataProvider;
import java.util.Arrays;
import java.util.Locale;

public class TestDataProviders {

  public static Object[][] fromList(Object... l) {
    Object[][] result = new Object[l.length][];
    for (int i = 0; i < l.length; i++) {
      result[i] = new Object[1];
      result[i][0] = l[i];
    }
    return result;
  }

  public static Object[][] concat(Object[][] left, Object[][] right) {
    Object[][] result = Arrays.copyOf(left, left.length + right.length);
    System.arraycopy(right, 0, result, left.length, right.length);
    return result;
  }

  // example: [ [a,b], [c,d] ], [ [1], [2] ], [ [true], [false] ]
  // => [ [a,b,1,true], [a,b,1,false], [a,b,2,true], [a,b,2,false], ... ]
  public static Object[][] combine(Object[][]... providers) {
    int numberOfProviders = providers.length; // (ex: 3)

    // ex: 2 * 2 * 2 combinations
    int numberOfCombinations = 1;
    for (Object[][] provider : providers) {
      numberOfCombinations *= provider.length;
    }

    Object[][] result = new Object[numberOfCombinations][];
    // The current index in each provider (ex: [1,0,1] => [c,d,1,false])
    int[] indices = new int[numberOfProviders];

    for (int c = 0; c < numberOfCombinations; c++) {
      int combinationLength = 0;
      for (int p = 0; p < numberOfProviders; p++) {
        combinationLength += providers[p][indices[p]].length;
      }
      Object[] combination = new Object[combinationLength];
      int destPos = 0;
      for (int p = 0; p < numberOfProviders; p++) {
        Object[] src = providers[p][indices[p]];
        System.arraycopy(src, 0, combination, destPos, src.length);
        destPos += src.length;
      }
      result[c] = combination;

      // Update indices: try to increment from the right, if it overflows reset and move left
      for (int p = providers.length - 1; p >= 0; p--) {
        if (indices[p] < providers[p].length - 1) {
          // ex: [0,0,0], p = 2 => [0,0,1]
          indices[p] += 1;
          break;
        } else {
          // ex: [0,0,1], p = 2 => [0,0,0], loop to increment to [0,1,0]
          indices[p] = 0;
        }
      }
    }
    return result;
  }

  @DataProvider
  public static Object[][] booleans() {
    return fromList(true, false);
  }

  /** An arbitrary set of locales to use when testing locale-sensitive operations. */
  @DataProvider
  public static Object[][] locales() {
    return new Object[][] {
      new Object[] {Locale.US},
      // non-latin alphabets
      new Object[] {Locale.CHINA},
      new Object[] {Locale.JAPAN},
      new Object[] {Locale.KOREA},
      new Object[] {new Locale("gr") /* greek */},
      new Object[] {new Locale("ar") /* arabic */},
      // latin-based alphabets with extended character sets
      new Object[] {new Locale("vi") /* vietnamese */},
      // JAVA-2883: Turkish is the most problematic locale as String.toLowerCase("TITLE")
      // wouldn't return "title" but rather  "tıtle", where 'ı' is the 'LATIN SMALL LETTER
      // DOTLESS I' character specific to the Turkish language.
      new Object[] {new Locale("tr") /* turkish*/},
    };
  }
}
