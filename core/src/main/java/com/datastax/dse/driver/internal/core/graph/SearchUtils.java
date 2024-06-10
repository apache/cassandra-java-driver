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
package com.datastax.dse.driver.internal.core.graph;

public class SearchUtils {

  /**
   * Finds the <a
   * href="https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance#Optimal_string_alignment_distance">Optimal
   * string alignment distance</a> – also referred to as the Damerau-Levenshtein distance – between
   * two strings.
   *
   * <p>This is the number of changes needed to change one string into another (insertions,
   * deletions or substitutions of a single character, or transpositions of two adjacent
   * characters).
   *
   * <p>This implementation is based on the Apache Commons Lang implementation of the Levenshtein
   * distance, only adding support for transpositions.
   *
   * <p>Note that this is the distance used in Lucene for {@code FuzzyTermsEnum}. Lucene itself has
   * an implementation of this algorithm, but it is much less efficient in terms of space (also note
   * that Lucene's implementation does not return the distance, but a similarity score based on it).
   *
   * @param s the first string, must not be {@code null}.
   * @param t the second string, must not be {@code null}.
   * @return The Optimal string alignment distance between the two strings.
   * @throws IllegalArgumentException if either String input is {@code null}.
   * @see org.apache.commons.lang.StringUtils#getLevenshteinDistance(String, String)
   * @see <a
   *     href="https://github.com/apache/lucene-solr/blob/releases/lucene-solr/6.4.2/lucene/suggest/src/java/org/apache/lucene/search/spell/LuceneLevenshteinDistance.java">
   *     <code>LuceneLevenshteinDistance</code></a>
   */
  public static int getOptimalStringAlignmentDistance(String s, String t) {

    /*
     * Code adapted from https://github.com/apache/commons-lang/blob/LANG_2_6/src/main/java/org/apache/commons/lang/StringUtils.java
     * which was originally released under the Apache 2.0 license with the following copyright:
     *
     * Licensed to the Apache Software Foundation (ASF) under one or more
     * contributor license agreements.  See the NOTICE file distributed with
     * this work for additional information regarding copyright ownership.
     * The ASF licenses this file to You under the Apache License, Version 2.0
     * (the "License"); you may not use this file except in compliance with
     * the License.  You may obtain a copy of the License at
     *
     *      http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */
    if (s == null || t == null) {
      throw new IllegalArgumentException("Strings must not be null");
    }

    int n = s.length(); // length of s
    int m = t.length(); // length of t

    if (n == 0) {
      return m;
    } else if (m == 0) {
      return n;
    }

    if (n > m) {
      // swap the input strings to consume less memory
      String tmp = s;
      s = t;
      t = tmp;
      n = m;
      m = t.length();
    }

    // instead of maintaining the full matrix in memory,
    // we use a sliding window containing 3 lines:
    // the current line being written to, and
    // the two previous ones.

    int d[] = new int[n + 1]; // current line in the cost matrix
    int p1[] = new int[n + 1]; // first line above the current one in the cost matrix
    int p2[] = new int[n + 1]; // second line above the current one in the cost matrix
    int _d[]; // placeholder to assist in swapping p1, p2 and d

    // indexes into strings s and t
    int i; // iterates through s
    int j; // iterates through t

    for (i = 0; i <= n; i++) {
      p1[i] = i;
    }

    for (j = 1; j <= m; j++) {

      // jth character of t
      char t_j = t.charAt(j - 1);
      d[0] = j;

      for (i = 1; i <= n; i++) {

        char s_i = s.charAt(i - 1);
        int cost = s_i == t_j ? 0 : 1;

        int deletion = d[i - 1] + 1; // cell to the left + 1
        int insertion = p1[i] + 1; // cell to the top + 1
        int substitution = p1[i - 1] + cost; // cell diagonally left and up + cost

        d[i] = Math.min(Math.min(deletion, insertion), substitution);

        // transposition
        if (i > 1 && j > 1 && s_i == t.charAt(j - 2) && s.charAt(i - 2) == t_j) {
          d[i] = Math.min(d[i], p2[i - 2] + cost);
        }
      }

      // swap arrays
      _d = p2;
      p2 = p1;
      p1 = d;
      d = _d;
    }

    // our last action in the above loop was to switch d and p1, so p1 now
    // actually has the most recent cost counts
    return p1[n];
  }
}
