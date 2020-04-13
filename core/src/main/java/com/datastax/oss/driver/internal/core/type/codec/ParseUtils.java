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
package com.datastax.oss.driver.internal.core.type.codec;

public class ParseUtils {

  /**
   * Returns the index of the first character in toParse from idx that is not a "space".
   *
   * @param toParse the string to skip space on.
   * @param idx the index to start skipping space from.
   * @return the index of the first character in toParse from idx that is not a "space.
   */
  public static int skipSpaces(String toParse, int idx) {
    while (idx < toParse.length() && isBlank(toParse.charAt(idx))) ++idx;
    return idx;
  }

  /**
   * Assuming that idx points to the beginning of a CQL value in toParse, returns the index of the
   * first character after this value.
   *
   * @param toParse the string to skip a value form.
   * @param idx the index to start parsing a value from.
   * @return the index ending the CQL value starting at {@code idx}.
   * @throws IllegalArgumentException if idx doesn't point to the start of a valid CQL value.
   */
  public static int skipCQLValue(String toParse, int idx) {
    if (idx >= toParse.length()) throw new IllegalArgumentException();

    if (isBlank(toParse.charAt(idx))) throw new IllegalArgumentException();

    int cbrackets = 0;
    int sbrackets = 0;
    int parens = 0;
    boolean inString = false;

    do {
      char c = toParse.charAt(idx);
      if (inString) {
        if (c == '\'') {
          if (idx + 1 < toParse.length() && toParse.charAt(idx + 1) == '\'') {
            ++idx; // this is an escaped quote, skip it
          } else {
            inString = false;
            if (cbrackets == 0 && sbrackets == 0 && parens == 0) return idx + 1;
          }
        }
        // Skip any other character
      } else if (c == '\'') {
        inString = true;
      } else if (c == '{') {
        ++cbrackets;
      } else if (c == '[') {
        ++sbrackets;
      } else if (c == '(') {
        ++parens;
      } else if (c == '}') {
        if (cbrackets == 0) return idx;

        --cbrackets;
        if (cbrackets == 0 && sbrackets == 0 && parens == 0) return idx + 1;
      } else if (c == ']') {
        if (sbrackets == 0) return idx;

        --sbrackets;
        if (cbrackets == 0 && sbrackets == 0 && parens == 0) return idx + 1;
      } else if (c == ')') {
        if (parens == 0) return idx;

        --parens;
        if (cbrackets == 0 && sbrackets == 0 && parens == 0) return idx + 1;
      } else if (isBlank(c) || !isCqlIdentifierChar(c)) {
        if (cbrackets == 0 && sbrackets == 0 && parens == 0) return idx;
      }
    } while (++idx < toParse.length());

    if (inString || cbrackets != 0 || sbrackets != 0 || parens != 0)
      throw new IllegalArgumentException();
    return idx;
  }

  /**
   * Assuming that idx points to the beginning of a CQL identifier in toParse, returns the index of
   * the first character after this identifier.
   *
   * @param toParse the string to skip an identifier from.
   * @param idx the index to start parsing an identifier from.
   * @return the index ending the CQL identifier starting at {@code idx}.
   * @throws IllegalArgumentException if idx doesn't point to the start of a valid CQL identifier.
   */
  public static int skipCQLId(String toParse, int idx) {
    if (idx >= toParse.length()) throw new IllegalArgumentException();

    char c = toParse.charAt(idx);
    if (isCqlIdentifierChar(c)) {
      while (idx < toParse.length() && isCqlIdentifierChar(toParse.charAt(idx))) idx++;
      return idx;
    }

    if (c != '"') throw new IllegalArgumentException();

    while (++idx < toParse.length()) {
      c = toParse.charAt(idx);
      if (c != '"') continue;

      if (idx + 1 < toParse.length() && toParse.charAt(idx + 1) == '\"')
        ++idx; // this is an escaped double quote, skip it
      else return idx + 1;
    }
    throw new IllegalArgumentException();
  }

  public static boolean isBlank(int c) {
    return c == ' ' || c == '\t' || c == '\n';
  }

  public static boolean isCqlIdentifierChar(int c) {
    return (c >= '0' && c <= '9')
        || (c >= 'a' && c <= 'z')
        || (c >= 'A' && c <= 'Z')
        || c == '-'
        || c == '+'
        || c == '.'
        || c == '_'
        || c == '&';
  }
}
