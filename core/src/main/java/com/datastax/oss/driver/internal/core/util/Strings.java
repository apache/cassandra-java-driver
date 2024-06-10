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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Objects;

public class Strings {

  /**
   * Return {@code true} if the given string is surrounded by single quotes, and {@code false}
   * otherwise.
   *
   * @param value The string to inspect.
   * @return {@code true} if the given string is surrounded by single quotes, and {@code false}
   *     otherwise.
   */
  public static boolean isQuoted(String value) {
    return isQuoted(value, '\'');
  }

  /**
   * Quote the given string; single quotes are escaped. If the given string is null, this method
   * returns a quoted empty string ({@code ''}).
   *
   * @param value The value to quote.
   * @return The quoted string.
   */
  public static String quote(String value) {
    return quote(value, '\'');
  }

  /**
   * Unquote the given string if it is quoted; single quotes are unescaped. If the given string is
   * not quoted, it is returned without any modification.
   *
   * @param value The string to unquote.
   * @return The unquoted string.
   */
  public static String unquote(String value) {
    return unquote(value, '\'');
  }

  /**
   * Return {@code true} if the given string is surrounded by double quotes, and {@code false}
   * otherwise.
   *
   * @param value The string to inspect.
   * @return {@code true} if the given string is surrounded by double quotes, and {@code false}
   *     otherwise.
   */
  public static boolean isDoubleQuoted(String value) {
    return isQuoted(value, '\"');
  }

  /**
   * Double quote the given string; double quotes are escaped. If the given string is null, this
   * method returns a quoted empty string ({@code ""}).
   *
   * @param value The value to double quote.
   * @return The double quoted string.
   */
  public static String doubleQuote(String value) {
    return quote(value, '"');
  }

  /**
   * Unquote the given string if it is double quoted; double quotes are unescaped. If the given
   * string is not double quoted, it is returned without any modification.
   *
   * @param value The string to un-double quote.
   * @return The un-double quoted string.
   */
  public static String unDoubleQuote(String value) {
    return unquote(value, '"');
  }

  /** Whether a string needs double quotes to be a valid CQL identifier. */
  public static boolean needsDoubleQuotes(String s) {
    // this method should only be called for C*-provided identifiers,
    // so we expect it to be non-null and non-empty.
    assert s != null && !s.isEmpty();
    char c = s.charAt(0);
    if (!(c >= 97 && c <= 122)) // a-z
    return true;
    for (int i = 1; i < s.length(); i++) {
      c = s.charAt(i);
      if (!((c >= 48 && c <= 57) // 0-9
          || (c == 95) // _
          || (c >= 97 && c <= 122) // a-z
      )) {
        return true;
      }
    }
    return isReservedCqlKeyword(s);
  }

  /**
   * Return {@code true} if the given string is surrounded by the quote character given, and {@code
   * false} otherwise.
   *
   * @param value The string to inspect.
   * @return {@code true} if the given string is surrounded by the quote character, and {@code
   *     false} otherwise.
   */
  private static boolean isQuoted(String value, char quoteChar) {
    return value != null
        && value.length() > 1
        && value.charAt(0) == quoteChar
        && value.charAt(value.length() - 1) == quoteChar;
  }

  /**
   * @param quoteChar " or '
   * @return A quoted empty string.
   */
  private static String emptyQuoted(char quoteChar) {
    // don't handle non quote characters, this is done so that these are interned and don't create
    // repeated empty quoted strings.
    assert quoteChar == '"' || quoteChar == '\'';
    if (quoteChar == '"') return "\"\"";
    else return "''";
  }

  /**
   * Quotes text and escapes any existing quotes in the text. {@code String.replace()} is a bit too
   * inefficient (see JAVA-67, JAVA-1262).
   *
   * @param text The text.
   * @param quoteChar The character to use as a quote.
   * @return The text with surrounded in quotes with all existing quotes escaped with (i.e. '
   *     becomes '')
   */
  private static String quote(String text, char quoteChar) {
    if (text == null || text.isEmpty()) return emptyQuoted(quoteChar);

    int nbMatch = 0;
    int start = -1;
    do {
      start = text.indexOf(quoteChar, start + 1);
      if (start != -1) ++nbMatch;
    } while (start != -1);

    // no quotes found that need to be escaped, simply surround in quotes and return.
    if (nbMatch == 0) return quoteChar + text + quoteChar;

    // 2 for beginning and end quotes.
    // length for original text
    // nbMatch for escape characters to add to quotes to be escaped.
    int newLength = 2 + text.length() + nbMatch;
    char[] result = new char[newLength];
    result[0] = quoteChar;
    result[newLength - 1] = quoteChar;
    int newIdx = 1;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == quoteChar) {
        // escape quote with another occurrence.
        result[newIdx++] = c;
        result[newIdx++] = c;
      } else {
        result[newIdx++] = c;
      }
    }
    return new String(result);
  }

  /**
   * Unquotes text and unescapes non surrounding quotes. {@code String.replace()} is a bit too
   * inefficient (see JAVA-67, JAVA-1262).
   *
   * @param text The text
   * @param quoteChar The character to use as a quote.
   * @return The text with surrounding quotes removed and non surrounding quotes unescaped (i.e. ''
   *     becomes ')
   */
  private static String unquote(String text, char quoteChar) {
    if (!isQuoted(text, quoteChar)) return text;

    if (text.length() == 2) return "";

    String search = emptyQuoted(quoteChar);
    int nbMatch = 0;
    int start = -1;
    do {
      start = text.indexOf(search, start + 2);
      // ignore the second to last character occurrence, as the last character is a quote.
      if (start != -1 && start != text.length() - 2) ++nbMatch;
    } while (start != -1);

    // no escaped quotes found, simply remove surrounding quotes and return.
    if (nbMatch == 0) return text.substring(1, text.length() - 1);

    // length of the new string will be its current length - the number of occurrences.
    int newLength = text.length() - nbMatch - 2;
    char[] result = new char[newLength];
    int newIdx = 0;
    // track whenever a quoteChar is encountered and the previous character is not a quoteChar.
    boolean firstFound = false;
    for (int i = 1; i < text.length() - 1; i++) {
      char c = text.charAt(i);
      if (c == quoteChar) {
        if (firstFound) {
          // The previous character was a quoteChar, don't add this to result, this action in
          // effect removes consecutive quotes.
          firstFound = false;
        } else {
          // found a quoteChar and the previous character was not a quoteChar, include in result.
          firstFound = true;
          result[newIdx++] = c;
        }
      } else {
        // non quoteChar encountered, include in result.
        result[newIdx++] = c;
        firstFound = false;
      }
    }
    return new String(result);
  }

  @VisibleForTesting
  static boolean isReservedCqlKeyword(String id) {
    return id != null && RESERVED_KEYWORDS.contains(id.toLowerCase(Locale.ROOT));
  }

  /**
   * Check whether the given string corresponds to a valid CQL long literal. Long literals are
   * composed solely by digits, but can have an optional leading minus sign.
   *
   * @param str The string to inspect.
   * @return {@code true} if the given string corresponds to a valid CQL integer literal, {@code
   *     false} otherwise.
   */
  public static boolean isLongLiteral(String str) {
    if (str == null || str.isEmpty()) return false;
    char[] chars = str.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      char c = chars[i];
      if ((c < '0' && (i != 0 || c != '-')) || c > '9') return false;
    }
    return true;
  }

  /**
   * Checks whether the given text is not null and not empty.
   *
   * @param text The text to check.
   * @param name The name of the argument.
   * @return The text (for method chaining).
   */
  public static String requireNotEmpty(String text, String name) {
    Objects.requireNonNull(text, name + " cannot be null");
    if (text.isEmpty()) {
      throw new IllegalArgumentException(name + " cannot be empty");
    }
    return text;
  }

  private Strings() {}

  private static final ImmutableSet<String> RESERVED_KEYWORDS =
      ImmutableSet.of(
          "-infinity",
          "-nan",
          "add",
          "allow",
          "alter",
          "and",
          "any",
          "apply",
          "asc",
          "authorize",
          "batch",
          "begin",
          "by",
          "cast",
          "columnfamily",
          "create",
          "default",
          "delete",
          "desc",
          "describe",
          "drop",
          "each_quorum",
          "entries",
          "execute",
          "from",
          "full",
          "grant",
          "if",
          "in",
          "index",
          "inet",
          "infinity",
          "insert",
          "into",
          "is",
          "keyspace",
          "keyspaces",
          "limit",
          "local_one",
          "local_quorum",
          "materialized",
          "mbean",
          "mbeans",
          "modify",
          "nan",
          "norecursive",
          "not",
          "null",
          "of",
          "on",
          "one",
          "or",
          "order",
          "password",
          "primary",
          "quorum",
          "rename",
          "replace",
          "revoke",
          "schema",
          "scylla_clustering_bound",
          "scylla_counter_shard_list",
          "scylla_timeuuid_list_index",
          "select",
          "set",
          "table",
          "to",
          "token",
          "three",
          "truncate",
          "two",
          "unlogged",
          "unset",
          "update",
          "use",
          "using",
          "view",
          "where",
          "with");
}
