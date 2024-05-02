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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;

/**
 * A very simple json parser. The only reason we need to read json in the driver is because for
 * historical reason Cassandra encodes a few properties using json in the schema and we need to
 * decode them.
 *
 * <p>We however don't need a full-blown JSON library because: 1) we know we only need to decode
 * string lists and string maps 2) we can basically assume the input is valid, we don't particularly
 * have to bother about decoding exactly JSON as long as we at least decode what we need. 3) we
 * don't really care much about performance, none of this is done in performance sensitive parts.
 *
 * <p>So instead of pulling a new dependency, we roll out our own very dumb parser. We should
 * obviously not expose this publicly.
 */
@NotThreadSafe
public class SimpleJsonParser {

  private final String input;
  private int idx;

  private SimpleJsonParser(String input) {
    this.input = input;
  }

  @SuppressWarnings("MixedMutabilityReturnType")
  public static List<String> parseStringList(String input) {
    if (input == null || input.isEmpty()) {
      return Collections.emptyList();
    }

    List<String> output = new ArrayList<>();
    SimpleJsonParser parser = new SimpleJsonParser(input);
    if (parser.nextCharSkipSpaces() != '[') {
      throw new IllegalArgumentException("Not a JSON list: " + input);
    }

    char c = parser.nextCharSkipSpaces();
    if (c == ']') {
      return output;
    }

    while (true) {
      assert c == '"';
      output.add(parser.nextString());
      c = parser.nextCharSkipSpaces();
      if (c == ']') {
        return output;
      }
      assert c == ',';
      c = parser.nextCharSkipSpaces();
    }
  }

  @SuppressWarnings("MixedMutabilityReturnType")
  public static Map<String, String> parseStringMap(String input) {
    if (input == null || input.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, String> output = new HashMap<>();
    SimpleJsonParser parser = new SimpleJsonParser(input);
    if (parser.nextCharSkipSpaces() != '{') {
      throw new IllegalArgumentException("Not a JSON map: " + input);
    }

    char c = parser.nextCharSkipSpaces();
    if (c == '}') {
      return output;
    }

    while (true) {
      assert c == '"';
      String key = parser.nextString();
      c = parser.nextCharSkipSpaces();
      assert c == ':';
      c = parser.nextCharSkipSpaces();
      assert c == '"';
      String value = parser.nextString();
      output.put(key, value);
      c = parser.nextCharSkipSpaces();
      if (c == '}') {
        return output;
      }
      assert c == ',';
      c = parser.nextCharSkipSpaces();
    }
  }

  /** Read the next char, the one at position idx, and advance ix. */
  private char nextChar() {
    if (idx >= input.length()) {
      throw new IllegalArgumentException("Invalid json input: " + input);
    }
    return input.charAt(idx++);
  }

  /** Same as nextChar, except that it skips space characters (' ', '\t' and '\n'). */
  private char nextCharSkipSpaces() {
    char c = nextChar();
    while (c == ' ' || c == '\t' || c == '\n') {
      c = nextChar();
    }
    return c;
  }

  /**
   * Reads a String, assuming idx is on the first character of the string (i.e. the one after the
   * opening double-quote character). After the string has been read, idx will be on the first
   * character after the closing double-quote.
   */
  private String nextString() {
    assert input.charAt(idx - 1) == '"' : "Char is '" + input.charAt(idx - 1) + '\'';
    StringBuilder sb = new StringBuilder();
    while (true) {
      char c = nextChar();
      switch (c) {
        case '\n':
        case '\r':
          throw new IllegalArgumentException("Unterminated string");
        case '\\':
          c = nextChar();
          switch (c) {
            case 'b':
              sb.append('\b');
              break;
            case 't':
              sb.append('\t');
              break;
            case 'n':
              sb.append('\n');
              break;
            case 'f':
              sb.append('\f');
              break;
            case 'r':
              sb.append('\r');
              break;
            case 'u':
              sb.append((char) Integer.parseInt(input.substring(idx, idx + 4), 16));
              idx += 4;
              break;
            case '"':
            case '\'':
            case '\\':
            case '/':
              sb.append(c);
              break;
            default:
              throw new IllegalArgumentException("Illegal escape");
          }
          break;
        default:
          if (c == '"') {
            return sb.toString();
          }
          sb.append(c);
      }
    }
  }
}
