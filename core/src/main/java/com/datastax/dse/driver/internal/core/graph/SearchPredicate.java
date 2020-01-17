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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * List of predicates for geolocation usage with DseGraph and Search indexes. Should not be accessed
 * directly but through the {@link com.datastax.dse.driver.api.core.graph.predicates.Search} static
 * methods.
 */
public enum SearchPredicate implements DsePredicate {
  /** Whether the text contains a given term as a token in the text (case insensitive). */
  token {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      return value != null && evaluate(value.toString(), (String) condition);
    }

    boolean evaluate(String value, String terms) {
      Set<String> tokens = Sets.newHashSet(tokenize(value.toLowerCase()));
      terms = terms.trim();
      List<String> tokenTerms = tokenize(terms.toLowerCase());
      if (!terms.isEmpty() && tokenTerms.isEmpty()) {
        return false;
      }
      for (String term : tokenTerms) {
        if (!tokens.contains(term)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean isValidCondition(Object condition) {
      return condition != null && isNotBlank((String) condition);
    }

    @Override
    public String toString() {
      return "token";
    }
  },

  /** Whether the text contains a token that starts with a given term (case insensitive). */
  tokenPrefix {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      return value != null && evaluate(value.toString(), (String) condition);
    }

    boolean evaluate(String value, String prefix) {
      for (String token : tokenize(value.toLowerCase())) {
        if (token.startsWith(prefix.toLowerCase().trim())) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean isValidCondition(Object condition) {
      return condition != null;
    }

    @Override
    public String toString() {
      return "tokenPrefix";
    }
  },

  /** Whether the text contains a token that matches a regular expression (case insensitive). */
  tokenRegex {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      return value != null && evaluate(value.toString(), (String) condition);
    }

    boolean evaluate(String value, String regex) {
      Pattern compiled = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
      for (String token : tokenize(value.toLowerCase())) {
        if (compiled.matcher(token).matches()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean isValidCondition(Object condition) {
      return condition != null && isNotBlank((String) condition);
    }

    @Override
    public String toString() {
      return "tokenRegex";
    }
  },

  /**
   * Whether some token in the text is within a given edit distance from the given term (case
   * insensitive).
   */
  tokenFuzzy {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      if (value == null) {
        return false;
      }

      EditDistance fuzzyCondition = (EditDistance) condition;

      for (String token : tokenize(value.toString().toLowerCase())) {
        if (SearchUtils.getOptimalStringAlignmentDistance(token, fuzzyCondition.query.toLowerCase())
            <= fuzzyCondition.distance) {
          return true;
        }
      }

      return false;
    }

    @Override
    public boolean isValidCondition(Object condition) {
      return condition != null;
    }

    @Override
    public String toString() {
      return "tokenFuzzy";
    }
  },

  /** Whether the text starts with a given prefix (case sensitive). */
  prefix {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      return value != null && value.toString().startsWith(((String) condition).trim());
    }

    @Override
    public boolean isValidCondition(Object condition) {
      return condition != null;
    }

    @Override
    public String toString() {
      return "prefix";
    }
  },

  /** Whether the text matches a regular expression (case sensitive). */
  regex {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      return value != null
          && Pattern.compile((String) condition, Pattern.DOTALL)
              .matcher(value.toString())
              .matches();
    }

    @Override
    public boolean isValidCondition(Object condition) {
      return condition != null && isNotBlank((String) condition);
    }

    @Override
    public String toString() {
      return "regex";
    }
  },

  /** Whether the text is within a given edit distance from the given term (case sensitive). */
  fuzzy {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      if (value == null) {
        return false;
      }
      EditDistance fuzzyCondition = (EditDistance) condition;
      return SearchUtils.getOptimalStringAlignmentDistance(value.toString(), fuzzyCondition.query)
          <= fuzzyCondition.distance;
    }

    @Override
    public boolean isValidCondition(Object condition) {
      return condition != null;
    }

    @Override
    public String toString() {
      return "fuzzy";
    }
  },

  /**
   * Whether tokenized text contains a given phrase, optionally within a given proximity (case
   * insensitive).
   */
  phrase {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      if (value == null) {
        return false;
      }

      EditDistance phraseCondition = (EditDistance) condition;

      List<String> valueTokens = tokenize(value.toString().toLowerCase());
      List<String> phraseTokens = tokenize(phraseCondition.query.toLowerCase());

      int valuePosition = 0;
      int phrasePosition = 0;
      int distance = 0;

      // Look for matches while phrase/value tokens and distance budget remain
      while (phrasePosition < phraseTokens.size()
          && valuePosition < valueTokens.size()
          && distance <= phraseCondition.distance) {

        if (phraseTokens.get(phrasePosition).equals(valueTokens.get(valuePosition))) {
          // Early return-true when we've matched the whole phrase (within the specified distance)
          if (phrasePosition == phraseTokens.size() - 1) {
            return true;
          }
          phrasePosition++;
        } else if (0 < phrasePosition) {
          // We've previously found at least one matching token in the input string,
          // but the current token does not match the phrase.  Increment distance.
          distance++;
        }

        valuePosition++;
      }

      return false;
    }

    @Override
    public boolean isValidCondition(Object condition) {
      return condition != null;
    }

    @Override
    public String toString() {
      return "phrase";
    }
  };

  private static boolean isNotBlank(String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    int strLen = str.length();
    for (int i = 0; i < strLen; i++) {
      if (!Character.isWhitespace(str.charAt(i))) {
        return true;
      }
    }
    return false;
  }

  // Match anything that is not either:
  // 1) a unicode letter, regardless of subcategory (same as Character.isLetter), or
  // 2) a unicode decimal digit number (same as Character.isDigit)
  private static final Pattern TOKEN_SPLIT_PATTERN = Pattern.compile("[^\\p{L}\\p{Nd}]");

  static List<String> tokenize(String str) {
    String[] rawTokens = TOKEN_SPLIT_PATTERN.split(str); // could contain empty strings
    return Stream.of(rawTokens).filter(t -> 0 < t.length()).collect(Collectors.toList());
  }
}
