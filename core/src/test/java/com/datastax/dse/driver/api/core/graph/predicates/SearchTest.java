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
package com.datastax.dse.driver.api.core.graph.predicates;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

public class SearchTest {

  @Test
  public void testToken() {
    P<Object> p = Search.token("needle");
    assertThat(p.test("needle")).isTrue();
    assertThat(p.test("This is a needle in a haystack")).isTrue();
    assertThat(p.test("This is just the haystack")).isFalse();
  }

  @Test
  public void testPrefix() {
    P<Object> p = Search.prefix("abcd");
    assertThat(p.test("abcd")).isTrue();
    assertThat(p.test("abcdefg hijkl")).isTrue();
    assertThat(p.test("zabcd")).isFalse();
  }

  @Test
  public void testTokenPrefix() {
    P<Object> p = Search.tokenPrefix("abcd");
    assertThat(p.test("abcd")).isTrue();
    assertThat(p.test("abcdefg hijkl")).isTrue();
    assertThat(p.test("z abcd")).isTrue();
    assertThat(p.test("ab cd")).isFalse();
  }

  @Test
  public void testRegex() {
    P<Object> p = Search.regex("(foo|bar)");
    assertThat(p.test("foo")).isTrue();
    assertThat(p.test("bar")).isTrue();
    assertThat(p.test("foo bar")).isFalse();
  }

  @Test
  public void testTokenRegex() {
    P<Object> p = Search.tokenRegex("(foo|bar)");
    assertThat(p.test("foo")).isTrue();
    assertThat(p.test("bar")).isTrue();
    assertThat(p.test("foo bar")).isTrue();
    assertThat(p.test("foo bar qix")).isTrue();
    assertThat(p.test("qix")).isFalse();
  }

  @Test
  public void testPhrase() {
    P<Object> p = Search.phrase("Hello world", 2);
    assertThat(p.test("Hello World")).isTrue();
    assertThat(p.test("Hello Big World")).isTrue();
    assertThat(p.test("Hello Big Wild World")).isTrue();
    assertThat(p.test("Hello The Big Wild World")).isFalse();
    assertThat(p.test("Goodbye world")).isFalse();
  }

  @Test
  public void testPhraseFragment() {
    // Tests JAVA-1744
    P<Object> p = Search.phrase("a b", 0);
    assertThat(p.test("a b")).isTrue();
    assertThat(p.test("a")).isFalse();
    assertThat(p.test("b")).isFalse();
  }

  @Test
  public void testFuzzy() {
    P<Object> p = Search.fuzzy("abc", 1);
    assertThat(p.test("abcd")).isTrue();
    assertThat(p.test("ab")).isTrue();
    assertThat(p.test("abce")).isTrue();
    assertThat(p.test("abdc")).isTrue();
    assertThat(p.test("badc")).isFalse();

    // Make sure we do NOT calculate the Damerau–Levenshtein distance (2), but the optimal string
    // alignment distance (3):
    assertThat(Search.tokenFuzzy("ca", 2).test("abc")).isFalse();
  }

  @Test
  public void testTokenFuzzy() {
    P<Object> p = Search.tokenFuzzy("abc", 1);
    assertThat(p.test("foo abcd")).isTrue();
    assertThat(p.test("foo ab")).isTrue();
    assertThat(p.test("foo abce")).isTrue();
    assertThat(p.test("foo abdc")).isTrue();
    assertThat(p.test("foo badc")).isFalse();

    // Make sure we do NOT calculate the Damerau–Levenshtein distance (2), but the optimal string
    // alignment distance (3):
    assertThat(Search.tokenFuzzy("ca", 2).test("abc 123")).isFalse();
  }
}
