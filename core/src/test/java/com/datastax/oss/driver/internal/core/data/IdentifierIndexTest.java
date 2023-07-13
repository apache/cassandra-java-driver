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
package com.datastax.oss.driver.internal.core.data;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import org.junit.Test;

public class IdentifierIndexTest {
  private static final CqlIdentifier Foo = CqlIdentifier.fromInternal("Foo");
  private static final CqlIdentifier foo = CqlIdentifier.fromInternal("foo");
  private static final CqlIdentifier fOO = CqlIdentifier.fromInternal("fOO");
  private IdentifierIndex index =
      new IdentifierIndex(ImmutableList.of(Foo, foo, fOO, Foo, foo, fOO));

  @Test
  public void should_find_first_index_of_existing_identifier() {
    assertThat(index.firstIndexOf(Foo)).isEqualTo(0);
    assertThat(index.firstIndexOf(foo)).isEqualTo(1);
    assertThat(index.firstIndexOf(fOO)).isEqualTo(2);
  }

  @Test
  public void should_not_find_index_of_nonexistent_identifier() {
    assertThat(index.firstIndexOf(CqlIdentifier.fromInternal("FOO"))).isEqualTo(-1);
  }

  @Test
  public void should_find_first_index_of_case_insensitive_name() {
    assertThat(index.firstIndexOf("foo")).isEqualTo(0);
  }

  @Test
  public void should_not_find_first_index_of_nonexistent_case_insensitive_name() {
    assertThat(index.firstIndexOf("bar")).isEqualTo(-1);
  }

  @Test
  public void should_find_first_index_of_case_sensitive_name() {
    assertThat(index.firstIndexOf("\"Foo\"")).isEqualTo(0);
    assertThat(index.firstIndexOf("\"foo\"")).isEqualTo(1);
    assertThat(index.firstIndexOf("\"fOO\"")).isEqualTo(2);
  }

  @Test
  public void should_not_find_index_of_nonexistent_case_sensitive_name() {
    assertThat(index.firstIndexOf("\"FOO\"")).isEqualTo(-1);
  }

  @Test
  public void should_find_all_indices_of_existing_identifier() {
    assertThat(index.allIndicesOf(Foo)).containsExactly(0, 3);
    assertThat(index.allIndicesOf(foo)).containsExactly(1, 4);
    assertThat(index.allIndicesOf(fOO)).containsExactly(2, 5);
  }

  @Test
  public void should_not_find_indices_of_nonexistent_identifier() {
    assertThat(index.allIndicesOf(CqlIdentifier.fromInternal("FOO"))).isEmpty();
  }

  @Test
  public void should_all_indices_of_case_insensitive_name() {
    assertThat(index.allIndicesOf("foo")).containsExactly(0, 1, 2, 3, 4, 5);
  }

  @Test
  public void should_not_find_indices_of_nonexistent_case_insensitive_name() {
    assertThat(index.allIndicesOf("bar")).isEmpty();
  }

  @Test
  public void should_find_all_indices_of_case_sensitive_name() {
    assertThat(index.allIndicesOf("\"Foo\"")).containsExactly(0, 3);
    assertThat(index.allIndicesOf("\"foo\"")).containsExactly(1, 4);
    assertThat(index.allIndicesOf("\"fOO\"")).containsExactly(2, 5);
  }

  @Test
  public void should_not_find_indices_of_nonexistent_case_sensitive_name() {
    assertThat(index.allIndicesOf("\"FOO\"")).isEmpty();
  }
}
