/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.data;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IdentifierIndexTest {
  private static final CqlIdentifier Foo = CqlIdentifier.fromInternal("Foo");
  private static final CqlIdentifier foo = CqlIdentifier.fromInternal("foo");
  private static final CqlIdentifier fOO = CqlIdentifier.fromInternal("fOO");
  private IdentifierIndex index = new IdentifierIndex(ImmutableList.of(Foo, foo, fOO));

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
}
