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
package com.datastax.oss.driver.api.core;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CqlIdentifierTest {
  @Test
  public void should_build_from_internal() {
    assertThat(CqlIdentifier.fromInternal("foo").asInternal()).isEqualTo("foo");
    assertThat(CqlIdentifier.fromInternal("Foo").asInternal()).isEqualTo("Foo");
    assertThat(CqlIdentifier.fromInternal("foo bar").asInternal()).isEqualTo("foo bar");
    assertThat(CqlIdentifier.fromInternal("foo\"bar").asInternal()).isEqualTo("foo\"bar");
    assertThat(CqlIdentifier.fromInternal("create").asInternal()).isEqualTo("create");
  }

  @Test
  public void should_build_from_valid_cql() {
    assertThat(CqlIdentifier.fromCql("foo").asInternal()).isEqualTo("foo");
    assertThat(CqlIdentifier.fromCql("Foo").asInternal()).isEqualTo("foo");
    assertThat(CqlIdentifier.fromCql("\"Foo\"").asInternal()).isEqualTo("Foo");
    assertThat(CqlIdentifier.fromCql("\"foo bar\"").asInternal()).isEqualTo("foo bar");
    assertThat(CqlIdentifier.fromCql("\"foo\"\"bar\"").asInternal()).isEqualTo("foo\"bar");
    assertThat(CqlIdentifier.fromCql("\"create\"").asInternal()).isEqualTo("create");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_fail_to_build_from_valid_cql_if_special_characters() {
    CqlIdentifier.fromCql("foo bar");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_fail_to_build_from_valid_cql_if_reserved_keyword() {
    CqlIdentifier.fromCql("Create");
  }

  @Test
  public void should_format_as_cql() {
    assertThat(CqlIdentifier.fromInternal("foo").asCql()).isEqualTo("\"foo\"");
    assertThat(CqlIdentifier.fromInternal("Foo").asCql()).isEqualTo("\"Foo\"");
    assertThat(CqlIdentifier.fromInternal("foo bar").asCql()).isEqualTo("\"foo bar\"");
    assertThat(CqlIdentifier.fromInternal("foo\"bar").asCql()).isEqualTo("\"foo\"\"bar\"");
    assertThat(CqlIdentifier.fromInternal("create").asCql()).isEqualTo("\"create\"");
  }

  @Test
  public void should_format_as_pretty_cql() {
    assertThat(CqlIdentifier.fromInternal("foo").asPrettyCql()).isEqualTo("foo");
    assertThat(CqlIdentifier.fromInternal("Foo").asPrettyCql()).isEqualTo("\"Foo\"");
    assertThat(CqlIdentifier.fromInternal("foo bar").asPrettyCql()).isEqualTo("\"foo bar\"");
    assertThat(CqlIdentifier.fromInternal("foo\"bar").asPrettyCql()).isEqualTo("\"foo\"\"bar\"");
    assertThat(CqlIdentifier.fromInternal("create").asPrettyCql()).isEqualTo("\"create\"");
  }
}
