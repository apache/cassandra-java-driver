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
package com.datastax.oss.driver.api.querybuilder.delete;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import org.junit.Test;

public class DeleteFluentConditionTest {

  @Test
  public void should_generate_simple_column_condition() {
    assertThat(
            deleteFrom("foo")
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .ifColumn("v")
                .isEqualTo(literal(1)))
        .hasCql("DELETE FROM foo WHERE k=? IF v=1");
    assertThat(
            deleteFrom("foo")
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .ifColumn("v1")
                .isEqualTo(literal(1))
                .ifColumn("v2")
                .isEqualTo(literal(2)))
        .hasCql("DELETE FROM foo WHERE k=? IF v1=1 AND v2=2");
  }

  @Test
  public void should_generate_field_condition() {
    assertThat(
            deleteFrom("foo")
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .ifField("v", "f")
                .isEqualTo(literal(1)))
        .hasCql("DELETE FROM foo WHERE k=? IF v.f=1");
  }

  @Test
  public void should_generate_element_condition() {
    assertThat(
            deleteFrom("foo")
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .ifElement("v", literal(1))
                .isEqualTo(literal(1)))
        .hasCql("DELETE FROM foo WHERE k=? IF v[1]=1");
  }

  @Test
  public void should_generate_if_exists_condition() {
    assertThat(deleteFrom("foo").whereColumn("k").isEqualTo(bindMarker()).ifExists())
        .hasCql("DELETE FROM foo WHERE k=? IF EXISTS");
  }

  @Test
  public void should_cancel_if_exists_if_other_condition_added() {
    assertThat(
            deleteFrom("foo")
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .ifExists()
                .ifColumn("v")
                .isEqualTo(literal(1)))
        .hasCql("DELETE FROM foo WHERE k=? IF v=1");
  }

  @Test
  public void should_cancel_other_conditions_if_if_exists_added() {
    assertThat(
            deleteFrom("foo")
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .ifColumn("v1")
                .isEqualTo(literal(1))
                .ifColumn("v2")
                .isEqualTo(literal(2))
                .ifExists())
        .hasCql("DELETE FROM foo WHERE k=? IF EXISTS");
  }
}
