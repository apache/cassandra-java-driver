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
package com.datastax.oss.driver.api.querybuilder.condition;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import org.junit.Test;

public class ConditionTest {

  @Test
  public void should_generate_simple_column_condition() {
    deleteFrom("foo").whereColumn("k").isEqualTo(bindMarker()).ifColumn("v").isEqualTo(literal(1));
    assertThat(
            deleteFrom("foo")
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .if_(Condition.column("v").isEqualTo(literal(1))))
        .hasCql("DELETE FROM foo WHERE k=? IF v=1");
    assertThat(
            deleteFrom("foo")
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .if_(
                    Condition.column("v1").isEqualTo(literal(1)),
                    Condition.column("v2").isEqualTo(literal(2))))
        .hasCql("DELETE FROM foo WHERE k=? IF v1=1 AND v2=2");
  }

  @Test
  public void should_generate_field_condition() {
    assertThat(
            deleteFrom("foo")
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .if_(Condition.field("v", "f").isEqualTo(literal(1))))
        .hasCql("DELETE FROM foo WHERE k=? IF v.f=1");
  }

  @Test
  public void should_generate_element_condition() {
    assertThat(
            deleteFrom("foo")
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .if_(Condition.element("v", literal(1)).isEqualTo(literal(1))))
        .hasCql("DELETE FROM foo WHERE k=? IF v[1]=1");
  }
}
