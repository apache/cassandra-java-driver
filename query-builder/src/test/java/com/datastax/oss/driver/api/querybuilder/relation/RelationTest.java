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
package com.datastax.oss.driver.api.querybuilder.relation;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.raw;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;

import org.assertj.core.util.Lists;
import org.junit.Test;

public class RelationTest {

  @Test
  public void should_generate_comparison_relation() {
    assertThat(selectFrom("foo").all().where(Relation.column("k").isEqualTo(bindMarker())))
        .hasCql("SELECT * FROM foo WHERE k=?");
    assertThat(selectFrom("foo").all().where(Relation.column("k").isEqualTo(bindMarker("value"))))
        .hasCql("SELECT * FROM foo WHERE k=:value");
  }

  @Test
  public void should_generate_is_not_null_relation() {
    assertThat(selectFrom("foo").all().where(Relation.column("k").isNotNull()))
        .hasCql("SELECT * FROM foo WHERE k IS NOT NULL");
  }

  @Test
  public void should_generate_contains_relation() {
    assertThat(selectFrom("foo").all().where(Relation.column("k").contains(literal(1))))
        .hasCql("SELECT * FROM foo WHERE k CONTAINS 1");
  }

  @Test
  public void should_generate_contains_key_relation() {
    assertThat(selectFrom("foo").all().where(Relation.column("k").containsKey(literal(1))))
        .hasCql("SELECT * FROM foo WHERE k CONTAINS KEY 1");
  }

  @Test
  public void should_generate_not_contains_relation() {
    assertThat(selectFrom("foo").all().where(Relation.column("k").notContains(literal(1))))
        .hasCql("SELECT * FROM foo WHERE k NOT CONTAINS 1");
  }

  @Test
  public void should_generate_not_contains_key_relation() {
    assertThat(selectFrom("foo").all().where(Relation.column("k").notContainsKey(literal(1))))
        .hasCql("SELECT * FROM foo WHERE k NOT CONTAINS KEY 1");
  }

  @Test
  public void should_generate_in_relation_bind_markers() {
    assertThat(selectFrom("foo").all().where(Relation.column("k").in(bindMarker())))
        .hasCql("SELECT * FROM foo WHERE k IN ?");
    assertThat(selectFrom("foo").all().where(Relation.column("k").in(bindMarker(), bindMarker())))
        .hasCql("SELECT * FROM foo WHERE k IN (?,?)");
  }

  @Test
  public void should_generate_in_relation_terms() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(
                    Relation.column("k")
                        .in(Lists.newArrayList(literal(1), literal(2), literal(3)))))
        .hasCql("SELECT * FROM foo WHERE k IN (1,2,3)");
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").in(literal(1), literal(2), literal(3))))
        .hasCql("SELECT * FROM foo WHERE k IN (1,2,3)");
  }

  @Test
  public void should_generate_not_in_relation_bind_markers() {
    assertThat(selectFrom("foo").all().where(Relation.column("k").notIn(bindMarker())))
        .hasCql("SELECT * FROM foo WHERE k NOT IN ?");
    assertThat(
            selectFrom("foo").all().where(Relation.column("k").notIn(bindMarker(), bindMarker())))
        .hasCql("SELECT * FROM foo WHERE k NOT IN (?,?)");
  }

  @Test
  public void should_generate_not_in_relation_terms() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(
                    Relation.column("k")
                        .notIn(Lists.newArrayList(literal(1), literal(2), literal(3)))))
        .hasCql("SELECT * FROM foo WHERE k NOT IN (1,2,3)");
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").notIn(literal(1), literal(2), literal(3))))
        .hasCql("SELECT * FROM foo WHERE k NOT IN (1,2,3)");
  }

  @Test
  public void should_generate_token_relation() {
    assertThat(selectFrom("foo").all().where(Relation.token("k1", "k2").isEqualTo(bindMarker("t"))))
        .hasCql("SELECT * FROM foo WHERE token(k1,k2)=:t");
  }

  @Test
  public void should_generate_column_component_relation() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(
                    Relation.column("id").isEqualTo(bindMarker()),
                    Relation.mapValue("user", raw("'name'")).isEqualTo(bindMarker())))
        .hasCql("SELECT * FROM foo WHERE id=? AND user['name']=?");
  }

  @Test
  public void should_generate_tuple_relation() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(bindMarker()))
                .where(Relation.columns("c1", "c2", "c3").in(bindMarker())))
        .hasCql("SELECT * FROM foo WHERE k=? AND (c1,c2,c3) IN ?");
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(bindMarker()))
                .where(Relation.columns("c1", "c2", "c3").in(bindMarker(), bindMarker())))
        .hasCql("SELECT * FROM foo WHERE k=? AND (c1,c2,c3) IN (?,?)");
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(bindMarker()))
                .where(Relation.columns("c1", "c2", "c3").in(bindMarker(), raw("(4,5,6)"))))
        .hasCql("SELECT * FROM foo WHERE k=? AND (c1,c2,c3) IN (?,(4,5,6))");
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(bindMarker()))
                .where(
                    Relation.columns("c1", "c2", "c3")
                        .in(
                            tuple(bindMarker(), bindMarker(), bindMarker()),
                            tuple(bindMarker(), bindMarker(), bindMarker()))))
        .hasCql("SELECT * FROM foo WHERE k=? AND (c1,c2,c3) IN ((?,?,?),(?,?,?))");

    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(bindMarker()))
                .where(Relation.columns("c1", "c2", "c3").isEqualTo(bindMarker())))
        .hasCql("SELECT * FROM foo WHERE k=? AND (c1,c2,c3)=?");
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(bindMarker()))
                .where(
                    Relation.columns("c1", "c2", "c3")
                        .isLessThan(tuple(bindMarker(), bindMarker(), bindMarker()))))
        .hasCql("SELECT * FROM foo WHERE k=? AND (c1,c2,c3)<(?,?,?)");
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(bindMarker()))
                .where(Relation.columns("c1", "c2", "c3").isGreaterThanOrEqualTo(raw("(1,2,3)"))))
        .hasCql("SELECT * FROM foo WHERE k=? AND (c1,c2,c3)>=(1,2,3)");
  }

  @Test
  public void should_generate_custom_index_relation() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(bindMarker()))
                .where(Relation.customIndex("my_index", raw("'custom expression'"))))
        .hasCql("SELECT * FROM foo WHERE k=? AND expr(my_index,'custom expression')");
  }

  @Test
  public void should_generate_raw_relation() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(bindMarker()))
                .where(raw("c = 'test'")))
        .hasCql("SELECT * FROM foo WHERE k=? AND c = 'test'");
  }
}
