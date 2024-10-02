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
package com.datastax.oss.driver.api.querybuilder.select;

import static com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder.ASC;
import static com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder.DESC;
import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.subCondition;

import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import org.junit.Test;

public class SelectOrderingTest {

  @Test
  public void should_generate_ordering_clauses() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(literal(1)))
                .orderBy("c1", ASC)
                .orderBy("c2", DESC))
        .hasCql("SELECT * FROM foo WHERE k=1 ORDER BY c1 ASC,c2 DESC");
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(literal(1)))
                .orderBy(ImmutableMap.of("c1", ASC, "c2", DESC)))
        .hasCql("SELECT * FROM foo WHERE k=1 ORDER BY c1 ASC,c2 DESC");
  }

  @Test
  public void should_support_nested_sub_conditions() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(literal(1)))
                .and()
                .where(
                    subCondition()
                        .where(Relation.column("l").isEqualTo(literal(2)))
                        .where(
                            subCondition()
                                .where(Relation.column("m").isEqualTo(literal(3)))
                                .or()
                                .where(Relation.column("x").isEqualTo(literal(4)))))
                .orderBy("c1", ASC))
        .hasCql("SELECT * FROM foo WHERE k=1 AND (l=2 AND (m=3 OR x=4)) ORDER BY c1 ASC");
  }

  @Test
  public void should_generate_criteria_alternative() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(literal(1)))
                .or()
                .where(Relation.column("l").isEqualTo(literal(2)))
                .orderBy("c1", ASC)
                .orderBy("c2", DESC))
        .hasCql("SELECT * FROM foo WHERE k=1 OR l=2 ORDER BY c1 ASC,c2 DESC");
  }

  @Test
  public void should_support_sub_condition() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(literal(1)))
                .and()
                .where(
                    subCondition()
                        .where(Relation.column("l").isEqualTo(literal(2)))
                        .or()
                        .where(Relation.column("m").isEqualTo(literal(3))))
                .orderBy("c1", ASC)
                .orderBy("c2", DESC))
        .hasCql("SELECT * FROM foo WHERE k=1 AND (l=2 OR m=3) ORDER BY c1 ASC,c2 DESC");
    assertThat(
            selectFrom("foo")
                .all()
                .where(
                    subCondition()
                        .where(Relation.column("l").isEqualTo(literal(2)))
                        .where(Relation.column("m").isEqualTo(literal(3))))
                .and()
                .where(Relation.column("k").isEqualTo(literal(1)))
                .orderBy("c1", ASC))
        .hasCql("SELECT * FROM foo WHERE (l=2 AND m=3) AND k=1 ORDER BY c1 ASC");
    assertThat(
            selectFrom("foo")
                .all()
                .where(
                    subCondition()
                        .where(Relation.column("l").isEqualTo(literal(2)))
                        .where(Relation.column("m").isEqualTo(literal(3)))
                        .where(Relation.column("x").isEqualTo(literal(4))))
                .orderBy("c1", ASC))
        .hasCql("SELECT * FROM foo WHERE (l=2 AND m=3 AND x=4) ORDER BY c1 ASC");
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(literal(1)))
                .where(
                    subCondition()
                        .where(Relation.column("l").isEqualTo(literal(2)))
                        .where(Relation.column("m").isEqualTo(literal(3)))
                        .or()
                        .where(Relation.column("x").isEqualTo(literal(4)))))
        .hasCql("SELECT * FROM foo WHERE k=1 AND (l=2 AND m=3 OR x=4)");
  }

  @Test
  public void should_use_conjunction_as_default_logical_operator() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(literal(1)))
                .where(
                    subCondition()
                        .where(Relation.column("l").isEqualTo(literal(2)))
                        .or()
                        .where(Relation.column("m").isEqualTo(literal(3))))
                .orderBy("c1", ASC)
                .orderBy("c2", DESC))
        .hasCql("SELECT * FROM foo WHERE k=1 AND (l=2 OR m=3) ORDER BY c1 ASC,c2 DESC");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_when_provided_names_resolve_to_the_same_id() {
    selectFrom("foo")
        .all()
        .where(Relation.column("k").isEqualTo(literal(1)))
        .orderBy(ImmutableMap.of("c1", ASC, "C1", DESC));
  }

  @Test
  public void should_replace_previous_ordering() {
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(literal(1)))
                .orderBy("c1", ASC)
                .orderBy("c2", DESC)
                .orderBy("c1", DESC))
        .hasCql("SELECT * FROM foo WHERE k=1 ORDER BY c2 DESC,c1 DESC");
    assertThat(
            selectFrom("foo")
                .all()
                .where(Relation.column("k").isEqualTo(literal(1)))
                .orderBy("c1", ASC)
                .orderBy("c2", DESC)
                .orderBy("c3", ASC)
                .orderBy(ImmutableMap.of("c1", DESC, "c2", ASC)))
        .hasCql("SELECT * FROM foo WHERE k=1 ORDER BY c3 ASC,c1 DESC,c2 ASC");
  }
}
