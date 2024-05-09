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
