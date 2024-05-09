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

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import org.junit.Test;

public class SelectGroupByTest {

  @Test
  public void should_generate_group_by_clauses() {
    assertThat(
            selectFrom("foo")
                .all()
                .whereColumn("k")
                .isEqualTo(literal(1))
                .groupBy("foo")
                .groupBy("bar"))
        .hasCql("SELECT * FROM foo WHERE k=1 GROUP BY foo,bar");

    assertThat(
            selectFrom("foo")
                .all()
                .whereColumn("k")
                .isEqualTo(literal(1))
                .groupByColumns("foo", "bar"))
        .hasCql("SELECT * FROM foo WHERE k=1 GROUP BY foo,bar");

    assertThat(
            selectFrom("foo")
                .all()
                .whereColumn("k")
                .isEqualTo(literal(1))
                .groupBy(
                    Selector.function("ks", "f", Selector.column("foo")), Selector.column("bar")))
        .hasCql("SELECT * FROM foo WHERE k=1 GROUP BY ks.f(foo),bar");
  }
}
