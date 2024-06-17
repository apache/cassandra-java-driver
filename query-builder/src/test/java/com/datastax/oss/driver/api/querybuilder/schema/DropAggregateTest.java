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
package com.datastax.oss.driver.api.querybuilder.schema;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropAggregate;

import org.junit.Test;

public class DropAggregateTest {
  @Test
  public void should_generate_drop_aggregate() {
    assertThat(dropAggregate("bar")).hasCql("DROP AGGREGATE bar");
  }

  @Test
  public void should_generate_drop_aggregate_with_keyspace() {
    assertThat(dropAggregate("foo", "bar")).hasCql("DROP AGGREGATE foo.bar");
  }

  @Test
  public void should_generate_drop_aggregate_if_exists() {
    assertThat(dropAggregate("bar").ifExists()).hasCql("DROP AGGREGATE IF EXISTS bar");
  }
}
