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
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import org.junit.Test;

public class SelectLimitTest {

  @Test
  public void should_generate_limit() {
    assertThat(selectFrom("foo").all().limit(1)).hasCql("SELECT * FROM foo LIMIT 1");
    assertThat(selectFrom("foo").all().limit(bindMarker("l"))).hasCql("SELECT * FROM foo LIMIT :l");
  }

  @Test
  public void should_use_last_limit_if_called_multiple_times() {
    assertThat(selectFrom("foo").all().limit(1).limit(2)).hasCql("SELECT * FROM foo LIMIT 2");
  }

  @Test
  public void should_generate_per_partition_limit() {
    assertThat(selectFrom("foo").all().perPartitionLimit(1))
        .hasCql("SELECT * FROM foo PER PARTITION LIMIT 1");
    assertThat(selectFrom("foo").all().perPartitionLimit(bindMarker("l")))
        .hasCql("SELECT * FROM foo PER PARTITION LIMIT :l");
  }

  @Test
  public void should_use_last_per_partition_limit_if_called_multiple_times() {
    assertThat(selectFrom("foo").all().perPartitionLimit(1).perPartitionLimit(2))
        .hasCql("SELECT * FROM foo PER PARTITION LIMIT 2");
  }
}
