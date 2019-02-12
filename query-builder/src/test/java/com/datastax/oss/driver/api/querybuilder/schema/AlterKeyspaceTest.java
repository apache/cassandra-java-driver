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
package com.datastax.oss.driver.api.querybuilder.schema;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.alterKeyspace;

import org.junit.Test;

public class AlterKeyspaceTest {

  @Test
  public void should_not_throw_on_toString_for_AlterKeyspaceStart() {
    assertThat(alterKeyspace("foo").toString()).isEqualTo("ALTER KEYSPACE foo");
  }

  @Test
  public void should_generate_alter_keyspace_with_replication() {
    assertThat(alterKeyspace("foo").withSimpleStrategy(3))
        .hasCql(
            "ALTER KEYSPACE foo WITH replication={'class':'SimpleStrategy','replication_factor':3}");
  }

  @Test
  public void should_generate_alter_keyspace_with_durable_writes_and_options() {
    assertThat(alterKeyspace("foo").withDurableWrites(true).withOption("hello", "world"))
        .hasCql("ALTER KEYSPACE foo WITH durable_writes=true AND hello='world'");
  }
}
