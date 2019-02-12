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
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropMaterializedView;

import org.junit.Test;

public class DropMaterializedViewTest {

  @Test
  public void should_generate_drop_view() {
    assertThat(dropMaterializedView("bar")).hasCql("DROP MATERIALIZED VIEW bar");
  }

  @Test
  public void should_generate_drop_view_with_keyspace() {
    assertThat(dropMaterializedView("foo", "bar")).hasCql("DROP MATERIALIZED VIEW foo.bar");
  }

  @Test
  public void should_generate_drop_view_if_exists() {
    assertThat(dropMaterializedView("bar").ifExists())
        .hasCql("DROP MATERIALIZED VIEW IF EXISTS bar");
  }
}
