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
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.alterMaterializedView;

import org.junit.Test;

public class AlterMaterializedViewTest {

  @Test
  public void should_not_throw_on_toString_for_AlterMaterializedViewStart() {
    assertThat(alterMaterializedView("foo").toString()).isEqualTo("ALTER MATERIALIZED VIEW foo");
  }

  @Test
  public void should_generate_alter_view_with_options() {
    assertThat(
            alterMaterializedView("baz").withLZ4Compression().withDefaultTimeToLiveSeconds(86400))
        .hasCql(
            "ALTER MATERIALIZED VIEW baz WITH compression={'class':'LZ4Compressor'} AND default_time_to_live=86400");
  }

  @Test
  public void should_generate_alter_view_with_keyspace_options() {
    assertThat(alterMaterializedView("foo", "baz").withCDC(true))
        .hasCql("ALTER MATERIALIZED VIEW foo.baz WITH cdc=true");
  }
}
