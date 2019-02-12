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
package com.datastax.oss.driver.api.querybuilder.delete;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;

import com.datastax.oss.driver.api.querybuilder.relation.RelationTest;
import com.datastax.oss.driver.api.querybuilder.select.SelectFluentRelationTest;
import org.junit.Test;

/**
 * Mostly covered by other tests already.
 *
 * @see SelectFluentRelationTest
 * @see RelationTest
 */
public class DeleteFluentRelationTest {

  @Test
  public void should_generate_delete_with_column_relation() {
    assertThat(deleteFrom("foo").whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("DELETE FROM foo WHERE k=?");
  }
}
