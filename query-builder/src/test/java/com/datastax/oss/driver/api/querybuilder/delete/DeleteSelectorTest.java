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
package com.datastax.oss.driver.api.querybuilder.delete;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import com.datastax.oss.driver.api.core.data.CqlVector;
import org.junit.Test;

public class DeleteSelectorTest {

  @Test
  public void should_generate_column_deletion() {
    assertThat(deleteFrom("foo").column("v").whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("DELETE v FROM foo WHERE k=?");
    assertThat(deleteFrom("ks", "foo").column("v").whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("DELETE v FROM ks.foo WHERE k=?");
  }

  @Test
  public void should_generate_vector_deletion() {
    assertThat(
            deleteFrom("foo")
                .column("v")
                .whereColumn("k")
                .isEqualTo(literal(CqlVector.newInstance(0.1, 0.2))))
        .hasCql("DELETE v FROM foo WHERE k=[0.1, 0.2]");
  }

  @Test
  public void should_generate_field_deletion() {
    assertThat(
            deleteFrom("foo").field("address", "street").whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("DELETE address.street FROM foo WHERE k=?");
  }

  @Test
  public void should_generate_element_deletion() {
    assertThat(deleteFrom("foo").element("m", literal(1)).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("DELETE m[1] FROM foo WHERE k=?");
  }
}
