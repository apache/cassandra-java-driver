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
package com.datastax.oss.driver.internal.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;

public class ConversionsTest {
  @Test
  public void should_find_pk_indices_if_all_bound() {
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("pk"))).containsExactly(0);
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("pk", "c")))
        .containsExactly(0);
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("c", "pk")))
        .containsExactly(1);
    assertThat(
            Conversions.findIndices(
                partitionKey("pk1", "pk2", "pk3"),
                variables("c1", "pk2", "pk3", "c2", "pk1", "c3")))
        .containsExactly(4, 1, 2);
  }

  @Test
  public void should_use_first_pk_index_if_bound_multiple_times() {
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("pk", "pk")))
        .containsExactly(0);
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("pk", "c1", "pk", "c2")))
        .containsExactly(0);
    assertThat(
            Conversions.findIndices(
                partitionKey("pk1", "pk2", "pk3"),
                variables("c1", "pk2", "pk3", "c2", "pk1", "c3", "pk1", "pk2")))
        .containsExactly(4, 1, 2);
  }

  @Test
  public void should_return_empty_pk_indices_if_at_least_one_component_not_bound() {
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("c1", "c2"))).isEmpty();
    assertThat(
            Conversions.findIndices(
                partitionKey("pk1", "pk2", "pk3"), variables("c1", "pk2", "c2", "pk1", "c3")))
        .isEmpty();
  }

  private List<ColumnMetadata> partitionKey(String... columnNames) {
    ImmutableList.Builder<ColumnMetadata> columns =
        ImmutableList.builderWithExpectedSize(columnNames.length);
    for (String columnName : columnNames) {
      ColumnMetadata column = mock(ColumnMetadata.class);
      when(column.getName()).thenReturn(CqlIdentifier.fromInternal(columnName));
      columns.add(column);
    }
    return columns.build();
  }

  private ColumnDefinitions variables(String... columnNames) {
    ImmutableList.Builder<ColumnDefinition> columns =
        ImmutableList.builderWithExpectedSize(columnNames.length);
    for (String columnName : columnNames) {
      ColumnDefinition column = mock(ColumnDefinition.class);
      when(column.getName()).thenReturn(CqlIdentifier.fromInternal(columnName));
      columns.add(column);
    }
    return DefaultColumnDefinitions.valueOf(columns.build());
  }
}
