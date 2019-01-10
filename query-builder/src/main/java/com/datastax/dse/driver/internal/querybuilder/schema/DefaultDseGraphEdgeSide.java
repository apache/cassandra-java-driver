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
package com.datastax.dse.driver.internal.querybuilder.schema;

import com.datastax.dse.driver.api.querybuilder.schema.DseGraphEdgeSide;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

public class DefaultDseGraphEdgeSide implements DseGraphEdgeSide {

  private final CqlIdentifier tableId;
  private final ImmutableList<CqlIdentifier> partitionKeyColumns;
  private final ImmutableList<CqlIdentifier> clusteringColumns;

  public DefaultDseGraphEdgeSide(CqlIdentifier tableId) {
    this(tableId, ImmutableList.of(), ImmutableList.of());
  }

  private DefaultDseGraphEdgeSide(
      CqlIdentifier tableId,
      ImmutableList<CqlIdentifier> partitionKeyColumns,
      ImmutableList<CqlIdentifier> clusteringColumns) {
    this.tableId = tableId;
    this.partitionKeyColumns = partitionKeyColumns;
    this.clusteringColumns = clusteringColumns;
  }

  @NonNull
  @Override
  public DseGraphEdgeSide withPartitionKey(@NonNull CqlIdentifier columnId) {
    return new DefaultDseGraphEdgeSide(
        tableId, ImmutableCollections.append(partitionKeyColumns, columnId), clusteringColumns);
  }

  @NonNull
  @Override
  public DseGraphEdgeSide withClusteringColumn(@NonNull CqlIdentifier columnId) {
    return new DefaultDseGraphEdgeSide(
        tableId, partitionKeyColumns, ImmutableCollections.append(clusteringColumns, columnId));
  }

  @NonNull
  @Override
  public CqlIdentifier getTableId() {
    return tableId;
  }

  @NonNull
  @Override
  public List<CqlIdentifier> getPartitionKeyColumns() {
    return partitionKeyColumns;
  }

  @NonNull
  @Override
  public List<CqlIdentifier> getClusteringColumns() {
    return clusteringColumns;
  }
}
