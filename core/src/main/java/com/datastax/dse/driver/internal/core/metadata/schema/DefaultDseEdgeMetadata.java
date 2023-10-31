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
package com.datastax.dse.driver.internal.core.metadata.schema;

import com.datastax.dse.driver.api.core.metadata.schema.DseEdgeMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

public class DefaultDseEdgeMetadata implements DseEdgeMetadata, Serializable {

  private static final long serialVersionUID = 1;

  @Nonnull private final CqlIdentifier labelName;

  @Nonnull private final CqlIdentifier fromTable;
  @Nonnull private final CqlIdentifier fromLabel;
  @Nonnull private final List<CqlIdentifier> fromPartitionKeyColumns;
  @Nonnull private final List<CqlIdentifier> fromClusteringColumns;

  @Nonnull private final CqlIdentifier toTable;
  @Nonnull private final CqlIdentifier toLabel;
  @Nonnull private final List<CqlIdentifier> toPartitionKeyColumns;
  @Nonnull private final List<CqlIdentifier> toClusteringColumns;

  public DefaultDseEdgeMetadata(
      @Nonnull CqlIdentifier labelName,
      @Nonnull CqlIdentifier fromTable,
      @Nonnull CqlIdentifier fromLabel,
      @Nonnull List<CqlIdentifier> fromPartitionKeyColumns,
      @Nonnull List<CqlIdentifier> fromClusteringColumns,
      @Nonnull CqlIdentifier toTable,
      @Nonnull CqlIdentifier toLabel,
      @Nonnull List<CqlIdentifier> toPartitionKeyColumns,
      @Nonnull List<CqlIdentifier> toClusteringColumns) {
    this.labelName = Preconditions.checkNotNull(labelName);
    this.fromTable = Preconditions.checkNotNull(fromTable);
    this.fromLabel = Preconditions.checkNotNull(fromLabel);
    this.fromPartitionKeyColumns = Preconditions.checkNotNull(fromPartitionKeyColumns);
    this.fromClusteringColumns = Preconditions.checkNotNull(fromClusteringColumns);
    this.toTable = Preconditions.checkNotNull(toTable);
    this.toLabel = Preconditions.checkNotNull(toLabel);
    this.toPartitionKeyColumns = Preconditions.checkNotNull(toPartitionKeyColumns);
    this.toClusteringColumns = Preconditions.checkNotNull(toClusteringColumns);
  }

  @Nonnull
  @Override
  public CqlIdentifier getLabelName() {
    return labelName;
  }

  @Nonnull
  @Override
  public CqlIdentifier getFromTable() {
    return fromTable;
  }

  @Nonnull
  @Override
  public CqlIdentifier getFromLabel() {
    return fromLabel;
  }

  @Nonnull
  @Override
  public List<CqlIdentifier> getFromPartitionKeyColumns() {
    return fromPartitionKeyColumns;
  }

  @Nonnull
  @Override
  public List<CqlIdentifier> getFromClusteringColumns() {
    return fromClusteringColumns;
  }

  @Nonnull
  @Override
  public CqlIdentifier getToTable() {
    return toTable;
  }

  @Nonnull
  @Override
  public CqlIdentifier getToLabel() {
    return toLabel;
  }

  @Nonnull
  @Override
  public List<CqlIdentifier> getToPartitionKeyColumns() {
    return toPartitionKeyColumns;
  }

  @Nonnull
  @Override
  public List<CqlIdentifier> getToClusteringColumns() {
    return toClusteringColumns;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DseEdgeMetadata) {
      DseEdgeMetadata that = (DseEdgeMetadata) other;
      return Objects.equals(this.labelName, that.getLabelName())
          && Objects.equals(this.fromTable, that.getFromTable())
          && Objects.equals(this.fromLabel, that.getFromLabel())
          && Objects.equals(this.fromPartitionKeyColumns, that.getFromPartitionKeyColumns())
          && Objects.equals(this.fromClusteringColumns, that.getFromClusteringColumns())
          && Objects.equals(this.toTable, that.getToTable())
          && Objects.equals(this.toLabel, that.getToLabel())
          && Objects.equals(this.toPartitionKeyColumns, that.getToPartitionKeyColumns())
          && Objects.equals(this.toClusteringColumns, that.getToClusteringColumns());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        labelName,
        fromTable,
        fromLabel,
        fromPartitionKeyColumns,
        fromClusteringColumns,
        toTable,
        toLabel,
        toPartitionKeyColumns,
        toClusteringColumns);
  }
}
