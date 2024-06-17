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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.RelationParser;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;

/** A table in the schema metadata. */
public interface TableMetadata extends RelationMetadata {

  boolean isCompactStorage();

  /** Whether this table is virtual */
  boolean isVirtual();

  @NonNull
  Map<CqlIdentifier, IndexMetadata> getIndexes();

  @NonNull
  default Optional<IndexMetadata> getIndex(@NonNull CqlIdentifier indexId) {
    return Optional.ofNullable(getIndexes().get(indexId));
  }

  /** Shortcut for {@link #getIndex(CqlIdentifier) getIndex(CqlIdentifier.fromCql(indexName))}. */
  @NonNull
  default Optional<IndexMetadata> getIndex(@NonNull String indexName) {
    return getIndex(CqlIdentifier.fromCql(indexName));
  }

  @NonNull
  @Override
  default String describe(boolean pretty) {
    ScriptBuilder builder = new ScriptBuilder(pretty);
    if (isVirtual()) {
      builder.append("/* VIRTUAL ");
    } else {
      builder.append("CREATE ");
    }

    builder
        .append("TABLE ")
        .append(getKeyspace())
        .append(".")
        .append(getName())
        .append(" (")
        .newLine()
        .increaseIndent();

    for (ColumnMetadata column : getColumns().values()) {
      builder.append(column.getName()).append(" ").append(column.getType().asCql(true, pretty));
      if (column.isStatic()) {
        builder.append(" static");
      }
      builder.append(",").newLine();
    }

    // PK
    builder.append("PRIMARY KEY (");
    if (getPartitionKey().size() == 1) { // PRIMARY KEY (k
      builder.append(getPartitionKey().get(0).getName());
    } else { // PRIMARY KEY ((k1, k2)
      builder.append("(");
      boolean first = true;
      for (ColumnMetadata pkColumn : getPartitionKey()) {
        if (first) {
          first = false;
        } else {
          builder.append(", ");
        }
        builder.append(pkColumn.getName());
      }
      builder.append(")");
    }
    // PRIMARY KEY (<pk portion>, cc1, cc2, cc3)
    for (ColumnMetadata clusteringColumn : getClusteringColumns().keySet()) {
      builder.append(", ").append(clusteringColumn.getName());
    }
    builder.append(")");

    builder.newLine().decreaseIndent().append(")");

    builder.increaseIndent();
    if (isCompactStorage()) {
      builder.andWith().append("COMPACT STORAGE");
    }
    if (getClusteringColumns().containsValue(ClusteringOrder.DESC)) {
      builder.andWith().append("CLUSTERING ORDER BY (");
      boolean first = true;
      for (Map.Entry<ColumnMetadata, ClusteringOrder> entry : getClusteringColumns().entrySet()) {
        if (first) {
          first = false;
        } else {
          builder.append(", ");
        }
        builder.append(entry.getKey().getName()).append(" ").append(entry.getValue().name());
      }
      builder.append(")");
    }
    Map<CqlIdentifier, Object> options = getOptions();
    RelationParser.appendOptions(options, builder);
    builder.append(";");
    if (isVirtual()) {
      builder.append(" */");
    }
    return builder.build();
  }

  /**
   * {@inheritDoc}
   *
   * <p>This describes the table and all of its indices. Contrary to previous driver versions, views
   * are <b>not</b> included.
   */
  @NonNull
  @Override
  default String describeWithChildren(boolean pretty) {
    String createTable = describe(pretty);
    ScriptBuilder builder = new ScriptBuilder(pretty).append(createTable);
    for (IndexMetadata indexMetadata : getIndexes().values()) {
      builder.forceNewLine(2).append(indexMetadata.describeWithChildren(pretty));
    }
    return builder.build();
  }
}
