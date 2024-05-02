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
package com.datastax.dse.driver.api.core.metadata.schema;

import com.datastax.dse.driver.internal.core.metadata.schema.ScriptHelper;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.RelationParser;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;

/**
 * Specialized table metadata, that handles the graph-specific properties introduced in DSE 6.8.
 *
 * <p>This type only exists to avoid breaking binary compatibility. When the driver is connected to
 * a DSE cluster, all the {@link TableMetadata} instances it returns can be safely downcast to this
 * interface.
 */
public interface DseGraphTableMetadata extends DseTableMetadata {
  /**
   * The vertex metadata if this table represents a vertex in graph, otherwise empty.
   *
   * <p>This is mutually exclusive with {@link #getEdge()}.
   */
  @NonNull
  Optional<DseVertexMetadata> getVertex();

  /**
   * The edge metadata if this table represents an edge in graph, otherwise empty.
   *
   * <p>This is mutually exclusive with {@link #getVertex()}.
   */
  @NonNull
  Optional<DseEdgeMetadata> getEdge();

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
      for (Map.Entry<? extends ColumnMetadata, ClusteringOrder> entry :
          getClusteringColumns().entrySet()) {
        if (first) {
          first = false;
        } else {
          builder.append(", ");
        }
        builder.append(entry.getKey().getName()).append(" ").append(entry.getValue().name());
      }
      builder.append(")");
    }
    getVertex()
        .ifPresent(
            vertex -> {
              builder.andWith().append("VERTEX LABEL").append(" ").append(vertex.getLabelName());
            });
    getEdge()
        .ifPresent(
            edge -> {
              builder.andWith().append("EDGE LABEL").append(" ").append(edge.getLabelName());
              ScriptHelper.appendEdgeSide(
                  builder,
                  edge.getFromTable(),
                  edge.getFromLabel(),
                  edge.getFromPartitionKeyColumns(),
                  edge.getFromClusteringColumns(),
                  "FROM");
              ScriptHelper.appendEdgeSide(
                  builder,
                  edge.getToTable(),
                  edge.getToLabel(),
                  edge.getToPartitionKeyColumns(),
                  edge.getToClusteringColumns(),
                  "TO");
            });
    Map<CqlIdentifier, Object> options = getOptions();
    RelationParser.appendOptions(options, builder);
    builder.append(";");
    if (isVirtual()) {
      builder.append(" */");
    }
    return builder.build();
  }
}
