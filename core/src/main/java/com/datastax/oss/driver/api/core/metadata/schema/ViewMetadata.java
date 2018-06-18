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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.RelationParser;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;

/** A materialized view in the schema metadata. */
public interface ViewMetadata extends RelationMetadata {

  /** The table that this view is based on. */
  @NonNull
  CqlIdentifier getBaseTable();

  /**
   * Whether this view does a {@code SELECT *} on its base table (this only affects the output of
   * {@link #describe(boolean)}).
   */
  boolean includesAllColumns();

  @NonNull
  Optional<String> getWhereClause();

  @NonNull
  @Override
  default String describe(boolean pretty) {
    ScriptBuilder builder =
        new ScriptBuilder(pretty)
            .append("CREATE MATERIALIZED VIEW ")
            .append(getKeyspace())
            .append(".")
            .append(getName())
            .append(" AS")
            .newLine();

    builder.append("SELECT");
    if (includesAllColumns()) {
      builder.append(" * ");
    } else {
      builder.newLine().increaseIndent();
      boolean first = true;
      for (ColumnMetadata column : getColumns().values()) {
        if (first) {
          first = false;
        } else {
          builder.append(",").newLine();
        }
        builder.append(column.getName());
      }
      builder.newLine().decreaseIndent();
    }

    builder.append("FROM ").append(getKeyspace()).append(".").append(getBaseTable());

    Optional<String> whereClause = getWhereClause();
    if (whereClause.isPresent() && !whereClause.get().isEmpty()) {
      builder.newLine().append("WHERE ").append(whereClause.get());
    }

    builder.newLine().append("PRIMARY KEY (");
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
    builder.append(")").increaseIndent();

    RelationParser.appendOptions(getOptions(), builder);
    return builder.append(";").build();
  }

  @NonNull
  @Override
  default String describeWithChildren(boolean pretty) {
    return describe(pretty); // A view has no children
  }
}
