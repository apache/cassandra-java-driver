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
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public interface DseTableGraphOptions<NextT> {

  /** Adds a vertex label to this table. */
  @NonNull
  NextT withVertexLabel(@Nullable CqlIdentifier vertexLabelId);

  /**
   * Shortcut for {@link #withVertexLabel(CqlIdentifier)
   * withVertexLabel(CqlIdentifier.fromCql(vertexLabel))}.
   */
  @NonNull
  default NextT withVertexLabel(@NonNull String vertexLabelName) {
    return withVertexLabel(CqlIdentifier.fromCql(vertexLabelName));
  }

  /**
   * Adds an anonymous vertex label to this table.
   *
   * <p>This is a shortcut for {@link #withVertexLabel(CqlIdentifier) withVertexLabel(null)}.
   */
  @NonNull
  default NextT withVertexLabel() {
    return withVertexLabel((CqlIdentifier) null);
  }

  /**
   * Adds an edge label to this table.
   *
   * <p>Use {@link DseGraphEdgeSide#table(CqlIdentifier)} to build the definitions of both sides,
   * for example:
   *
   * <pre>{@code
   * withEdgeLabel("contrib",
   *               table("person")
   *                 .withPartitionKey("contributor"),
   *               table("soft")
   *                 .withPartitionKey("company_name"),
   *                 .withPartitionKey("software_name"),
   *                 .withClusteringColumn("software_version"))
   * }</pre>
   */
  @NonNull
  NextT withEdgeLabel(
      @Nullable CqlIdentifier edgeLabelId,
      @NonNull DseGraphEdgeSide from,
      @NonNull DseGraphEdgeSide to);

  /**
   * Shortcut for {@link #withEdgeLabel(CqlIdentifier, DseGraphEdgeSide, DseGraphEdgeSide)
   * withEdgeLabel(CqlIdentifier.fromCql(edgeLabelName), from, to)}.
   */
  @NonNull
  default NextT withEdgeLabel(
      @NonNull String edgeLabelName, @NonNull DseGraphEdgeSide from, @NonNull DseGraphEdgeSide to) {
    return withEdgeLabel(CqlIdentifier.fromCql(edgeLabelName), from, to);
  }

  /**
   * Adds an anonymous edge label to this table.
   *
   * <p>This is a shortcut for {@link #withEdgeLabel(CqlIdentifier, DseGraphEdgeSide,
   * DseGraphEdgeSide) withEdgeLabel(null, from, to)}.
   */
  @NonNull
  default NextT withEdgeLabel(@NonNull DseGraphEdgeSide from, @NonNull DseGraphEdgeSide to) {
    return withEdgeLabel((CqlIdentifier) null, from, to);
  }
}
