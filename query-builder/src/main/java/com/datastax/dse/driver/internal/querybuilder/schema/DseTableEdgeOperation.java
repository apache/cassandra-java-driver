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
package com.datastax.dse.driver.internal.querybuilder.schema;

import com.datastax.dse.driver.api.querybuilder.schema.DseGraphEdgeSide;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import java.util.List;

public class DseTableEdgeOperation {

  private final DseTableGraphOperationType type;
  private final CqlIdentifier label;
  private final DseGraphEdgeSide from;
  private final DseGraphEdgeSide to;

  public DseTableEdgeOperation(
      DseTableGraphOperationType type,
      CqlIdentifier label,
      DseGraphEdgeSide from,
      DseGraphEdgeSide to) {
    this.type = type;
    this.label = label;
    this.from = from;
    this.to = to;
  }

  public DseTableGraphOperationType getType() {
    return type;
  }

  public CqlIdentifier getLabel() {
    return label;
  }

  public DseGraphEdgeSide getFrom() {
    return from;
  }

  public DseGraphEdgeSide getTo() {
    return to;
  }

  public void append(StringBuilder builder) {
    builder.append("EDGE LABEL");
    if (label != null) {
      builder.append(' ').append(label.asCql(true));
    }
    if (type == DseTableGraphOperationType.WITH) {
      builder.append(" FROM ");
      append(from, builder);
      builder.append(" TO ");
      append(to, builder);
    }
  }

  private static void append(DseGraphEdgeSide side, StringBuilder builder) {
    builder.append(side.getTableId().asCql(true)).append('(');
    List<CqlIdentifier> pkColumns = side.getPartitionKeyColumns();
    if (pkColumns.size() == 1) {
      builder.append(pkColumns.get(0).asCql(true));
    } else {
      CqlHelper.appendIds(pkColumns, builder, "(", ",", ")");
    }
    CqlHelper.appendIds(side.getClusteringColumns(), builder, ",", ",", null);
    builder.append(')');
  }
}
