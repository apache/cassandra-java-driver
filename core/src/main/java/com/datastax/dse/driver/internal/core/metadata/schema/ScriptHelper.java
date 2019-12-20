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
package com.datastax.dse.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import java.util.List;

public class ScriptHelper {

  public static void appendEdgeSide(
      ScriptBuilder builder,
      CqlIdentifier table,
      CqlIdentifier label,
      List<CqlIdentifier> partitionKeyColumns,
      List<CqlIdentifier> clusteringColumns,
      String keyword) {
    builder.append(" ").append(keyword).append(label).append("(");

    if (partitionKeyColumns.size() == 1) { // PRIMARY KEY (k
      builder.append(partitionKeyColumns.get(0));
    } else { // PRIMARY KEY ((k1, k2)
      builder.append("(");
      boolean first = true;
      for (CqlIdentifier pkColumn : partitionKeyColumns) {
        if (first) {
          first = false;
        } else {
          builder.append(", ");
        }
        builder.append(pkColumn);
      }
      builder.append(")");
    }
    // PRIMARY KEY (<pk portion>, cc1, cc2, cc3)
    for (CqlIdentifier clusteringColumn : clusteringColumns) {
      builder.append(", ").append(clusteringColumn);
    }
    builder.append(")");
  }
}
