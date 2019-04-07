/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
