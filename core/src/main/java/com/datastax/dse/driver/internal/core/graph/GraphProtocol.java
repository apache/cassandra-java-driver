/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum GraphProtocol {
  GRAPHSON_1_0("graphson-1.0"),
  GRAPHSON_2_0("graphson-2.0"),
  GRAPHSON_3_0("graphson-3.0"),
  GRAPH_BINARY_1_0("graph-binary-1.0");

  private static final Map<String, GraphProtocol> BY_CODE;

  static {
    Map<String, GraphProtocol> tmp = new HashMap<>();
    for (GraphProtocol value : values()) {
      tmp.put(value.stringRepresentation, value);
    }
    BY_CODE = Collections.unmodifiableMap(tmp);
  }

  private final String stringRepresentation;

  GraphProtocol(String stringRepresentation) {
    this.stringRepresentation = stringRepresentation;
  }

  @NonNull
  public String toInternalCode() {
    return stringRepresentation;
  }

  @NonNull
  public static GraphProtocol fromString(@Nullable String stringRepresentation) {
    if (stringRepresentation == null || !BY_CODE.containsKey(stringRepresentation)) {
      StringBuilder sb =
          new StringBuilder(
              String.format(
                  "Graph protocol used [\"%s\"] unknown. Possible values are: [ \"%s\"",
                  stringRepresentation, GraphProtocol.values()[0].toInternalCode()));
      for (int i = 1; i < GraphProtocol.values().length; i++) {
        sb.append(String.format(", \"%s\"", GraphProtocol.values()[i].toInternalCode()));
      }
      sb.append("]");
      throw new IllegalArgumentException(sb.toString());
    }
    return BY_CODE.get(stringRepresentation);
  }

  public boolean isGraphBinary() {
    return this == GRAPH_BINARY_1_0;
  }
}
