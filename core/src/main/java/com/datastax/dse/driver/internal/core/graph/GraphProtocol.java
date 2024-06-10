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
package com.datastax.dse.driver.internal.core.graph;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum GraphProtocol {
  GRAPHSON_1_0("graphson-1.0"),
  GRAPHSON_2_0("graphson-2.0"),
  GRAPH_BINARY_1_0("graph-binary-1.0"),
  ;

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
