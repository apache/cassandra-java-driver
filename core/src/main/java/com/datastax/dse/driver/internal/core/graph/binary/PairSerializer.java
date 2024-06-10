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
package com.datastax.dse.driver.internal.core.graph.binary;

import java.io.IOException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.javatuples.Pair;

public class PairSerializer extends AbstractDynamicGraphBinaryCustomSerializer<Pair> {

  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_PAIR_TYPE_NAME;
  }

  @Override
  protected Pair readDynamicCustomValue(Buffer buffer, GraphBinaryReader context)
      throws IOException {
    return new Pair<>(context.read(buffer), context.read(buffer));
  }

  @Override
  protected void writeDynamicCustomValue(Pair value, Buffer buffer, GraphBinaryWriter context)
      throws IOException {
    context.write(value.getValue0(), buffer);
    context.write(value.getValue1(), buffer);
  }
}
