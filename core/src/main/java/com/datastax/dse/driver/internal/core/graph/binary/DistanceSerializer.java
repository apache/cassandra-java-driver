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
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.internal.core.data.geometry.Distance;
import java.io.IOException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

public class DistanceSerializer extends AbstractSimpleGraphBinaryCustomSerializer<Distance> {
  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_DISTANCE_TYPE_NAME;
  }

  @Override
  protected Distance readCustomValue(int valueLength, Buffer buffer, GraphBinaryReader context)
      throws IOException {
    Point p = context.readValue(buffer, Point.class, false);
    checkValueSize(GraphBinaryUtils.sizeOfDistance(p), valueLength);
    return new Distance(p, context.readValue(buffer, Double.class, false));
  }

  @Override
  protected void writeCustomValue(Distance value, Buffer buffer, GraphBinaryWriter context)
      throws IOException {
    context.writeValue(GraphBinaryUtils.sizeOfDistance(value.getCenter()), buffer, false);
    context.writeValue(value.getCenter(), buffer, false);
    context.writeValue(value.getRadius(), buffer, false);
  }
}
