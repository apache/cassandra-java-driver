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

import com.datastax.dse.driver.api.core.data.geometry.Point;
import java.nio.charset.StandardCharsets;

class GraphBinaryUtils {
  static int sizeOfInt() {
    return 4;
  }

  static int sizeOfLong() {
    return 8;
  }

  static int sizeOfDouble() {
    return 8;
  }

  static int sizeOfPoint(Point point) {
    return point.asWellKnownBinary().remaining();
  }

  /* assumes UTF8 */
  static int sizeOfString(String s) {
    // length + data length
    return sizeOfInt() + s.getBytes(StandardCharsets.UTF_8).length;
  }

  static int sizeOfDuration() {
    return sizeOfInt() + sizeOfInt() + sizeOfLong();
  }

  static int sizeOfDistance(Point point) {
    return sizeOfPoint(point) + sizeOfDouble();
  }

  static int sizeOfEditDistance(String s) {
    return sizeOfInt() + sizeOfString(s);
  }
}
