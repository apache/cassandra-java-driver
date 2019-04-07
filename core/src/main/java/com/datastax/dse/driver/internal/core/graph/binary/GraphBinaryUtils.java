/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
