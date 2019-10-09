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
package com.datastax.dse.driver.internal.core.data.geometry;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorExportToWkb;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.WkbExportFlags;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Helper class to serialize OGC geometries to Well-Known Binary, forcing the byte order to little
 * endian.
 *
 * <p>WKB encodes the byte order, so in theory we could send the buffer in any order, even if it is
 * different from the server. However DSE server performs an additional validation step server-side:
 * it deserializes to Java, serializes back to WKB, and then compares the original buffer to the
 * "re-serialized" one. If they don't match, a MarshalException is thrown. So with a client in
 * big-endian and a server in little-endian, we would get:
 *
 * <pre>
 * incoming buffer (big endian) --> Java --> reserialized buffer (little endian)
 * </pre>
 *
 * Since the two buffers have a different endian-ness, they don't match.
 *
 * <p>The ESRI library defaults to the native byte order and doesn't let us change it. Therefore:
 *
 * <ul>
 *   <li>if the native order is little endian (vast majority of cases), this class simply delegates
 *       to the appropriate public API method;
 *   <li>if the native order is big endian, it re-implements the serialization code, using
 *       reflection to get access to a private method. If reflection fails for any reason (updated
 *       ESRI library, security manager...), a runtime exception will be thrown.
 * </ul>
 */
class WkbUtil {

  private static final boolean IS_NATIVE_LITTLE_ENDIAN =
      ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN)
          && System.getProperty("com.datastax.driver.dse.geometry.FORCE_REFLECTION_WKB")
              == null; // only for tests

  static ByteBuffer asLittleEndianBinary(OGCGeometry ogcGeometry) {
    if (IS_NATIVE_LITTLE_ENDIAN) {
      return ogcGeometry.asBinary(); // the default implementation does what we want
    } else {
      int exportFlags;
      if (ogcGeometry instanceof OGCPoint) {
        exportFlags = 0;
      } else if (ogcGeometry instanceof OGCLineString) {
        exportFlags = WkbExportFlags.wkbExportLineString;
      } else if (ogcGeometry instanceof OGCPolygon) {
        exportFlags = WkbExportFlags.wkbExportPolygon;
      } else {
        throw new AssertionError("Unsupported type: " + ogcGeometry.getClass());
      }

      // Copy-pasted from OperatorExportToWkbLocal#execute, except for the flags and order
      int size = exportToWKB(exportFlags, ogcGeometry.getEsriGeometry(), null);
      ByteBuffer wkbBuffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
      exportToWKB(exportFlags, ogcGeometry.getEsriGeometry(), wkbBuffer);
      return wkbBuffer;
    }
  }

  // Provides reflective access to the private static method OperatorExportToWkbLocal#exportToWKB
  private static int exportToWKB(int exportFlags, Geometry geometry, ByteBuffer wkbBuffer) {
    assert !IS_NATIVE_LITTLE_ENDIAN;
    try {
      return (Integer) exportToWKB.invoke(null, exportFlags, geometry, wkbBuffer);
    } catch (Exception e) {
      throw new RuntimeException(
          "Couldn't invoke private method OperatorExportToWkbLocal#exportToWKB", e);
    }
  }

  private static final Method exportToWKB;

  static {
    if (IS_NATIVE_LITTLE_ENDIAN) {
      exportToWKB = null; // won't be used
    } else {
      try {
        OperatorExportToWkb op =
            (OperatorExportToWkb)
                OperatorFactoryLocal.getInstance().getOperator(Operator.Type.ExportToWkb);
        exportToWKB =
            op.getClass()
                .getDeclaredMethod("exportToWKB", int.class, Geometry.class, ByteBuffer.class);
        exportToWKB.setAccessible(true);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(
            "Couldn't get access to private method OperatorExportToWkbLocal#exportToWKB", e);
      }
    }
  }
}
