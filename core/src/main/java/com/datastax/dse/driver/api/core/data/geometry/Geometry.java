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
package com.datastax.dse.driver.api.core.data.geometry;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;

/**
 * The driver-side representation for a DSE geospatial type.
 *
 * <pre>
 *     Row row = dseSession.execute("SELECT coords FROM points_of_interest WHERE name = 'Eiffel Tower'").one();
 *     Point coords = row.get("coords", Point.class);
 * </pre>
 *
 * The default implementations returned by the driver are immutable and serializable. If you write
 * your own implementations, they should at least be thread-safe; serializability is not mandatory,
 * but recommended for use with some 3rd-party tools like Apache Spark &trade;.
 */
public interface Geometry {

  /**
   * Returns a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text</a> (WKT)
   * representation of this geospatial type.
   */
  @NonNull
  String asWellKnownText();

  /**
   * Returns a <a href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known
   * Binary</a> (WKB) representation of this geospatial type.
   *
   * <p>Note that, due to DSE implementation details, the resulting byte buffer always uses
   * little-endian order, regardless of the platform's native order.
   */
  @NonNull
  ByteBuffer asWellKnownBinary();

  /** Returns a JSON representation of this geospatial type. */
  @NonNull
  String asGeoJson();

  /**
   * Tests whether this geospatial type instance contains another instance.
   *
   * @param other the other instance.
   * @return whether {@code this} contains {@code other}.
   */
  boolean contains(@NonNull Geometry other);
}
