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
package com.datastax.dse.driver.internal.core.type.codec.geometry;

import static com.datastax.oss.driver.internal.core.util.Strings.isQuoted;

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.util.Strings;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

/** Base class for geospatial type codecs. */
@ThreadSafe
public abstract class GeometryCodec<T extends Geometry> implements TypeCodec<T> {

  @Nullable
  @Override
  public T decode(@Nullable ByteBuffer bb, @NonNull ProtocolVersion protocolVersion) {
    return bb == null || bb.remaining() == 0 ? null : fromWellKnownBinary(bb.slice());
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable T geometry, @NonNull ProtocolVersion protocolVersion) {
    return geometry == null ? null : toWellKnownBinary(geometry);
  }

  @Nullable
  @Override
  public T parse(@Nullable String s) {
    if (s == null) {
      return null;
    }
    s = s.trim();
    if (s.isEmpty() || s.equalsIgnoreCase("NULL")) {
      return null;
    }
    if (!isQuoted(s)) {
      throw new IllegalArgumentException("Geometry values must be enclosed by single quotes");
    }
    return fromWellKnownText(Strings.unquote(s));
  }

  @NonNull
  @Override
  public String format(@Nullable T geometry) throws IllegalArgumentException {
    return geometry == null ? "NULL" : Strings.quote(toWellKnownText(geometry));
  }

  /**
   * Creates an instance of this codec's geospatial type from its <a
   * href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text</a> (WKT) representation.
   *
   * @param source the Well-known Text representation to parse. Cannot be null.
   * @return A new instance of this codec's geospatial type.
   * @throws IllegalArgumentException if the string does not contain a valid Well-known Text
   *     representation.
   */
  @NonNull
  protected abstract T fromWellKnownText(@NonNull String source);

  /**
   * Creates an instance of a geospatial type from its <a
   * href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known Binary</a>
   * (WKB) representation.
   *
   * @param bb the Well-known Binary representation to parse. Cannot be null.
   * @return A new instance of this codec's geospatial type.
   * @throws IllegalArgumentException if the given {@link ByteBuffer} does not contain a valid
   *     Well-known Binary representation.
   */
  @NonNull
  protected abstract T fromWellKnownBinary(@NonNull ByteBuffer bb);

  /**
   * Returns a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text</a> (WKT)
   * representation of the given geospatial object.
   *
   * @param geometry the geospatial object to convert. Cannot be null.
   * @return A Well-known Text representation of the given object.
   */
  @NonNull
  protected abstract String toWellKnownText(@NonNull T geometry);

  /**
   * Returns a <a href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known
   * Binary</a> (WKB) representation of the given geospatial object.
   *
   * @param geometry the geospatial object to convert. Cannot be null.
   * @return A Well-known Binary representation of the given object.
   */
  @NonNull
  protected abstract ByteBuffer toWellKnownBinary(@NonNull T geometry);
}
