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

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.type.DseDataTypes;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultGeometry;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultLineString;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.esri.core.geometry.ogc.OGCLineString;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

/**
 * A custom type codec to use {@link LineString} instances in driver.
 *
 * <p>If you use {@link com.datastax.dse.driver.api.core.DseSessionBuilder} to build your cluster,
 * it will automatically register this codec.
 */
@ThreadSafe
public class LineStringCodec extends GeometryCodec<LineString> {

  private static final GenericType<LineString> JAVA_TYPE = GenericType.of(LineString.class);

  @NonNull
  @Override
  public GenericType<LineString> getJavaType() {
    return JAVA_TYPE;
  }

  @NonNull
  @Override
  protected LineString fromWellKnownText(@NonNull String source) {
    return new DefaultLineString(DefaultGeometry.fromOgcWellKnownText(source, OGCLineString.class));
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == LineString.class;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof LineString;
  }

  @NonNull
  @Override
  protected LineString fromWellKnownBinary(@NonNull ByteBuffer bb) {
    return new DefaultLineString(DefaultGeometry.fromOgcWellKnownBinary(bb, OGCLineString.class));
  }

  @NonNull
  @Override
  protected String toWellKnownText(@NonNull LineString geometry) {
    return geometry.asWellKnownText();
  }

  @NonNull
  @Override
  protected ByteBuffer toWellKnownBinary(@NonNull LineString geometry) {
    return geometry.asWellKnownBinary();
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DseDataTypes.LINE_STRING;
  }
}
