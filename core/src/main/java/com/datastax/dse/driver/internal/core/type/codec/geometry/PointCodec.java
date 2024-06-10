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
package com.datastax.dse.driver.internal.core.type.codec.geometry;

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.type.DseDataTypes;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultGeometry;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.esri.core.geometry.ogc.OGCPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

/**
 * A custom type codec to use {@link Point} instances in the driver.
 *
 * <p>If you use {@link com.datastax.dse.driver.api.core.DseSessionBuilder} to build your cluster,
 * it will automatically register this codec.
 */
@ThreadSafe
public class PointCodec extends GeometryCodec<Point> {

  private static final GenericType<Point> JAVA_TYPE = GenericType.of(Point.class);

  @NonNull
  @Override
  public GenericType<Point> getJavaType() {
    return JAVA_TYPE;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DseDataTypes.POINT;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == Point.class;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof Point;
  }

  @NonNull
  @Override
  protected String toWellKnownText(@NonNull Point geometry) {
    return geometry.asWellKnownText();
  }

  @NonNull
  @Override
  protected ByteBuffer toWellKnownBinary(@NonNull Point geometry) {
    return geometry.asWellKnownBinary();
  }

  @NonNull
  @Override
  protected Point fromWellKnownText(@NonNull String source) {
    return new DefaultPoint(DefaultGeometry.fromOgcWellKnownText(source, OGCPoint.class));
  }

  @NonNull
  @Override
  protected Point fromWellKnownBinary(@NonNull ByteBuffer source) {
    return new DefaultPoint(DefaultGeometry.fromOgcWellKnownBinary(source, OGCPoint.class));
  }
}
