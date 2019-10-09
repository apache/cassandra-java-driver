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
package com.datastax.dse.driver.api.core.type.codec;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.dse.driver.internal.core.type.codec.geometry.LineStringCodec;
import com.datastax.dse.driver.internal.core.type.codec.geometry.PointCodec;
import com.datastax.dse.driver.internal.core.type.codec.geometry.PolygonCodec;
import com.datastax.dse.driver.internal.core.type.codec.time.DateRangeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

/** Extends {@link TypeCodecs} to handle DSE-specific types. */
public class DseTypeCodecs extends TypeCodecs {

  public static final TypeCodec<LineString> LINE_STRING = new LineStringCodec();

  public static final TypeCodec<Point> POINT = new PointCodec();

  public static final TypeCodec<Polygon> POLYGON = new PolygonCodec();

  public static final TypeCodec<DateRange> DATE_RANGE = new DateRangeCodec();
}
