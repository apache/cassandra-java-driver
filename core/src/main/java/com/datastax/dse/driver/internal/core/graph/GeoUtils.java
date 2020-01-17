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
package com.datastax.dse.driver.internal.core.graph;

public class GeoUtils {
  private static final double DEGREES_TO_RADIANS = Math.PI / 180;
  private static final double EARTH_MEAN_RADIUS_KM = 6371.0087714;
  private static final double DEG_TO_KM = DEGREES_TO_RADIANS * EARTH_MEAN_RADIUS_KM;
  private static final double KM_TO_MILES = 0.621371192;
  public static final double KM_TO_DEG = 1 / DEG_TO_KM;
  public static final double MILES_TO_KM = 1 / KM_TO_MILES;
}
