/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
