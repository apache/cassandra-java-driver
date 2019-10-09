/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.insights.schema;

public enum InsightType {
  EVENT,
  GAUGE,
  COUNTER,
  HISTOGRAM,
  TIMER,
  METER,
  LOG;
}
