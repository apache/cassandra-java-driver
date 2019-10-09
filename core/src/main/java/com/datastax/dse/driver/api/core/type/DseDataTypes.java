/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.type;

import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataTypes;

/** Extends {@link DataTypes} to handle DSE-specific types. */
public class DseDataTypes extends DataTypes {

  public static final CustomType LINE_STRING =
      (CustomType) custom("org.apache.cassandra.db.marshal.LineStringType");

  public static final CustomType POINT =
      (CustomType) custom("org.apache.cassandra.db.marshal.PointType");

  public static final CustomType POLYGON =
      (CustomType) custom("org.apache.cassandra.db.marshal.PolygonType");

  public static final CustomType DATE_RANGE =
      (CustomType) custom("org.apache.cassandra.db.marshal.DateRangeType");
}
