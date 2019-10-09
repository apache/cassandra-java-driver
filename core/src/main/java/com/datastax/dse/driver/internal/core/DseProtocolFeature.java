/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core;

import com.datastax.oss.driver.internal.core.ProtocolFeature;

/**
 * Features that are supported by DataStax Enterprise (DSE) protocol versions.
 *
 * @see com.datastax.dse.driver.api.core.DseProtocolVersion
 * @see com.datastax.oss.driver.internal.core.DefaultProtocolFeature
 */
public enum DseProtocolFeature implements ProtocolFeature {

  /**
   * The ability to execute continuous paging requests.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-11521">CASSANDRA-11521</a>
   * @see com.datastax.dse.driver.api.core.cql.continuous.ContinuousSession
   */
  CONTINUOUS_PAGING,
  ;
}
