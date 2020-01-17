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
