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
package com.datastax.oss.driver.internal.core;

/**
 * Features that are commonly supported by most Apache Cassandra protocol versions.
 *
 * @see com.datastax.oss.driver.api.core.DefaultProtocolVersion
 */
public enum DefaultProtocolFeature implements ProtocolFeature {

  /**
   * The ability to leave variables unset in prepared statements.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-7304">CASSANDRA-7304</a>
   */
  UNSET_BOUND_VALUES,

  /**
   * The ability to override the keyspace on a per-request basis.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-10145">CASSANDRA-10145</a>
   */
  PER_REQUEST_KEYSPACE,

  /**
   * Support for smallint and tinyint types.
   *
   * @see <a href="https://jira.apache.org/jira/browse/CASSANDRA-8951">CASSANDRA-8951</a>
   */
  SMALLINT_AND_TINYINT_TYPES,

  /**
   * Support for the date type.
   *
   * @see <a href="https://jira.apache.org/jira/browse/CASSANDRA-7523">CASSANDRA-7523</a>
   */
  DATE_TYPE,

  /**
   * The ability to set a custom "now" time on statements (for testing purposes).
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-14664">CASSANDRA-14664</a>
   */
  NOW_IN_SECONDS,

  /**
   * The new protocol framing format introduced in Cassandra 4: wrapping multiple frames into a
   * single "segment" to checksum (and possibly compress) them together.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-15299">CASSANDRA-15299</a>
   */
  MODERN_FRAMING,
  ;
}
