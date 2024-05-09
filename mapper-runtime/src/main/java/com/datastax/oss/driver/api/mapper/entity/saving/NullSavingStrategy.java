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
package com.datastax.oss.driver.api.mapper.entity.saving;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.mapper.annotations.Entity;

/** Defines how null {@link Entity} properties will be handled during data insertion. */
public enum NullSavingStrategy {

  /**
   * Do not insert null properties.
   *
   * <p>In other words, the mapper won't call the corresponding setter on the {@link
   * BoundStatement}. The generated code looks approximately like this:
   *
   * <pre>
   * if (entity.getDescription() != null) {
   *   boundStatement = boundStatement.setString("description", entity.getDescription());
   * }
   * </pre>
   *
   * This avoids inserting tombstones for null properties. On the other hand, if the query is an
   * update and the column previously had another value, it won't be overwritten.
   *
   * <p>Note that unset values are only supported with {@link DefaultProtocolVersion#V4 native
   * protocol v4} (Cassandra 2.2) or above. If you try to use this strategy with a lower Cassandra
   * version, the mapper will throw a {@link MapperException} when you try to build the
   * corresponding DAO.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-7304">CASSANDRA-7304</a>
   */
  DO_NOT_SET,

  /**
   * Insert null properties as a CQL {@code NULL}.
   *
   * <p>In other words, the mapper will always call the corresponding setter on the {@link
   * BoundStatement}. The generated code looks approximately like this:
   *
   * <pre>
   * // Called even if entity.getDescription() == null
   * boundStatement = boundStatement.setString("description", entity.getDescription());
   * </pre>
   */
  SET_TO_NULL
}
