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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.api.querybuilder.update;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The beginning of an UPDATE statement. It needs at least one assignment before the WHERE clause
 * can be added.
 */
public interface UpdateStart extends OngoingAssignment {

  /**
   * Adds a USING TIMESTAMP clause to this statement with a literal value.
   *
   * <p>If this method or {@link #usingTimestamp(BindMarker)} is called multiple times, the last
   * value is used.
   */
  @NonNull
  UpdateStart usingTimestamp(long timestamp);

  /**
   * Adds a USING TIMESTAMP clause to this statement with a bind marker.
   *
   * <p>If this method or {@link #usingTimestamp(long)} is called multiple times, the last value is
   * used.
   */
  @NonNull
  UpdateStart usingTimestamp(@NonNull BindMarker bindMarker);

  /**
   * Adds a {@code USING TTL} clause to this statement with a literal value. Setting a value of
   * {@code null} will remove the {@code USING TTL} clause on this statement. Setting a value of
   * {@code 0} will update the data and remove any TTL on the column when the statement is executed,
   * overriding any TTL (table or column) that may exist in Cassandra.
   *
   * <p>If this method or {@link #usingTtl(BindMarker) } is called multiple times, the value from
   * the last invocation is used.
   *
   * @param ttlInSeconds Time, in seconds, the inserted data should live before expiring.
   */
  @NonNull
  UpdateStart usingTtl(int ttlInSeconds);

  /**
   * Adds a {@code USING TTL} clause to this statement with a bind marker. Setting a value of {@code
   * null} will remove the {@code USING TTL} clause on this statement. Binding a value of {@code 0}
   * will update the data and remove any TTL on the column when the statement is executed,
   * overriding any TTL (table or column) that may exist in Cassandra.
   *
   * <p>If this method or {@link #usingTtl(int)} is called multiple times, the value from the last
   * invocation is used.
   *
   * @param bindMarker A bind marker that is understood to be a value in seconds.
   */
  @NonNull
  UpdateStart usingTtl(@NonNull BindMarker bindMarker);

  /**
   * Adds a {@code USING TIMEOUT} clause to this statement with a literal value. Setting a value of
   * {@code null} will remove the {@code USING TIMEOUT} clause on this statement.
   *
   * <p>If this method or {@link #usingTimeout(BindMarker) } is called multiple times, the value
   * from the last invocation is used.
   *
   * @param timeout A timeout value controlling server-side query timeout.
   */
  @NonNull
  UpdateStart usingTimeout(@NonNull CqlDuration timeout);

  /**
   * Adds a {@code USING TIMEOUT} clause to this statement with a bind marker. Setting a value of
   * {@code null} will remove the {@code USING TIMEOUT} clause on this statement.
   *
   * <p>If this method or {@link #usingTimeout(CqlDuration) } is called multiple times, the value
   * from the last invocation is used.
   *
   * @param timeout A bind marker understood as {@link CqlDuration} controlling server-side query
   *     timeout.
   */
  @NonNull
  UpdateStart usingTimeout(@NonNull BindMarker timeout);
}
