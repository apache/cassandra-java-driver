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
package com.datastax.oss.driver.api.querybuilder.insert;

import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/** A complete INSERT statement that is ready to be built. */
public interface Insert extends BuildableQuery {

  /** Adds an IF NOT EXISTS clause to this statement. */
  @NonNull
  Insert ifNotExists();

  /**
   * Adds a USING TIMESTAMP clause to this statement with a literal value.
   *
   * <p>If this method or {@link #usingTimestamp(BindMarker)} is called multiple times, the last
   * value is used.
   */
  @NonNull
  Insert usingTimestamp(long timestamp);

  /**
   * Adds a USING TIMESTAMP clause to this statement with a bind marker.
   *
   * <p>If this method or {@link #usingTimestamp(long)} is called multiple times, the last value is
   * used. Passing {@code null} to this method removes any previous timestamp.
   */
  @NonNull
  Insert usingTimestamp(@Nullable BindMarker bindMarker);

  /**
   * Adds a {@code USING TTL} clause to this statement with a literal value. Setting a value of
   * {@code null} will remove the {@code USING TTL} clause on this statement. Setting a value of
   * {@code 0} will insert the data with no TTL when the statement is executed, overriding any Table
   * TTL that might exist.
   *
   * <p>If this method or {@link #usingTtl(BindMarker) } is called multiple times, the value from
   * the last invocation is used.
   *
   * @param ttlInSeconds Time, in seconds, the inserted data should live before expiring.
   */
  @NonNull
  Insert usingTtl(int ttlInSeconds);

  /**
   * Adds a {@code USING TTL} clause to this statement with a bind marker. Setting a value of {@code
   * null} will remove the {@code USING TTL} clause on this statement. Binding a value of {@code 0}
   * will insert the data with no TTL when the statement is executed, overriding any Table TTL that
   * might exist.
   *
   * <p>If this method or {@link #usingTtl(int) } is called multiple times, the value from the last
   * invocation is used.
   *
   * @param bindMarker A bind marker that is understood to be a value in seconds.
   */
  @NonNull
  Insert usingTtl(@Nullable BindMarker bindMarker);
}
