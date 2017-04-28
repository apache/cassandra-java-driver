/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/** Information about the execution of a query. */
public interface ExecutionInfo {

  /** The node that was used as a coordinator to successfully complete the query. */
  Node getCoordinator();

  /**
   * The number of speculative executions that were started for this query.
   *
   * <p>This does not include the initial, normal execution of the query. Therefore, if speculative
   * executions are disabled, this will always be 0. If they are enabled and one speculative
   * execution was triggered in addition to the initial execution, this will be 1, etc.
   *
   * @see SpeculativeExecutionPolicy
   */
  int getSpeculativeExecutionCount();

  /**
   * The index of the execution that completed this query.
   *
   * <p>0 represents the initial, normal execution of the query, 1 the first speculative execution,
   * etc.
   *
   * @see SpeculativeExecutionPolicy
   */
  int getSuccessfulExecutionIndex();

  /**
   * The errors encountered on previous coordinators, if any.
   *
   * <p>The list is in chronological order, based on the time that the driver processed the error
   * responses. If speculative executions are enabled, they run concurrently so their errors will be
   * interleaved. A node can appear multiple times (if the retry policy decided to retry on the same
   * node).
   */
  List<Map.Entry<Node, Throwable>> getErrors();

  /**
   * The paging state of the query.
   *
   * <p>This represents the next page to be fetched if this query has multiple page of results. It
   * can be saved and reused later on the same statement.
   *
   * @return the paging state, or {@code null} if there is no next page.
   */
  ByteBuffer getPagingState();

  /**
   * The server-side warnings for this query, if any (otherwise the list will be empty).
   *
   * <p>This feature is only available with {@link CoreProtocolVersion#V4} or above; with lower
   * versions, this list will always be empty.
   */
  List<String> getWarnings();

  /**
   * The custom payload sent back by the server with the response, if any (otherwise the map will be
   * empty).
   *
   * <p>This method returns a read-only view of the original map, but its values remain inherently
   * mutable. If multiple clients will read these values, care should be taken not to corrupt the
   * data (in particular, preserve the indices by calling {@link ByteBuffer#duplicate()}).
   *
   * <p>This feature is only available with {@link CoreProtocolVersion#V4} or above; with lower
   * versions, this map will always be empty.
   */
  Map<String, ByteBuffer> getIncomingPayload();
}
