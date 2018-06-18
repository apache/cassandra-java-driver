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
package com.datastax.oss.driver.api.core.specex;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The policy that decides if the driver will send speculative queries to the next nodes when the
 * current node takes too long to respond.
 */
public interface SpeculativeExecutionPolicy extends AutoCloseable {

  /**
   * @param node the node that caused the speculative execution (that is, the node that was queried
   *     previously but was too slow to answer)
   * @param keyspace the CQL keyspace currently associated to the session. This is set either
   *     through the configuration, by calling {@link SessionBuilder#withKeyspace(CqlIdentifier)},
   *     or by manually executing a {@code USE} CQL statement. It can be {@code null} if the session
   *     has no keyspace.
   * @param request the request to execute.
   * @param runningExecutions the number of executions that are already running (including the
   *     initial, non-speculative request). For example, if this is 2 it means the initial attempt
   *     was sent, then the driver scheduled a first speculative execution, and it is now asking for
   *     the delay until the second speculative execution.
   * @return the time (in milliseconds) until a speculative request is sent to the next node, or 0
   *     to send it immediately, or a negative value to stop sending requests.
   */
  long nextExecution(
      @NonNull Node node,
      @Nullable CqlIdentifier keyspace,
      @NonNull Request request,
      int runningExecutions);

  /** Called when the cluster that this policy is associated with closes. */
  @Override
  void close();
}
