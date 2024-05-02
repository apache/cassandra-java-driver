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
package com.datastax.dse.driver.api.core.graph.reactive;

import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link GraphNode} produced by a {@linkplain ReactiveGraphResultSet reactive graph result set}.
 *
 * <p>This is essentially an extension of the driver's {@link GraphNode} object that also exposes
 * useful information about {@linkplain #getExecutionInfo() request execution} (note however that
 * this information is also exposed at result set level for convenience).
 *
 * @see ReactiveGraphSession
 * @see ReactiveGraphResultSet
 */
public interface ReactiveGraphNode extends GraphNode {

  /**
   * The execution information for the paged request that produced this result.
   *
   * <p>This object is the same for two rows pertaining to the same page, but differs for rows
   * pertaining to different pages.
   *
   * @return the execution information for the paged request that produced this result.
   * @see ReactiveGraphResultSet#getExecutionInfos()
   */
  @NonNull
  ExecutionInfo getExecutionInfo();
}
