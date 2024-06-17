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
package com.datastax.dse.driver.api.core.cql.continuous;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * The result of a {@linkplain ContinuousSession#executeContinuously(Statement) synchronous
 * continuous paging query}.
 *
 * <p>It uses {@linkplain ContinuousAsyncResultSet asynchronous calls} internally, but blocks on the
 * results in order to provide a synchronous API to its clients. If the query is paged, only the
 * first page will be fetched initially, and iteration will trigger background fetches of the next
 * pages when necessary.
 *
 * <p>Note that this object can only be iterated once: rows are "consumed" as they are read,
 * subsequent calls to {@code iterator()} will return the same iterator instance.
 *
 * <p>Implementations of this type are <b>not</b> thread-safe. They can only be iterated by the
 * thread that invoked {@code session.executeContinuously}.
 */
public interface ContinuousResultSet extends ResultSet {

  /**
   * Cancels the continuous query.
   *
   * <p>There might still be rows available in the current page after the cancellation; the
   * iteration will only stop when such rows are fully iterated upon.
   *
   * <p>Also, there might be more pages available in the driver's local page cache after the
   * cancellation; <em>these extra pages will be discarded</em>.
   *
   * <p>Therefore, if you plan to resume the iteration later, the correct procedure is as follows:
   *
   * <ol>
   *   <li>Cancel the operation by invoking this method;
   *   <li>Keep iterating on this object until it doesn't return any more rows;
   *   <li>Retrieve the paging state with {@link #getExecutionInfo()
   *       getExecutionInfo().getPagingState()};
   *   <li>{@linkplain Statement#setPagingState(ByteBuffer) Re-inject the paging state} in the
   *       statement;
   *   <li>Resume the operation by invoking {@link ContinuousSession#executeContinuously(Statement)
   *       executeContinuously} again.
   * </ol>
   */
  void cancel();

  /**
   * {@inheritDoc}
   *
   * <p>Note: because the driver does not support query traces for continuous queries, {@link
   * ExecutionInfo#getTracingId()} will always be {@code null}.
   */
  @NonNull
  @Override
  default ExecutionInfo getExecutionInfo() {
    List<ExecutionInfo> infos = getExecutionInfos();
    return infos.get(infos.size() - 1);
  }
}
