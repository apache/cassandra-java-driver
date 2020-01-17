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
package com.datastax.dse.driver.api.core.cql.continuous;

import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;

/**
 * The result of an {@linkplain ContinuousSession#executeContinuouslyAsync(Statement) asynchronous
 * continuous paging query}.
 *
 * <p>DSE replies to a continuous query with a stream of response frames. There is one instance of
 * this class for each frame.
 */
public interface ContinuousAsyncResultSet
    extends AsyncPagingIterable<Row, ContinuousAsyncResultSet> {

  /** Returns the current page's number. Pages are numbered starting from 1. */
  int pageNumber();

  /**
   * Cancels the continuous query.
   *
   * <p>There might still be rows available in the {@linkplain #currentPage() current page} after
   * the cancellation; these rows can be retrieved normally.
   *
   * <p>Also, there might be more pages available in the driver's local page cache after the
   * cancellation; <em>these extra pages will be discarded</em>.
   *
   * <p>Therefore, if you plan to resume the iteration later, the correct procedure is as follows:
   *
   * <ol>
   *   <li>Cancel the operation by invoking this method, or by cancelling the {@linkplain
   *       #fetchNextPage() next page's future};
   *   <li>Keep iterating on the current page until it doesn't return any more rows;
   *   <li>Retrieve the paging state with {@link #getExecutionInfo()
   *       getExecutionInfo().getPagingState()};
   *   <li>{@linkplain Statement#setPagingState(ByteBuffer) Re-inject the paging state} in the
   *       statement;
   *   <li>Resume the operation by invoking {@link
   *       ContinuousSession#executeContinuouslyAsync(Statement) executeContinuouslyAsync} again.
   * </ol>
   *
   * After a cancellation, futures returned by {@link #fetchNextPage()} that are not yet complete
   * will always complete exceptionally by throwing a {@link CancellationException}, <em>even if
   * they were obtained before the cancellation</em>.
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
  ExecutionInfo getExecutionInfo();
}
