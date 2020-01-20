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
package com.datastax.oss.driver.internal.core.adminrequest;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.AbstractIterator;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Rows;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe // wraps a mutable queue
public class AdminResult implements Iterable<AdminRow> {

  private final Queue<List<ByteBuffer>> data;
  private final Map<String, ColumnSpec> columnSpecs;
  private final AdminRequestHandler<AdminResult> nextHandler;
  private final ProtocolVersion protocolVersion;

  public AdminResult(
      Rows rows, AdminRequestHandler<AdminResult> nextHandler, ProtocolVersion protocolVersion) {
    this.data = rows.getData();

    ImmutableMap.Builder<String, ColumnSpec> columnSpecsBuilder = ImmutableMap.builder();
    for (ColumnSpec spec : rows.getMetadata().columnSpecs) {
      columnSpecsBuilder.put(spec.name, spec);
    }
    // Admin queries are simple selects only, so there are no duplicate names (if that ever
    // changes, build() will fail and we'll have to do things differently)
    this.columnSpecs = columnSpecsBuilder.build();

    this.nextHandler = nextHandler;
    this.protocolVersion = protocolVersion;
  }

  /** This consumes the result's data and can be called only once. */
  @NonNull
  @Override
  public Iterator<AdminRow> iterator() {
    return new AbstractIterator<AdminRow>() {
      @Override
      protected AdminRow computeNext() {
        List<ByteBuffer> rowData = data.poll();
        return (rowData == null)
            ? endOfData()
            : new AdminRow(columnSpecs, rowData, protocolVersion);
      }
    };
  }

  public boolean hasNextPage() {
    return nextHandler != null;
  }

  public CompletionStage<AdminResult> nextPage() {
    return (nextHandler == null)
        ? CompletableFutures.failedFuture(
            new AssertionError("No next page, use hasNextPage() before you call this method"))
        : nextHandler.start();
  }
}
