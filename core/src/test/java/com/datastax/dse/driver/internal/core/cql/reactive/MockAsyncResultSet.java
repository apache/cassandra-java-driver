/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.reactive;

import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.util.Lists;

public class MockAsyncResultSet implements AsyncResultSet {

  private final List<Row> rows;
  private final Iterator<Row> iterator;
  private final CompletionStage<AsyncResultSet> nextPage;
  private final ExecutionInfo executionInfo = mock(ExecutionInfo.class);
  private final ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
  private int remaining;

  public MockAsyncResultSet(int size, CompletionStage<AsyncResultSet> nextPage) {
    this(IntStream.range(0, size).boxed().map(MockRow::new).collect(Collectors.toList()), nextPage);
  }

  public MockAsyncResultSet(List<Row> rows, CompletionStage<AsyncResultSet> nextPage) {
    this.rows = rows;
    iterator = rows.iterator();
    remaining = rows.size();
    this.nextPage = nextPage;
  }

  @Override
  public Row one() {
    Row next = iterator.next();
    remaining--;
    return next;
  }

  @Override
  public int remaining() {
    return remaining;
  }

  @NonNull
  @Override
  public List<Row> currentPage() {
    return Lists.newArrayList(rows);
  }

  @Override
  public boolean hasMorePages() {
    return nextPage != null;
  }

  @NonNull
  @Override
  public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
    return nextPage;
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return columnDefinitions;
  }

  @NonNull
  @Override
  public ExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  @Override
  public boolean wasApplied() {
    return true;
  }
}
