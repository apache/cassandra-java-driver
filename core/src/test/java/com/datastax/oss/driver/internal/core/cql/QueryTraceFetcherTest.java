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
package com.datastax.oss.driver.internal.core.cql;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.TraceEvent;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryTraceFetcherTest {

  private static final UUID TRACING_ID = UUID.randomUUID();
  private static final ByteBuffer PAGING_STATE = Bytes.fromHexString("0xdeadbeef");
  private static final int PORT = 7000;

  @Mock private CqlSession session;
  @Mock private InternalDriverContext context;
  @Mock private DriverExecutionProfile config;
  @Mock private DriverExecutionProfile traceConfig;
  @Mock private NettyOptions nettyOptions;
  @Mock private EventExecutorGroup adminEventExecutorGroup;
  @Mock private EventExecutor eventExecutor;
  @Mock private InetAddress address;

  @Captor private ArgumentCaptor<SimpleStatement> statementCaptor;

  @Before
  public void setup() {
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventExecutorGroup);
    when(adminEventExecutorGroup.next()).thenReturn(eventExecutor);
    // Always execute scheduled tasks immediately:
    when(eventExecutor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
        .thenAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              // OK because the production code doesn't use the result:
              return null;
            });

    when(config.getInt(DefaultDriverOption.REQUEST_TRACE_ATTEMPTS)).thenReturn(3);
    // Doesn't really matter since we mock the scheduler
    when(config.getDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL)).thenReturn(Duration.ZERO);
    when(config.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.LOCAL_ONE.name());
    when(config.getString(DefaultDriverOption.REQUEST_TRACE_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.ONE.name());

    when(config.withString(
            DefaultDriverOption.REQUEST_CONSISTENCY, DefaultConsistencyLevel.ONE.name()))
        .thenReturn(traceConfig);
  }

  @Test
  public void should_succeed_when_both_queries_succeed_immediately() {
    // Given
    CompletionStage<AsyncResultSet> sessionRow = completeSessionRow();
    CompletionStage<AsyncResultSet> eventRows = singlePageEventRows();
    when(session.executeAsync(any(SimpleStatement.class)))
        .thenAnswer(invocation -> sessionRow)
        .thenAnswer(invocation -> eventRows);

    // When
    QueryTraceFetcher fetcher = new QueryTraceFetcher(TRACING_ID, session, context, config);
    CompletionStage<QueryTrace> traceFuture = fetcher.fetch();

    // Then
    verify(session, times(2)).executeAsync(statementCaptor.capture());
    List<SimpleStatement> statements = statementCaptor.getAllValues();
    assertSessionQuery(statements.get(0));
    SimpleStatement statement = statements.get(1);
    assertEventsQuery(statement);
    verifyNoMoreInteractions(session);

    assertThatStage(traceFuture)
        .isSuccess(
            trace -> {
              assertThat(trace.getTracingId()).isEqualTo(TRACING_ID);
              assertThat(trace.getRequestType()).isEqualTo("mock request");
              assertThat(trace.getDurationMicros()).isEqualTo(42);
              assertThat(trace.getCoordinatorAddress().getAddress()).isEqualTo(address);
              assertThat(trace.getCoordinatorAddress().getPort()).isEqualTo(PORT);
              assertThat(trace.getParameters())
                  .hasSize(2)
                  .containsEntry("key1", "value1")
                  .containsEntry("key2", "value2");
              assertThat(trace.getStartedAt()).isEqualTo(0);

              List<TraceEvent> events = trace.getEvents();
              assertThat(events).hasSize(3);
              for (int i = 0; i < events.size(); i++) {
                TraceEvent event = events.get(i);
                assertThat(event.getActivity()).isEqualTo("mock activity " + i);
                assertThat(event.getTimestamp()).isEqualTo(i);
                assertThat(event.getSourceAddress()).isNotNull();
                assertThat(event.getSourceAddress().getAddress()).isEqualTo(address);
                assertThat(event.getSourceAddress().getPort()).isEqualTo(PORT);
                assertThat(event.getSourceElapsedMicros()).isEqualTo(i);
                assertThat(event.getThreadName()).isEqualTo("mock thread " + i);
              }
            });
  }

  /**
   * This should not happen with a sane configuration, but we need to handle it in case {@link
   * DefaultDriverOption#REQUEST_PAGE_SIZE} is set ridiculously low.
   */
  @Test
  public void should_succeed_when_events_query_is_paged() {
    // Given
    CompletionStage<AsyncResultSet> sessionRow = completeSessionRow();
    CompletionStage<AsyncResultSet> eventRows1 = multiPageEventRows1();
    CompletionStage<AsyncResultSet> eventRows2 = multiPageEventRows2();
    when(session.executeAsync(any(SimpleStatement.class)))
        .thenAnswer(invocation -> sessionRow)
        .thenAnswer(invocation -> eventRows1)
        .thenAnswer(invocation -> eventRows2);

    // When
    QueryTraceFetcher fetcher = new QueryTraceFetcher(TRACING_ID, session, context, config);
    CompletionStage<QueryTrace> traceFuture = fetcher.fetch();

    // Then
    verify(session, times(3)).executeAsync(statementCaptor.capture());
    List<SimpleStatement> statements = statementCaptor.getAllValues();
    assertSessionQuery(statements.get(0));
    assertEventsQuery(statements.get(1));
    assertEventsQuery(statements.get(2));
    assertThat(statements.get(2).getPagingState()).isEqualTo(PAGING_STATE);
    verifyNoMoreInteractions(session);

    assertThatStage(traceFuture).isSuccess(trace -> assertThat(trace.getEvents()).hasSize(2));
  }

  @Test
  public void should_retry_when_session_row_is_incomplete() {
    // Given
    CompletionStage<AsyncResultSet> sessionRow1 = incompleteSessionRow();
    CompletionStage<AsyncResultSet> sessionRow2 = completeSessionRow();
    CompletionStage<AsyncResultSet> eventRows = singlePageEventRows();
    when(session.executeAsync(any(SimpleStatement.class)))
        .thenAnswer(invocation -> sessionRow1)
        .thenAnswer(invocation -> sessionRow2)
        .thenAnswer(invocation -> eventRows);

    // When
    QueryTraceFetcher fetcher = new QueryTraceFetcher(TRACING_ID, session, context, config);
    CompletionStage<QueryTrace> traceFuture = fetcher.fetch();

    // Then
    verify(session, times(3)).executeAsync(statementCaptor.capture());
    List<SimpleStatement> statements = statementCaptor.getAllValues();
    assertSessionQuery(statements.get(0));
    assertSessionQuery(statements.get(1));
    assertEventsQuery(statements.get(2));
    verifyNoMoreInteractions(session);

    assertThatStage(traceFuture)
        .isSuccess(
            trace -> {
              assertThat(trace.getTracingId()).isEqualTo(TRACING_ID);
              assertThat(trace.getRequestType()).isEqualTo("mock request");
              assertThat(trace.getDurationMicros()).isEqualTo(42);
              assertThat(trace.getCoordinatorAddress().getAddress()).isEqualTo(address);
              assertThat(trace.getCoordinatorAddress().getPort()).isEqualTo(PORT);
              assertThat(trace.getParameters())
                  .hasSize(2)
                  .containsEntry("key1", "value1")
                  .containsEntry("key2", "value2");
              assertThat(trace.getStartedAt()).isEqualTo(0);

              List<TraceEvent> events = trace.getEvents();
              assertThat(events).hasSize(3);
              for (int i = 0; i < events.size(); i++) {
                TraceEvent event = events.get(i);
                assertThat(event.getActivity()).isEqualTo("mock activity " + i);
                assertThat(event.getTimestamp()).isEqualTo(i);
                assertThat(event.getSourceAddress()).isNotNull();
                assertThat(event.getSourceAddress().getAddress()).isEqualTo(address);
                assertThat(event.getSourceAddress().getPort()).isEqualTo(PORT);
                assertThat(event.getSourceElapsedMicros()).isEqualTo(i);
                assertThat(event.getThreadName()).isEqualTo("mock thread " + i);
              }
            });
  }

  @Test
  public void should_fail_when_session_query_fails() {
    // Given
    RuntimeException mockError = new RuntimeException("mock error");
    when(session.executeAsync(any(SimpleStatement.class)))
        .thenReturn(CompletableFutures.failedFuture(mockError));

    // When
    QueryTraceFetcher fetcher = new QueryTraceFetcher(TRACING_ID, session, context, config);
    CompletionStage<QueryTrace> traceFuture = fetcher.fetch();

    // Then
    verify(session).executeAsync(statementCaptor.capture());
    SimpleStatement statement = statementCaptor.getValue();
    assertSessionQuery(statement);
    verifyNoMoreInteractions(session);

    assertThatStage(traceFuture).isFailed(error -> assertThat(error).isSameAs(mockError));
  }

  @Test
  public void should_fail_when_session_query_still_incomplete_after_max_tries() {
    // Given
    CompletionStage<AsyncResultSet> sessionRow1 = incompleteSessionRow();
    CompletionStage<AsyncResultSet> sessionRow2 = incompleteSessionRow();
    CompletionStage<AsyncResultSet> sessionRow3 = incompleteSessionRow();
    when(session.executeAsync(any(SimpleStatement.class)))
        .thenAnswer(invocation -> sessionRow1)
        .thenAnswer(invocation -> sessionRow2)
        .thenAnswer(invocation -> sessionRow3);

    // When
    QueryTraceFetcher fetcher = new QueryTraceFetcher(TRACING_ID, session, context, config);
    CompletionStage<QueryTrace> traceFuture = fetcher.fetch();

    // Then
    verify(session, times(3)).executeAsync(statementCaptor.capture());
    List<SimpleStatement> statements = statementCaptor.getAllValues();
    for (int i = 0; i < 3; i++) {
      assertSessionQuery(statements.get(i));
    }

    assertThatStage(traceFuture)
        .isFailed(
            error ->
                assertThat(error.getMessage())
                    .isEqualTo(
                        String.format("Trace %s still not complete after 3 attempts", TRACING_ID)));
  }

  private CompletionStage<AsyncResultSet> completeSessionRow() {
    return sessionRow(42);
  }

  private CompletionStage<AsyncResultSet> incompleteSessionRow() {
    return sessionRow(null);
  }

  private CompletionStage<AsyncResultSet> sessionRow(Integer duration) {
    Row row = mock(Row.class);
    ColumnDefinitions definitions = mock(ColumnDefinitions.class);
    when(row.getColumnDefinitions()).thenReturn(definitions);
    when(row.getString("request")).thenReturn("mock request");
    if (duration == null) {
      when(row.isNull("duration")).thenReturn(true);
    } else {
      when(row.getInt("duration")).thenReturn(duration);
    }
    when(row.getInetAddress("coordinator")).thenReturn(address);
    when(definitions.contains("coordinator_port")).thenReturn(true);
    when(row.getInt("coordinator_port")).thenReturn(PORT);
    when(row.getMap("parameters", String.class, String.class))
        .thenReturn(ImmutableMap.of("key1", "value1", "key2", "value2"));
    when(row.isNull("started_at")).thenReturn(false);
    when(row.getInstant("started_at")).thenReturn(Instant.EPOCH);

    AsyncResultSet rs = mock(AsyncResultSet.class);
    when(rs.one()).thenReturn(row);
    return CompletableFuture.completedFuture(rs);
  }

  private CompletionStage<AsyncResultSet> singlePageEventRows() {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      rows.add(eventRow(i));
    }

    AsyncResultSet rs = mock(AsyncResultSet.class);
    when(rs.currentPage()).thenReturn(rows);

    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(executionInfo.getPagingState()).thenReturn(null);
    when(rs.getExecutionInfo()).thenReturn(executionInfo);

    return CompletableFuture.completedFuture(rs);
  }

  private CompletionStage<AsyncResultSet> multiPageEventRows1() {
    AsyncResultSet rs = mock(AsyncResultSet.class);

    ImmutableList<Row> rows = ImmutableList.of(eventRow(0));
    when(rs.currentPage()).thenReturn(rows);

    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(executionInfo.getPagingState()).thenReturn(PAGING_STATE);
    when(rs.getExecutionInfo()).thenReturn(executionInfo);

    return CompletableFuture.completedFuture(rs);
  }

  private CompletionStage<AsyncResultSet> multiPageEventRows2() {
    AsyncResultSet rs = mock(AsyncResultSet.class);

    ImmutableList<Row> rows = ImmutableList.of(eventRow(1));
    when(rs.currentPage()).thenReturn(rows);

    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(executionInfo.getPagingState()).thenReturn(null);
    when(rs.getExecutionInfo()).thenReturn(executionInfo);

    return CompletableFuture.completedFuture(rs);
  }

  private Row eventRow(int i) {
    Row row = mock(Row.class);
    ColumnDefinitions definitions = mock(ColumnDefinitions.class);
    when(row.getColumnDefinitions()).thenReturn(definitions);
    when(row.getString("activity")).thenReturn("mock activity " + i);
    when(row.getUuid("event_id")).thenReturn(Uuids.startOf(i));
    when(row.getInetAddress("source")).thenReturn(address);
    when(definitions.contains("source_port")).thenReturn(true);
    when(row.getInt("source_port")).thenReturn(PORT);
    when(row.getInt("source_elapsed")).thenReturn(i);
    when(row.getString("thread")).thenReturn("mock thread " + i);
    return row;
  }

  private void assertSessionQuery(SimpleStatement statement) {
    assertThat(statement.getQuery())
        .isEqualTo("SELECT * FROM system_traces.sessions WHERE session_id = ?");
    assertThat(statement.getPositionalValues()).containsOnly(TRACING_ID);
    assertThat(statement.getExecutionProfile()).isEqualTo(traceConfig);
  }

  private void assertEventsQuery(SimpleStatement statement) {
    assertThat(statement.getQuery())
        .isEqualTo("SELECT * FROM system_traces.events WHERE session_id = ?");
    assertThat(statement.getPositionalValues()).containsOnly(TRACING_ID);
    assertThat(statement.getExecutionProfile()).isEqualTo(traceConfig);
  }
}
