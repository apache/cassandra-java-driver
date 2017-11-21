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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.TraceEvent;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;

@RunWith(MockitoJUnitRunner.class)
public class QueryTraceFetcherTest {

  private static final UUID TRACING_ID = UUID.randomUUID();
  private static final ByteBuffer PAGING_STATE = Bytes.fromHexString("0xdeadbeef");

  @Mock private CqlSession session;
  @Mock private InternalDriverContext context;
  @Mock private DriverConfigProfile config;
  @Mock private DriverConfigProfile traceConfig;
  @Mock private NettyOptions nettyOptions;
  @Mock private EventExecutorGroup adminEventExecutorGroup;
  @Mock private EventExecutor eventExecutor;
  @Mock private InetAddress address;

  @Captor private ArgumentCaptor<SimpleStatement> statementCaptor;

  @Before
  public void setup() {
    Mockito.when(context.nettyOptions()).thenReturn(nettyOptions);
    Mockito.when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventExecutorGroup);
    Mockito.when(adminEventExecutorGroup.next()).thenReturn(eventExecutor);
    // Always execute scheduled tasks immediately:
    Mockito.when(eventExecutor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
        .thenAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              // OK because the production code doesn't use the result:
              return null;
            });

    Mockito.when(config.getInt(CoreDriverOption.REQUEST_TRACE_ATTEMPTS)).thenReturn(3);
    // Doesn't really matter since we mock the scheduler
    Mockito.when(config.getDuration(CoreDriverOption.REQUEST_TRACE_INTERVAL))
        .thenReturn(Duration.ZERO);
    Mockito.when(config.getConsistencyLevel(CoreDriverOption.REQUEST_TRACE_CONSISTENCY))
        .thenReturn(ConsistencyLevel.ONE);

    Mockito.when(
            config.withConsistencyLevel(CoreDriverOption.REQUEST_CONSISTENCY, ConsistencyLevel.ONE))
        .thenReturn(traceConfig);
  }

  @Test
  public void should_succeed_when_both_queries_succeed_immediately() {
    // Given
    CompletionStage<AsyncResultSet> sessionRow = completeSessionRow();
    CompletionStage<AsyncResultSet> eventRows = singlePageEventRows();
    Mockito.when(session.executeAsync(any(SimpleStatement.class)))
        .thenReturn(sessionRow, eventRows);

    // When
    QueryTraceFetcher fetcher = new QueryTraceFetcher(TRACING_ID, session, context, config);
    CompletionStage<QueryTrace> traceFuture = fetcher.fetch();

    // Then
    Mockito.verify(session, times(2)).executeAsync(statementCaptor.capture());
    List<SimpleStatement> statements = statementCaptor.getAllValues();
    assertSessionQuery(statements.get(0));
    SimpleStatement statement = statements.get(1);
    assertEventsQuery(statement);
    Mockito.verifyNoMoreInteractions(session);

    assertThat(traceFuture)
        .isSuccess(
            trace -> {
              assertThat(trace.getTracingId()).isEqualTo(TRACING_ID);
              assertThat(trace.getRequestType()).isEqualTo("mock request");
              assertThat(trace.getDurationMicros()).isEqualTo(42);
              assertThat(trace.getCoordinator()).isEqualTo(address);
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
                assertThat(event.getSource()).isEqualTo(address);
                assertThat(event.getSourceElapsedMicros()).isEqualTo(i);
                assertThat(event.getThreadName()).isEqualTo("mock thread " + i);
              }
            });
  }

  /**
   * This should not happen with a sane configuration, but we need to handle it in case {@link
   * CoreDriverOption#REQUEST_PAGE_SIZE} is set ridiculously low.
   */
  @Test
  public void should_succeed_when_events_query_is_paged() {
    // Given
    CompletionStage<AsyncResultSet> sessionRow = completeSessionRow();
    CompletionStage<AsyncResultSet> eventRows1 = multiPageEventRows1();
    CompletionStage<AsyncResultSet> eventRows2 = multiPageEventRows2();
    Mockito.when(session.executeAsync(any(SimpleStatement.class)))
        .thenReturn(sessionRow, eventRows1, eventRows2);

    // When
    QueryTraceFetcher fetcher = new QueryTraceFetcher(TRACING_ID, session, context, config);
    CompletionStage<QueryTrace> traceFuture = fetcher.fetch();

    // Then
    Mockito.verify(session, times(3)).executeAsync(statementCaptor.capture());
    List<SimpleStatement> statements = statementCaptor.getAllValues();
    assertSessionQuery(statements.get(0));
    assertEventsQuery(statements.get(1));
    assertEventsQuery(statements.get(2));
    assertThat(statements.get(2).getPagingState()).isEqualTo(PAGING_STATE);
    Mockito.verifyNoMoreInteractions(session);

    assertThat(traceFuture).isSuccess(trace -> assertThat(trace.getEvents()).hasSize(2));
  }

  @Test
  public void should_retry_when_session_row_is_incomplete() {
    // Given
    CompletionStage<AsyncResultSet> sessionRow1 = incompleteSessionRow();
    CompletionStage<AsyncResultSet> sessionRow2 = completeSessionRow();
    CompletionStage<AsyncResultSet> eventRows = singlePageEventRows();
    Mockito.when(session.executeAsync(any(SimpleStatement.class)))
        .thenReturn(sessionRow1, sessionRow2, eventRows);

    // When
    QueryTraceFetcher fetcher = new QueryTraceFetcher(TRACING_ID, session, context, config);
    CompletionStage<QueryTrace> traceFuture = fetcher.fetch();

    // Then
    Mockito.verify(session, times(3)).executeAsync(statementCaptor.capture());
    List<SimpleStatement> statements = statementCaptor.getAllValues();
    assertSessionQuery(statements.get(0));
    assertSessionQuery(statements.get(1));
    assertEventsQuery(statements.get(2));
    Mockito.verifyNoMoreInteractions(session);

    assertThat(traceFuture)
        .isSuccess(
            trace -> {
              assertThat(trace.getTracingId()).isEqualTo(TRACING_ID);
              assertThat(trace.getRequestType()).isEqualTo("mock request");
              assertThat(trace.getDurationMicros()).isEqualTo(42);
              assertThat(trace.getCoordinator()).isEqualTo(address);
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
                assertThat(event.getSource()).isEqualTo(address);
                assertThat(event.getSourceElapsedMicros()).isEqualTo(i);
                assertThat(event.getThreadName()).isEqualTo("mock thread " + i);
              }
            });
  }

  @Test
  public void should_fail_when_session_query_fails() {
    // Given
    RuntimeException mockError = new RuntimeException("mock error");
    Mockito.when(session.executeAsync(any(SimpleStatement.class)))
        .thenReturn(CompletableFutures.failedFuture(mockError));

    // When
    QueryTraceFetcher fetcher = new QueryTraceFetcher(TRACING_ID, session, context, config);
    CompletionStage<QueryTrace> traceFuture = fetcher.fetch();

    // Then
    Mockito.verify(session).executeAsync(statementCaptor.capture());
    SimpleStatement statement = statementCaptor.getValue();
    assertSessionQuery(statement);
    Mockito.verifyNoMoreInteractions(session);

    assertThat(traceFuture).isFailed(error -> assertThat(error).isSameAs(mockError));
  }

  @Test
  public void should_fail_when_session_query_still_incomplete_after_max_tries() {
    // Given
    CompletionStage<AsyncResultSet> sessionRow1 = incompleteSessionRow();
    CompletionStage<AsyncResultSet> sessionRow2 = incompleteSessionRow();
    CompletionStage<AsyncResultSet> sessionRow3 = incompleteSessionRow();
    Mockito.when(session.executeAsync(any(SimpleStatement.class)))
        .thenReturn(sessionRow1, sessionRow2, sessionRow3);

    // When
    QueryTraceFetcher fetcher = new QueryTraceFetcher(TRACING_ID, session, context, config);
    CompletionStage<QueryTrace> traceFuture = fetcher.fetch();

    // Then
    Mockito.verify(session, times(3)).executeAsync(statementCaptor.capture());
    List<SimpleStatement> statements = statementCaptor.getAllValues();
    for (int i = 0; i < 3; i++) {
      assertSessionQuery(statements.get(i));
    }

    assertThat(traceFuture)
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
    Row row = Mockito.mock(Row.class);
    Mockito.when(row.getString("request")).thenReturn("mock request");
    if (duration == null) {
      Mockito.when(row.isNull("duration")).thenReturn(true);
    } else {
      Mockito.when(row.getInt("duration")).thenReturn(duration);
    }
    Mockito.when(row.getInetAddress("coordinator")).thenReturn(address);
    Mockito.when(row.getMap("parameters", String.class, String.class))
        .thenReturn(ImmutableMap.of("key1", "value1", "key2", "value2"));
    Mockito.when(row.isNull("started_at")).thenReturn(false);
    Mockito.when(row.getInstant("started_at")).thenReturn(Instant.EPOCH);

    AsyncResultSet rs = Mockito.mock(AsyncResultSet.class);
    Mockito.when(rs.one()).thenReturn(row);
    return CompletableFuture.completedFuture(rs);
  }

  private CompletionStage<AsyncResultSet> singlePageEventRows() {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      rows.add(eventRow(i));
    }

    AsyncResultSet rs = Mockito.mock(AsyncResultSet.class);
    Mockito.when(rs.currentPage()).thenReturn(rows);

    ExecutionInfo executionInfo = Mockito.mock(ExecutionInfo.class);
    Mockito.when(executionInfo.getPagingState()).thenReturn(null);
    Mockito.when(rs.getExecutionInfo()).thenReturn(executionInfo);

    return CompletableFuture.completedFuture(rs);
  }

  private CompletionStage<AsyncResultSet> multiPageEventRows1() {
    AsyncResultSet rs = Mockito.mock(AsyncResultSet.class);

    ImmutableList<Row> rows = ImmutableList.of(eventRow(0));
    Mockito.when(rs.currentPage()).thenReturn(rows);

    ExecutionInfo executionInfo = Mockito.mock(ExecutionInfo.class);
    Mockito.when(executionInfo.getPagingState()).thenReturn(PAGING_STATE);
    Mockito.when(rs.getExecutionInfo()).thenReturn(executionInfo);

    return CompletableFuture.completedFuture(rs);
  }

  private CompletionStage<AsyncResultSet> multiPageEventRows2() {
    AsyncResultSet rs = Mockito.mock(AsyncResultSet.class);

    ImmutableList<Row> rows = ImmutableList.of(eventRow(1));
    Mockito.when(rs.currentPage()).thenReturn(rows);

    ExecutionInfo executionInfo = Mockito.mock(ExecutionInfo.class);
    Mockito.when(executionInfo.getPagingState()).thenReturn(null);
    Mockito.when(rs.getExecutionInfo()).thenReturn(executionInfo);

    return CompletableFuture.completedFuture(rs);
  }

  private Row eventRow(int i) {
    Row row = Mockito.mock(Row.class);
    Mockito.when(row.getString("activity")).thenReturn("mock activity " + i);
    Mockito.when(row.getUuid("event_id")).thenReturn(Uuids.startOf(i));
    Mockito.when(row.getInetAddress("source")).thenReturn(address);
    Mockito.when(row.getInt("source_elapsed")).thenReturn(i);
    Mockito.when(row.getString("thread")).thenReturn("mock thread " + i);
    return row;
  }

  private void assertSessionQuery(SimpleStatement statement) {
    assertThat(statement.getQuery())
        .isEqualTo("SELECT * FROM system_traces.sessions WHERE session_id = ?");
    assertThat(statement.getPositionalValues()).containsOnly(TRACING_ID);
    assertThat(statement.getConfigProfile()).isEqualTo(traceConfig);
  }

  private void assertEventsQuery(SimpleStatement statement) {
    assertThat(statement.getQuery())
        .isEqualTo("SELECT * FROM system_traces.events WHERE session_id = ?");
    assertThat(statement.getPositionalValues()).containsOnly(TRACING_ID);
    assertThat(statement.getConfigProfile()).isEqualTo(traceConfig);
  }
}
