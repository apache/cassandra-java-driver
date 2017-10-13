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
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.EventLoop;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static com.datastax.oss.driver.Assertions.assertThat;

public class ReprepareOnUpTest {
  @Mock private ChannelPool pool;
  @Mock private DriverChannel channel;
  @Mock private EventLoop eventLoop;
  @Mock private DriverConfig config;
  @Mock private DriverConfigProfile defaultConfigProfile;
  private Runnable whenPrepared;
  private CompletionStage<Void> done;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    Mockito.when(pool.next()).thenReturn(channel);
    Mockito.when(channel.eventLoop()).thenReturn(eventLoop);
    Mockito.when(eventLoop.inEventLoop()).thenReturn(true);

    Mockito.when(config.getDefaultProfile()).thenReturn(defaultConfigProfile);
    Mockito.when(defaultConfigProfile.getBoolean(CoreDriverOption.REPREPARE_CHECK_SYSTEM_TABLE))
        .thenReturn(true);
    Mockito.when(defaultConfigProfile.getDuration(CoreDriverOption.REPREPARE_TIMEOUT))
        .thenReturn(Duration.ofMillis(500));
    Mockito.when(defaultConfigProfile.getInt(CoreDriverOption.REPREPARE_MAX_STATEMENTS))
        .thenReturn(0);
    Mockito.when(defaultConfigProfile.getInt(CoreDriverOption.REPREPARE_MAX_PARALLELISM))
        .thenReturn(100);

    done = new CompletableFuture<>();
    whenPrepared = () -> ((CompletableFuture<Void>) done).complete(null);
  }

  @Test
  public void should_complete_immediately_if_no_prepared_statements() {
    // Given
    MockReprepareOnUp reprepareOnUp =
        new MockReprepareOnUp("test", pool, getMockPayloads(/*none*/ ), config, whenPrepared);

    // When
    reprepareOnUp.start();

    // Then
    assertThat(done).isSuccess(v -> assertThat(reprepareOnUp.queries).isEmpty());
  }

  @Test
  public void should_complete_immediately_if_pool_empty() {
    // Given
    Mockito.when(pool.next()).thenReturn(null);
    MockReprepareOnUp reprepareOnUp =
        new MockReprepareOnUp("test", pool, getMockPayloads('a'), config, whenPrepared);

    // When
    reprepareOnUp.start();

    // Then
    assertThat(done).isSuccess(v -> assertThat(reprepareOnUp.queries).isEmpty());
  }

  @Test
  public void should_reprepare_all_if_system_table_query_fails() {
    MockReprepareOnUp reprepareOnUp =
        new MockReprepareOnUp(
            "test", pool, getMockPayloads('a', 'b', 'c', 'd', 'e', 'f'), config, whenPrepared);

    reprepareOnUp.start();

    MockAdminQuery adminQuery = reprepareOnUp.queries.poll();
    assertThat(adminQuery.request).isInstanceOf(Query.class);
    assertThat(((Query) adminQuery.request).query)
        .isEqualTo("SELECT prepared_id FROM system.prepared_statements");
    adminQuery.resultFuture.completeExceptionally(new RuntimeException("mock error"));

    for (char c = 'a'; c <= 'f'; c++) {
      adminQuery = reprepareOnUp.queries.poll();
      assertThat(adminQuery.request).isInstanceOf(Prepare.class);
      assertThat(((Prepare) adminQuery.request).cqlQuery).isEqualTo("mock query " + c);
      adminQuery.resultFuture.complete(null);
    }

    assertThat(done).isSuccess(v -> assertThat(reprepareOnUp.queries).isEmpty());
  }

  @Test
  public void should_reprepare_all_if_system_table_empty() {
    MockReprepareOnUp reprepareOnUp =
        new MockReprepareOnUp(
            "test", pool, getMockPayloads('a', 'b', 'c', 'd', 'e', 'f'), config, whenPrepared);

    reprepareOnUp.start();

    MockAdminQuery adminQuery = reprepareOnUp.queries.poll();
    assertThat(adminQuery.request).isInstanceOf(Query.class);
    assertThat(((Query) adminQuery.request).query)
        .isEqualTo("SELECT prepared_id FROM system.prepared_statements");
    // server knows no ids:
    adminQuery.resultFuture.complete(
        new AdminResult(preparedIdRows(/*none*/ ), null, CoreProtocolVersion.DEFAULT));

    for (char c = 'a'; c <= 'f'; c++) {
      adminQuery = reprepareOnUp.queries.poll();
      assertThat(adminQuery.request).isInstanceOf(Prepare.class);
      assertThat(((Prepare) adminQuery.request).cqlQuery).isEqualTo("mock query " + c);
      adminQuery.resultFuture.complete(null);
    }

    assertThat(done).isSuccess(v -> assertThat(reprepareOnUp.queries).isEmpty());
  }

  @Test
  public void should_reprepare_all_if_system_query_disabled() {
    Mockito.when(defaultConfigProfile.getBoolean(CoreDriverOption.REPREPARE_CHECK_SYSTEM_TABLE))
        .thenReturn(false);

    MockReprepareOnUp reprepareOnUp =
        new MockReprepareOnUp(
            "test", pool, getMockPayloads('a', 'b', 'c', 'd', 'e', 'f'), config, whenPrepared);

    reprepareOnUp.start();

    MockAdminQuery adminQuery;
    for (char c = 'a'; c <= 'f'; c++) {
      adminQuery = reprepareOnUp.queries.poll();
      assertThat(adminQuery.request).isInstanceOf(Prepare.class);
      assertThat(((Prepare) adminQuery.request).cqlQuery).isEqualTo("mock query " + c);
      adminQuery.resultFuture.complete(null);
    }

    assertThat(done).isSuccess(v -> assertThat(reprepareOnUp.queries).isEmpty());
  }

  @Test
  public void should_not_reprepare_already_known_statements() {
    MockReprepareOnUp reprepareOnUp =
        new MockReprepareOnUp(
            "test", pool, getMockPayloads('a', 'b', 'c', 'd', 'e', 'f'), config, whenPrepared);

    reprepareOnUp.start();

    MockAdminQuery adminQuery = reprepareOnUp.queries.poll();
    assertThat(adminQuery.request).isInstanceOf(Query.class);
    assertThat(((Query) adminQuery.request).query)
        .isEqualTo("SELECT prepared_id FROM system.prepared_statements");
    // server knows d, e and f already:
    adminQuery.resultFuture.complete(
        new AdminResult(preparedIdRows('d', 'e', 'f'), null, CoreProtocolVersion.DEFAULT));

    for (char c = 'a'; c <= 'c'; c++) {
      adminQuery = reprepareOnUp.queries.poll();
      assertThat(adminQuery.request).isInstanceOf(Prepare.class);
      assertThat(((Prepare) adminQuery.request).cqlQuery).isEqualTo("mock query " + c);
      adminQuery.resultFuture.complete(null);
    }

    assertThat(done).isSuccess(v -> assertThat(reprepareOnUp.queries).isEmpty());
  }

  @Test
  public void should_limit_number_of_statements_to_reprepare() {
    Mockito.when(defaultConfigProfile.getInt(CoreDriverOption.REPREPARE_MAX_STATEMENTS))
        .thenReturn(3);

    MockReprepareOnUp reprepareOnUp =
        new MockReprepareOnUp(
            "test", pool, getMockPayloads('a', 'b', 'c', 'd', 'e', 'f'), config, whenPrepared);

    reprepareOnUp.start();

    MockAdminQuery adminQuery = reprepareOnUp.queries.poll();
    assertThat(adminQuery.request).isInstanceOf(Query.class);
    assertThat(((Query) adminQuery.request).query)
        .isEqualTo("SELECT prepared_id FROM system.prepared_statements");
    // server knows no ids:
    adminQuery.resultFuture.complete(
        new AdminResult(preparedIdRows(/*none*/ ), null, CoreProtocolVersion.DEFAULT));

    for (char c = 'a'; c <= 'c'; c++) {
      adminQuery = reprepareOnUp.queries.poll();
      assertThat(adminQuery.request).isInstanceOf(Prepare.class);
      assertThat(((Prepare) adminQuery.request).cqlQuery).isEqualTo("mock query " + c);
      adminQuery.resultFuture.complete(null);
    }

    assertThat(done).isSuccess(v -> assertThat(reprepareOnUp.queries).isEmpty());
  }

  @Test
  public void should_limit_number_of_statements_reprepared_in_parallel() {
    Mockito.when(defaultConfigProfile.getInt(CoreDriverOption.REPREPARE_MAX_PARALLELISM))
        .thenReturn(3);

    MockReprepareOnUp reprepareOnUp =
        new MockReprepareOnUp(
            "test", pool, getMockPayloads('a', 'b', 'c', 'd', 'e', 'f'), config, whenPrepared);

    reprepareOnUp.start();

    MockAdminQuery adminQuery = reprepareOnUp.queries.poll();
    assertThat(adminQuery.request).isInstanceOf(Query.class);
    assertThat(((Query) adminQuery.request).query)
        .isEqualTo("SELECT prepared_id FROM system.prepared_statements");
    // server knows no ids => will reprepare all 6:
    adminQuery.resultFuture.complete(
        new AdminResult(preparedIdRows(/*none*/ ), null, CoreProtocolVersion.DEFAULT));

    // 3 statements have enqueued, we've not completed the queries yet so no more should be sent:
    assertThat(reprepareOnUp.queries.size()).isEqualTo(3);

    // As we complete each statement, another one should enqueue:
    for (char c = 'a'; c <= 'c'; c++) {
      adminQuery = reprepareOnUp.queries.poll();
      assertThat(adminQuery.request).isInstanceOf(Prepare.class);
      assertThat(((Prepare) adminQuery.request).cqlQuery).isEqualTo("mock query " + c);
      adminQuery.resultFuture.complete(null);
      assertThat(reprepareOnUp.queries.size()).isEqualTo(3);
    }

    // Complete the last 3:
    for (char c = 'd'; c <= 'f'; c++) {
      adminQuery = reprepareOnUp.queries.poll();
      assertThat(adminQuery.request).isInstanceOf(Prepare.class);
      assertThat(((Prepare) adminQuery.request).cqlQuery).isEqualTo("mock query " + c);
      adminQuery.resultFuture.complete(null);
    }

    assertThat(done).isSuccess(v -> assertThat(reprepareOnUp.queries).isEmpty());
  }

  private Map<ByteBuffer, RepreparePayload> getMockPayloads(char... values) {
    ImmutableMap.Builder<ByteBuffer, RepreparePayload> builder = ImmutableMap.builder();
    for (char value : values) {
      ByteBuffer id = Bytes.fromHexString("0x0" + value);
      builder.put(
          id, new RepreparePayload(id, "mock query " + value, null, Collections.emptyMap()));
    }
    return builder.build();
  }

  /** Bypasses the channel to make testing easier. */
  private static class MockReprepareOnUp extends ReprepareOnUp {

    private Queue<MockAdminQuery> queries = new LinkedList<>();

    MockReprepareOnUp(
        String logPrefix,
        ChannelPool pool,
        Map<ByteBuffer, RepreparePayload> repreparePayloads,
        DriverConfig config,
        Runnable whenPrepared) {
      super(logPrefix, pool, repreparePayloads, config, whenPrepared);
    }

    @Override
    protected CompletionStage<AdminResult> queryAsync(
        Message message, Map<String, ByteBuffer> customPayload, String debugString) {
      CompletableFuture<AdminResult> resultFuture = new CompletableFuture<>();
      queries.add(new MockAdminQuery(message, resultFuture));
      return resultFuture;
    }
  }

  private static class MockAdminQuery {
    private final Message request;
    private final CompletableFuture<AdminResult> resultFuture;

    public MockAdminQuery(Message request, CompletableFuture<AdminResult> resultFuture) {
      this.request = request;
      this.resultFuture = resultFuture;
    }
  }

  private Rows preparedIdRows(char... values) {
    ColumnSpec preparedIdSpec =
        new ColumnSpec(
            "system",
            "prepared_statements",
            "prepared_id",
            0,
            RawType.PRIMITIVES.get(ProtocolConstants.DataType.BLOB));
    RowsMetadata rowsMetadata =
        new RowsMetadata(ImmutableList.of(preparedIdSpec), null, null, null);
    Queue<List<ByteBuffer>> data = new LinkedList<>();
    for (char value : values) {
      data.add(ImmutableList.of(Bytes.fromHexString("0x0" + value)));
    }
    return new DefaultRows(rowsMetadata, data);
  }
}
