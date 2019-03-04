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
package com.datastax.oss.driver.internal.core.metadata;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.addresstranslation.PassThroughAddressTranslator;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DefaultTopologyMonitorTest {

  private static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);

  @Mock private InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile defaultConfig;
  @Mock private ControlConnection controlConnection;
  @Mock private DriverChannel channel;
  @Mock protected MetricsFactory metricsFactory;

  private AddressTranslator addressTranslator;
  private DefaultNode node1;
  private DefaultNode node2;

  private TestTopologyMonitor topologyMonitor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(context.getMetricsFactory()).thenReturn(metricsFactory);

    node1 = TestNodeFactory.newNode(1, context);
    node2 = TestNodeFactory.newNode(2, context);

    when(defaultConfig.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT))
        .thenReturn(Duration.ofSeconds(1));
    when(config.getDefaultProfile()).thenReturn(defaultConfig);
    when(context.getConfig()).thenReturn(config);

    addressTranslator = spy(new PassThroughAddressTranslator(context));
    when(context.getAddressTranslator()).thenReturn(addressTranslator);

    when(channel.getEndPoint()).thenReturn(node1.getEndPoint());
    when(controlConnection.channel()).thenReturn(channel);
    when(context.getControlConnection()).thenReturn(controlConnection);

    topologyMonitor = new TestTopologyMonitor(context);
  }

  @Test
  public void should_initialize_control_connection() {
    // When
    topologyMonitor.init();

    // Then
    verify(controlConnection).init(true, false, true);
  }

  @Test
  public void should_not_refresh_control_node() {
    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node1);

    // Then
    assertThatStage(futureInfo).isSuccess(maybeInfo -> assertThat(maybeInfo.isPresent()).isFalse());
  }

  @Test
  public void should_refresh_node_from_peers_if_broadcast_address_is_present() {
    // Given
    node2.broadcastAddress = ADDRESS2;
    topologyMonitor.isSchemaV2 = false;
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.peers WHERE peer = :address",
            ImmutableMap.of("address", ADDRESS2.getAddress()),
            mockResult(mockPeersRow(2, node2.getHostId()))));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
            });
  }

  @Test
  public void should_refresh_node_from_peers_if_broadcast_address_is_present_v2() {
    // Given
    node2.broadcastAddress = ADDRESS2;
    topologyMonitor.isSchemaV2 = true;
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.peers_v2 WHERE peer = :address and peer_port = :port",
            ImmutableMap.of("address", ADDRESS2.getAddress(), "peer", 9042),
            mockResult(mockPeersV2Row(2, node2.getHostId()))));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
              assertThat(info.getBroadcastAddress().get().getPort()).isEqualTo(7002);
            });
  }

  @Test
  public void should_refresh_node_from_peers_if_broadcast_address_is_not_present() {
    // Given
    topologyMonitor.isSchemaV2 = false;
    node2.broadcastAddress = null;
    AdminRow peer3 = mockPeersRow(3, UUID.randomUUID());
    AdminRow peer2 = mockPeersRow(2, node2.getHostId());
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer3, peer2)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
            });
    // The rpc_address in each row should have been tried, only the last row should have been
    // converted
    verify(peer3).getUuid("host_id");
    verify(peer3, never()).getString(anyString());

    verify(peer2, times(2)).getUuid("host_id");
    verify(peer2).getString("data_center");
  }

  @Test
  public void should_refresh_node_from_peers_if_broadcast_address_is_not_present_V2() {
    // Given
    topologyMonitor.isSchemaV2 = true;
    node2.broadcastAddress = null;
    AdminRow peer3 = mockPeersV2Row(3, UUID.randomUUID());
    AdminRow peer2 = mockPeersV2Row(2, node2.getHostId());
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.peers_v2", mockResult(peer3, peer2)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
            });
    // The host_id in each row should have been tried, only the last row should have been
    // converted
    verify(peer3).getUuid("host_id");
    verify(peer3, never()).getString(anyString());

    verify(peer2, times(2)).getUuid("host_id");
    verify(peer2).getString("data_center");
  }

  @Test
  public void should_get_new_node_from_peers() {
    // Given
    AdminRow peer3 = mockPeersRow(3, UUID.randomUUID());
    AdminRow peer2 = mockPeersRow(2, node2.getHostId());
    AdminRow peer1 = mockPeersRow(1, node1.getHostId());
    topologyMonitor.isSchemaV2 = false;
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer3, peer2, peer1)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.getNewNodeInfo(ADDRESS1);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc1");
            });
    // The rpc_address in each row should have been tried, only the last row should have been
    // converted
    verify(peer3).getInetAddress("rpc_address");
    verify(peer3, never()).getString(anyString());

    verify(peer2).getInetAddress("rpc_address");
    verify(peer2, never()).getString(anyString());

    verify(peer1).getInetAddress("rpc_address");
    verify(peer1).getString("data_center");
  }

  @Test
  public void should_get_new_node_from_peers_v2() {
    // Given
    AdminRow peer3 = mockPeersV2Row(3, UUID.randomUUID());
    AdminRow peer2 = mockPeersV2Row(2, node2.getHostId());
    AdminRow peer1 = mockPeersV2Row(1, node1.getHostId());
    topologyMonitor.isSchemaV2 = true;
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.peers_v2", mockResult(peer3, peer2, peer1)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.getNewNodeInfo(ADDRESS1);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc1");
            });
    // The natove in each row should have been tried, only the last row should have been
    // converted
    verify(peer3).getInetAddress("native_address");
    verify(peer3, never()).getString(anyString());

    verify(peer2).getInetAddress("native_address");
    verify(peer2, never()).getString(anyString());

    verify(peer1).getInetAddress("native_address");
    verify(peer1).getString("data_center");
  }

  @Test
  public void should_refresh_node_list_from_local_and_peers() {
    // Given
    AdminRow peer3 = mockPeersRow(3, UUID.randomUUID());
    AdminRow peer2 = mockPeersRow(2, node2.getHostId());
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.local", mockResult(mockLocalRow(1))),
        new StubbedQuery(
            "SELECT * FROM system.peers_v2",
            Collections.emptyMap(),
            mockResult(peer3, peer2),
            true),
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer3, peer2)));

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThatStage(futureInfos)
        .isSuccess(
            infos -> {
              Iterator<NodeInfo> iterator = infos.iterator();
              NodeInfo info1 = iterator.next();
              assertThat(info1.getEndPoint()).isEqualTo(node1.getEndPoint());
              assertThat(info1.getDatacenter()).isEqualTo("dc1");
              NodeInfo info3 = iterator.next();
              assertThat(info3.getEndPoint().resolve())
                  .isEqualTo(new InetSocketAddress("127.0.0.3", 9042));
              assertThat(info3.getDatacenter()).isEqualTo("dc3");
              NodeInfo info2 = iterator.next();
              assertThat(info2.getEndPoint()).isEqualTo(node2.getEndPoint());
              assertThat(info2.getDatacenter()).isEqualTo("dc2");
            });
  }

  @Test
  public void should_stop_executing_queries_once_closed() throws Exception {
    // Given
    topologyMonitor.close();

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThatStage(futureInfos)
        .isFailed(error -> assertThat(error).isInstanceOf(IllegalStateException.class));
  }

  /** Mocks the query execution logic. */
  private static class TestTopologyMonitor extends DefaultTopologyMonitor {

    private final Queue<StubbedQuery> queries = new ArrayDeque<>();

    private TestTopologyMonitor(InternalDriverContext context) {
      super(context);
      port = 9042;
    }

    private void stubQueries(StubbedQuery... queries) {
      this.queries.addAll(Arrays.asList(queries));
    }

    @Override
    protected CompletionStage<AdminResult> query(
        DriverChannel channel, String queryString, Map<String, Object> parameters) {
      StubbedQuery nextQuery = queries.poll();
      assertThat(nextQuery).isNotNull();
      assertThat(nextQuery.queryString).isEqualTo(queryString);
      assertThat(nextQuery.parameters).isEqualTo(parameters);
      if (nextQuery.error) {
        new CompletableFuture<AdminResult>().completeExceptionally(new Exception("PlaceHolder"));
      }
      return CompletableFuture.completedFuture(nextQuery.result);
    }
  }

  private static class StubbedQuery {
    private final String queryString;
    private final Map<String, Object> parameters;
    private final AdminResult result;
    private final boolean error;

    private StubbedQuery(
        String queryString, Map<String, Object> parameters, AdminResult result, boolean error) {
      this.queryString = queryString;
      this.parameters = parameters;
      this.result = result;
      this.error = error;
    }

    private StubbedQuery(String queryString, Map<String, Object> parameters, AdminResult result) {
      this(queryString, parameters, result, false);
    }

    private StubbedQuery(String queryString, AdminResult result) {
      this(queryString, Collections.emptyMap(), result);
    }

    private CompletionStage<AdminResult> throwException() throws Exception {
      throw new Exception("Placeholder");
    }
  }

  private AdminRow mockLocalRow(int i) {
    try {
      AdminRow row = mock(AdminRow.class);
      when(row.getInetAddress("broadcast_address"))
          .thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.getString("data_center")).thenReturn("dc" + i);
      when(row.getInetAddress("listen_address")).thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.getString("rack")).thenReturn("rack" + i);
      when(row.getString("release_version")).thenReturn("release_version" + i);

      // The driver should not use this column for the local row, because it can contain the
      // non-broadcast RPC address. Simulate the bug to ensure it's handled correctly.
      when(row.getInetAddress("rpc_address")).thenReturn(InetAddress.getByName("0.0.0.0"));

      when(row.getSetOfString("tokens")).thenReturn(ImmutableSet.of("token" + i));
      return row;
    } catch (UnknownHostException e) {
      fail("unexpected", e);
      return null;
    }
  }

  private AdminRow mockPeersRow(int i, UUID hostId) {
    try {
      AdminRow row = mock(AdminRow.class);
      when(row.getUuid("host_id")).thenReturn(hostId);
      when(row.getInetAddress("peer")).thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.getString("data_center")).thenReturn("dc" + i);
      when(row.getString("rack")).thenReturn("rack" + i);
      when(row.getString("release_version")).thenReturn("release_version" + i);
      when(row.getInetAddress("rpc_address")).thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.getSetOfString("tokens")).thenReturn(ImmutableSet.of("token" + i));
      return row;
    } catch (UnknownHostException e) {
      fail("unexpected", e);
      return null;
    }
  }

  private AdminRow mockPeersV2Row(int i, UUID hostId) {
    try {
      AdminRow row = mock(AdminRow.class);
      when(row.getUuid("host_id")).thenReturn(hostId);
      when(row.getInetAddress("peer")).thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.getInteger("peer_port")).thenReturn(7000 + i);
      when(row.getString("data_center")).thenReturn("dc" + i);
      when(row.getString("rack")).thenReturn("rack" + i);
      when(row.getString("release_version")).thenReturn("release_version" + i);
      when(row.getInetAddress("native_address")).thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.getInteger("native_port")).thenReturn(9042);
      when(row.getSetOfString("tokens")).thenReturn(ImmutableSet.of("token" + i));
      when(row.contains("peer_port")).thenReturn(true);
      when(row.contains("native_port")).thenReturn(true);
      return row;
    } catch (UnknownHostException e) {
      fail("unexpected", e);
      return null;
    }
  }

  private AdminResult mockResult(AdminRow... rows) {
    AdminResult result = mock(AdminResult.class);
    when(result.iterator()).thenReturn(Iterators.forArray(rows));
    return result;
  }

  private AdminResult errorResult() throws Exception {
    throw new Exception("Boiler plate Exception");
  }
}
