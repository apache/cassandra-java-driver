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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.addresstranslation.PassThroughAddressTranslator;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;

public class DefaultTopologyMonitorTest {

  private static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);

  @Mock private InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverConfigProfile defaultConfig;
  @Mock private ControlConnection controlConnection;
  @Mock private DriverChannel channel;
  private AddressTranslator addressTranslator;
  private DefaultNode node1;
  private DefaultNode node2;

  private TestTopologyMonitor topologyMonitor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    Mockito.when(defaultConfig.getDuration(CoreDriverOption.CONTROL_CONNECTION_TIMEOUT))
        .thenReturn(Duration.ofSeconds(1));
    Mockito.when(config.getDefaultProfile()).thenReturn(defaultConfig);
    Mockito.when(context.config()).thenReturn(config);

    addressTranslator =
        Mockito.spy(
            new PassThroughAddressTranslator(context, CoreDriverOption.ADDRESS_TRANSLATOR_ROOT));
    Mockito.when(context.addressTranslator()).thenReturn(addressTranslator);

    Mockito.when(channel.address()).thenReturn(ADDRESS1);
    Mockito.when(controlConnection.channel()).thenReturn(channel);
    Mockito.when(context.controlConnection()).thenReturn(controlConnection);

    node1 = new DefaultNode(ADDRESS1);
    node2 = new DefaultNode(ADDRESS2);

    topologyMonitor = new TestTopologyMonitor(context);
  }

  @Test
  public void should_initialize_control_connection() {
    // When
    topologyMonitor.init();

    // Then
    Mockito.verify(controlConnection).init(true);
  }

  @Test
  public void should_not_refresh_control_node() {
    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node1);

    // Then
    assertThat(futureInfo).isSuccess(maybeInfo -> assertThat(maybeInfo.isPresent()).isFalse());
  }

  @Test
  public void should_refresh_node_from_peers_if_broadcast_address_is_present() {
    // Given
    InetAddress broadcastAddress = ADDRESS2.getAddress();
    node2.broadcastAddress = Optional.of(broadcastAddress);
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.peers WHERE peer = :address",
            ImmutableMap.of("address", broadcastAddress),
            mockResult(mockPeersRow(2))));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThat(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
            });
  }

  @Test
  public void should_refresh_node_from_peers_if_broadcast_address_is_not_present() {
    // Given
    node2.broadcastAddress = Optional.empty();
    AdminResult.Row peer3 = mockPeersRow(3);
    AdminResult.Row peer2 = mockPeersRow(2);
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer3, peer2)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThat(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
            });
    // The rpc_address in each row should have been tried, only the last row should have been
    // converted
    Mockito.verify(peer3).getInetAddress("rpc_address");
    Mockito.verify(addressTranslator).translate(new InetSocketAddress("127.0.0.3", 9042));
    Mockito.verify(peer3, never()).getString(anyString());

    Mockito.verify(peer2).getInetAddress("rpc_address");
    Mockito.verify(addressTranslator).translate(new InetSocketAddress("127.0.0.2", 9042));
    Mockito.verify(peer2).getString("data_center");
  }

  @Test
  public void should_get_new_node_from_peers() {
    // Given
    AdminResult.Row peer3 = mockPeersRow(3);
    AdminResult.Row peer2 = mockPeersRow(2);
    AdminResult.Row peer1 = mockPeersRow(1);
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer3, peer2, peer1)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.getNewNodeInfo(ADDRESS1);

    // Then
    assertThat(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc1");
            });
    // The rpc_address in each row should have been tried, only the last row should have been
    // converted
    Mockito.verify(peer3).getInetAddress("rpc_address");
    Mockito.verify(addressTranslator).translate(new InetSocketAddress("127.0.0.3", 9042));
    Mockito.verify(peer3, never()).getString(anyString());

    Mockito.verify(peer2).getInetAddress("rpc_address");
    Mockito.verify(addressTranslator).translate(new InetSocketAddress("127.0.0.2", 9042));
    Mockito.verify(peer2, never()).getString(anyString());

    Mockito.verify(peer1).getInetAddress("rpc_address");
    Mockito.verify(addressTranslator).translate(new InetSocketAddress("127.0.0.1", 9042));
    Mockito.verify(peer1).getString("data_center");
  }

  @Test
  public void should_refresh_node_list_from_local_and_peers() {
    // Given
    AdminResult.Row peer3 = mockPeersRow(3);
    AdminResult.Row peer2 = mockPeersRow(2);
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.local", mockResult(mockLocalRow(1))),
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer3, peer2)));

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThat(futureInfos)
        .isSuccess(
            infos -> {
              Iterator<NodeInfo> iterator = infos.iterator();
              NodeInfo info1 = iterator.next();
              assertThat(info1.getConnectAddress()).isEqualTo(ADDRESS1);
              assertThat(info1.getDatacenter()).isEqualTo("dc1");
              NodeInfo info3 = iterator.next();
              assertThat(info3.getConnectAddress())
                  .isEqualTo(new InetSocketAddress("127.0.0.3", 9042));
              assertThat(info3.getDatacenter()).isEqualTo("dc3");
              NodeInfo info2 = iterator.next();
              assertThat(info2.getConnectAddress())
                  .isEqualTo(new InetSocketAddress("127.0.0.2", 9042));
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
    assertThat(futureInfos)
        .isFailed(error -> assertThat(error).isInstanceOf(IllegalStateException.class));
  }

  /** Mocks the query execution logic. */
  private static class TestTopologyMonitor extends DefaultTopologyMonitor {

    private final Queue<StubbedQuery> queries = new LinkedList<>();

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
      return CompletableFuture.completedFuture(nextQuery.result);
    }
  }

  private static class StubbedQuery {
    private final String queryString;
    private final Map<String, Object> parameters;
    private final AdminResult result;

    private StubbedQuery(String queryString, Map<String, Object> parameters, AdminResult result) {
      this.queryString = queryString;
      this.parameters = parameters;
      this.result = result;
    }

    private StubbedQuery(String queryString, AdminResult result) {
      this(queryString, Collections.emptyMap(), result);
    }
  }

  private AdminResult.Row mockLocalRow(int i) {
    try {
      AdminResult.Row row = Mockito.mock(AdminResult.Row.class);
      Mockito.when(row.getInetAddress("broadcast_address"))
          .thenReturn(InetAddress.getByName("127.0.0." + i));
      Mockito.when(row.getString("data_center")).thenReturn("dc" + i);
      Mockito.when(row.getInetAddress("listen_address"))
          .thenReturn(InetAddress.getByName("127.0.0." + i));
      Mockito.when(row.getString("rack")).thenReturn("rack" + i);
      Mockito.when(row.getString("release_version")).thenReturn("release_version" + i);

      // The driver should not use this column for the local row, because it can contain the
      // non-broadcast RPC address. Simulate the bug to ensure it's handled correctly.
      Mockito.when(row.getInetAddress("rpc_address")).thenReturn(InetAddress.getByName("0.0.0.0"));

      Mockito.when(row.getSetOfString("tokens")).thenReturn(ImmutableSet.of("token" + i));
      return row;
    } catch (UnknownHostException e) {
      fail("unexpected", e);
      return null;
    }
  }

  private AdminResult.Row mockPeersRow(int i) {
    try {
      AdminResult.Row row = Mockito.mock(AdminResult.Row.class);
      Mockito.when(row.getInetAddress("peer")).thenReturn(InetAddress.getByName("127.0.0." + i));
      Mockito.when(row.getString("data_center")).thenReturn("dc" + i);
      Mockito.when(row.getString("rack")).thenReturn("rack" + i);
      Mockito.when(row.getString("release_version")).thenReturn("release_version" + i);
      Mockito.when(row.getInetAddress("rpc_address"))
          .thenReturn(InetAddress.getByName("127.0.0." + i));
      Mockito.when(row.getSetOfString("tokens")).thenReturn(ImmutableSet.of("token" + i));
      return row;
    } catch (UnknownHostException e) {
      fail("unexpected", e);
      return null;
    }
  }

  private AdminResult mockResult(AdminResult.Row... rows) {
    AdminResult result = Mockito.mock(AdminResult.class);
    Mockito.when(result.iterator()).thenReturn(Iterators.forArray(rows));
    return result;
  }
}
