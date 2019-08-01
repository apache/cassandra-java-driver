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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;

public class DbaasTopologyMonitor extends DefaultTopologyMonitor {
  private final InetSocketAddress proxyAddr;

  public DbaasTopologyMonitor(InternalDriverContext context, InetSocketAddress proxyAddr) {
    super(context);
    this.proxyAddr = proxyAddr;
  }

  @NonNull
  @Override
  protected DefaultNodeInfo.Builder nodeInfoBuilder(
      @NonNull AdminRow row,
      @Nullable InetSocketAddress broadcastRpcAddress,
      @NonNull EndPoint localEndPoint) {
    UUID uuid = row.getUuid("host_id");

    EndPoint endPoint = new SniEndPoint(proxyAddr, uuid.toString());

    DefaultNodeInfo.Builder builder =
        DefaultNodeInfo.builder()
            .withEndPoint(endPoint)
            .withBroadcastRpcAddress(broadcastRpcAddress);
    InetAddress broadcastAddress = row.getInetAddress("broadcast_address"); // in system.local
    if (broadcastAddress == null) {
      broadcastAddress = row.getInetAddress("peer"); // in system.peers
    }
    int broadcastPort = 0;
    if (row.contains("peer_port")) {
      broadcastPort = row.getInteger("peer_port");
    }
    builder.withBroadcastAddress(new InetSocketAddress(broadcastAddress, broadcastPort));
    InetAddress listenAddress = row.getInetAddress("listen_address");
    int listen_port = 0;
    if (row.contains("listen_port")) {
      listen_port = row.getInteger("listen_port");
    }
    return builder
        .withListenAddress(new InetSocketAddress(listenAddress, listen_port))
        .withDatacenter(row.getString("data_center"))
        .withRack(row.getString("rack"))
        .withCassandraVersion(row.getString("release_version"))
        .withTokens(row.getSetOfString("tokens"))
        .withPartitioner(row.getString("partitioner"))
        .withHostId(row.getUuid("host_id"))
        .withSchemaVersion(row.getUuid("schema_version"));
  }
}
