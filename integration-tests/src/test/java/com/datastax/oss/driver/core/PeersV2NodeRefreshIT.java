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
package com.datastax.oss.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import java.util.concurrent.ExecutionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test for JAVA-2654. */
public class PeersV2NodeRefreshIT {

  private static Server peersV2Server;
  private static BoundCluster cluster;

  @BeforeClass
  public static void setup() {
    peersV2Server = Server.builder().withMultipleNodesPerIp(true).build();
    cluster = peersV2Server.register(ClusterSpec.builder().withNodes(2));
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stop();
    }
    if (peersV2Server != null) {
      peersV2Server.close();
    }
  }

  @Test
  public void should_successfully_send_peers_v2_node_refresh_query()
      throws InterruptedException, ExecutionException {
    CqlSession session =
        CqlSession.builder().addContactPoint(cluster.node(1).inetSocketAddress()).build();
    Node node = findNonControlNode(session);
    ((InternalDriverContext) session.getContext())
        .getMetadataManager()
        .refreshNode(node)
        .toCompletableFuture()
        .get();
    assertThat(hasNodeRefreshQuery())
        .describedAs("Expecting peers_v2 node refresh query to be present but it wasn't")
        .isTrue();
  }

  private Node findNonControlNode(CqlSession session) {
    EndPoint controlNode =
        ((InternalDriverContext) session.getContext())
            .getControlConnection()
            .channel()
            .getEndPoint();
    return session.getMetadata().getNodes().values().stream()
        .filter(node -> !node.getEndPoint().equals(controlNode))
        .findAny()
        .orElseThrow(() -> new IllegalStateException("Expecting at least one non-control node"));
  }

  private boolean hasNodeRefreshQuery() {
    for (QueryLog log : cluster.getLogs().getQueryLogs()) {
      if (log.getFrame().message instanceof Query) {
        if (((Query) log.getFrame().message)
            .query.contains(
                "SELECT * FROM system.peers_v2 WHERE peer = :address and peer_port = :port")) {
          return true;
        }
      }
    }
    return false;
  }
}
