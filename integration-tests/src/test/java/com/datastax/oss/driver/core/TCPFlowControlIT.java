package com.datastax.oss.driver.core;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.request.Query;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IsolatedTests.class)
public class TCPFlowControlIT {
  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));

  private static int MAX_REQUESTS_PER_CONNECTION = 2048;

  @Test
  public void should_not_write_more_requests_to_the_socket_after_the_server_paused_reading()
      throws InterruptedException, ExecutionException {
    byte[] buffer = new byte[10240];
    Arrays.fill(buffer, (byte) 1);
    ByteBuffer buffer10Kb = ByteBuffer.wrap(buffer);

    String queryString =
        String.format("INSERT INTO table1 (id) VALUES (%s)", ByteUtils.toHexString(buffer10Kb));
    Query query = new Query(queryString, emptyList(), emptyMap(), emptyMap());

    SIMULACRON_RULE.cluster().prime(when(query).then(noRows()));

    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
                .build()) {
      List<Integer> collect =
          session.getMetadata().getNodes().values().stream()
              .map(Node::getOpenConnections)
              .collect(Collectors.toList());
      System.out.println(collect);

      SIMULACRON_RULE.cluster().pauseRead();

      // The TCP send and receive buffer size depends on the OS
      // We don't know how much data is needed to be flushed in order for OS to signal as full (and
      // Channel#isWritable return false)
      // Send 20Mb+ to each node
      List<CompletionStage<AsyncResultSet>> pendingRequests = new ArrayList<>();
      for (int i = 0; i < MAX_REQUESTS_PER_CONNECTION; i++) {
        System.out.println("i" + i);
        pendingRequests.add(session.executeAsync(SimpleStatement.newInstance(queryString)));
      }

      System.out.println("after");

      // todo assert that isWritable was false and requests are queued

      SIMULACRON_RULE.cluster().resumeRead();
      for (CompletionStage<AsyncResultSet> result : pendingRequests) {
        result.toCompletableFuture().get();
      }
    }
  }
}
