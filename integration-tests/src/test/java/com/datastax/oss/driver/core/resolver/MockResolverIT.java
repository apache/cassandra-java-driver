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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.core.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IsolatedTests.class)
public class MockResolverIT {

  private static final Logger LOG = LoggerFactory.getLogger(MockResolverIT.class);

  private static final int CLUSTER_WAIT_SECONDS =
      60; // Maximal wait time for cluster nodes to get up

  @Test
  public void should_connect_with_mocked_hostname() {
    CcmBridge.Builder ccmBridgeBuilder = CcmBridge.builder().withNodes(1);
    try (CcmBridge ccmBridge = ccmBridgeBuilder.build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      ccmBridge.create();
      ccmBridge.start();

      DriverConfigLoader loader =
          new DefaultProgrammaticDriverConfigLoaderBuilder()
              .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
              .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
              .withStringList(
                  TypedDriverOption.CONTACT_POINTS.getRawOption(),
                  Collections.singletonList("test.cluster.fake:9042"))
              .build();

      CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
      try (CqlSession session = builder.build()) {
        ResultSet rs = session.execute("select * from system.local where key='local'");
        List<Row> rows = rs.all();
        assertThat(rows).hasSize(1);
        LOG.trace("system.local contents: {}", rows.get(0).getFormattedContents());
        Collection<Node> nodes = session.getMetadata().getNodes().values();
        for (Node node : nodes) {
          LOG.trace("Found metadata node: {}", node);
        }
        Set<Node> filteredNodes;
        filteredNodes =
            nodes.stream()
                .filter(x -> x.toString().contains("test.cluster.fake"))
                .collect(Collectors.toSet());
        assertThat(filteredNodes).hasSize(1);
        InetSocketAddress address =
            (InetSocketAddress) filteredNodes.iterator().next().getEndPoint().resolve();
        assertTrue(address.isUnresolved());
      }
    }
  }

  @Test
  public void replace_cluster_test() {
    final int numberOfNodes = 3;
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
            .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
            .withStringList(
                TypedDriverOption.CONTACT_POINTS.getRawOption(),
                Collections.singletonList("test.cluster.fake:9042"))
            .build();

    CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
    CqlSession session;

    try (CcmBridge ccmBridge =
        CcmBridge.builder().withNodes(numberOfNodes).withIpPrefix("127.0.1.").build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
      ccmBridge.create();
      ccmBridge.start();
      session = builder.build();
      final CqlSession firstSession = session;
      try {
        await()
            .atMost(Duration.ofSeconds(CLUSTER_WAIT_SECONDS))
            .pollInterval(Duration.ofSeconds(1))
            .until(() -> countUpNodes(firstSession) == numberOfNodes);
      } catch (org.awaitility.core.ConditionTimeoutException e) {
        LOG.error(
            "Driver sees only {} nodes UP instead of {} after waiting {}s",
            countUpNodes(firstSession),
            numberOfNodes,
            CLUSTER_WAIT_SECONDS);
      }
      ResultSet rs = session.execute("select * from system.local where key='local'");
      assertThat(rs).isNotNull();
      Row row = rs.one();
      assertThat(row).isNotNull();
      Collection<Node> nodes = session.getMetadata().getNodes().values();
      assertThat(nodes).hasSize(numberOfNodes);
      Iterator<Node> iterator = nodes.iterator();
      while (iterator.hasNext()) {
        LOG.trace("Metadata node: " + iterator.next().toString());
      }
      Set<Node> filteredNodes;
      filteredNodes =
          nodes.stream()
              .filter(x -> x.toString().contains("test.cluster.fake"))
              .collect(Collectors.toSet());
      assertThat(filteredNodes).hasSize(1);
    }
    try (CcmBridge ccmBridge =
        CcmBridge.builder().withNodes(numberOfNodes).withIpPrefix("127.0.1.").build()) {
      ccmBridge.create();
      ccmBridge.start();
      try {
        await()
            .atMost(Duration.ofSeconds(CLUSTER_WAIT_SECONDS))
            .pollInterval(Duration.ofSeconds(1))
            .until(() -> countUpNodes(session) == numberOfNodes);
      } catch (org.awaitility.core.ConditionTimeoutException e) {
        LOG.error(
            "Driver sees only {} nodes UP instead of {} after waiting {}s",
            countUpNodes(session),
            numberOfNodes,
            CLUSTER_WAIT_SECONDS);
      }
      ResultSet rs = session.execute("select * from system.local where key='local'");
      assertThat(rs).isNotNull();
      Row row = rs.one();
      assertThat(row).isNotNull();

      Collection<Node> nodes = session.getMetadata().getNodes().values();
      assertThat(nodes).hasSize(numberOfNodes);
      Iterator<Node> iterator = nodes.iterator();
      while (iterator.hasNext()) {
        LOG.trace("Metadata node: " + iterator.next().toString());
      }
      Set<Node> filteredNodes;
      filteredNodes =
          nodes.stream()
              .filter(x -> x.toString().contains("test.cluster.fake"))
              .collect(Collectors.toSet());
      if (filteredNodes.size() == 0) {
        LOG.error(
            "No metadata node with \"test.cluster.fake\" substring. The unresolved endpoint socket was likely "
                + "replaced with resolved one.");
      } else if (filteredNodes.size() > 1) {
        fail(
            "Somehow there is more than 1 node in metadata with unresolved hostname. This should not ever happen.");
      }
    }
    session.close();
  }

  @SuppressWarnings("unused")
  public void run_replace_test_20_times() {
    for (int i = 1; i <= 20; i++) {
      LOG.info(
          "Running ({}/20}) {}", i, MockResolverIT.class.toString() + "#replace_cluster_test()");
      replace_cluster_test();
    }
  }

  // This is too long to run during CI, but is useful for manual investigations.
  @SuppressWarnings("unused")
  public void cannot_reconnect_with_resolved_socket() {
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
            .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
            .withStringList(
                TypedDriverOption.CONTACT_POINTS.getRawOption(),
                Collections.singletonList("test.cluster.fake:9042"))
            .build();

    CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
    CqlSession session;
    Collection<Node> nodes;
    Set<Node> filteredNodes;
    try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(3).build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
      ccmBridge.create();
      ccmBridge.start();
      session = builder.build();
      final CqlSession firstUnusedSession = session;
      awaitAllNodesUp(firstUnusedSession, 3);
      ResultSet rs = session.execute("select * from system.local where key='local'");
      assertThat(rs).isNotNull();
      Row row = rs.one();
      assertThat(row).isNotNull();
      nodes = session.getMetadata().getNodes().values();
      assertThat(nodes).hasSize(3);
      Iterator<Node> iterator = nodes.iterator();
      while (iterator.hasNext()) {
        LOG.trace("Metadata node: " + iterator.next().toString());
      }
      filteredNodes =
          nodes.stream()
              .filter(x -> x.toString().contains("test.cluster.fake"))
              .collect(Collectors.toSet());
      assertThat(filteredNodes).hasSize(1);
    }
    int counter = 0;
    while (filteredNodes.size() == 1) {
      counter++;
      if (counter == 255) {
        LOG.error("Completed 254 runs. Breaking.");
        break;
      }
      LOG.warn(
          "Launching another cluster until we lose resolved socket from metadata (run {}).",
          counter);
      try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(3).build()) {
        MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
        MultimapHostResolverProvider.addResolverEntry(
            "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
        MultimapHostResolverProvider.addResolverEntry(
            "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
        MultimapHostResolverProvider.addResolverEntry(
            "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
        ccmBridge.create();
        ccmBridge.start();
        awaitAllNodesUp(session, 3);
        nodes = session.getMetadata().getNodes().values();
        assertThat(nodes).hasSize(3);
        Iterator<Node> iterator = nodes.iterator();
        while (iterator.hasNext()) {
          LOG.trace("Metadata node: " + iterator.next().toString());
        }
        filteredNodes =
            nodes.stream()
                .filter(x -> x.toString().contains("test.cluster.fake"))
                .collect(Collectors.toSet());
        if (filteredNodes.size() > 1) {
          fail(
              "Somehow there is more than 1 node in metadata with unresolved hostname. This should not ever happen.");
        }
      }
    }
    Iterator<Node> iterator = nodes.iterator();
    while (iterator.hasNext()) {
      InetSocketAddress address = (InetSocketAddress) iterator.next().getEndPoint().resolve();
      assertFalse(address.isUnresolved());
    }
    try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(3).build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
      // Now the driver should fail to reconnect since unresolved hostname is gone.
      ccmBridge.create();
      ccmBridge.start();
      awaitAllNodesUp(session, 3);
      session.execute("select * from system.local where key='local'");
    }
    session.close();
  }

  private static int countUpNodes(CqlSession session) {
    int count = 0;
    for (Node node : session.getMetadata().getNodes().values()) {
      if (node.getUpSinceMillis() > 0) {
        count++;
      }
    }
    return count;
  }

  private static void awaitAllNodesUp(CqlSession session, int expectedNodes) {
    try {
      await()
          .atMost(Duration.ofSeconds(CLUSTER_WAIT_SECONDS))
          .pollInterval(Duration.ofSeconds(1))
          .until(() -> countUpNodes(session) == expectedNodes);
    } catch (org.awaitility.core.ConditionTimeoutException e) {
      LOG.error(
          "Driver sees only {} nodes UP instead of {} after waiting {}s",
          countUpNodes(session),
          expectedNodes,
          CLUSTER_WAIT_SECONDS);
    }
  }
}
