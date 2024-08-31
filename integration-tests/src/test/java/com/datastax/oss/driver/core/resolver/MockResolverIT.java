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
import static org.junit.Assert.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.resolver.ResolverProvider;
import com.datastax.oss.driver.internal.core.resolver.mockResolver.MockResolverFactory;
import com.datastax.oss.driver.internal.core.resolver.mockResolver.ValidResponse;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IsolatedTests.class)
public class MockResolverIT {

  private static final Logger LOG = LoggerFactory.getLogger(MockResolverIT.class);

  @ClassRule
  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(1).build();

  @Test
  public void should_connect_with_mocked_hostname() {
    CcmBridge ccmBridge = CCM_RULE.getCcmBridge();

    MockResolverFactory resolverFactory = new MockResolverFactory();
    resolverFactory.updateResponse(
        "node-1.cluster.fake",
        new ValidResponse(new InetAddress[] {getNodeInetAddress(ccmBridge, 1)}));
    ResolverProvider.setDefaultResolverFactory(resolverFactory);

    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
            .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
            .withStringList(
                TypedDriverOption.CONTACT_POINTS.getRawOption(),
                Collections.singletonList("node-1.cluster.fake:9042"))
            .build();

    CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
    try (CqlSession session = builder.build()) {
      ResultSet rs = session.execute("SELECT * FROM system.local");
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
              .filter(x -> x.toString().contains("node-1.cluster.fake"))
              .collect(Collectors.toSet());
      assertThat(filteredNodes).hasSize(1);
      InetSocketAddress address =
          (InetSocketAddress) filteredNodes.iterator().next().getEndPoint().resolve();
      assertTrue(address.isUnresolved());
    }
  }

  private InetAddress getNodeInetAddress(CcmBridge ccmBridge, int nodeid) {
    try {
      return InetAddress.getByName(ccmBridge.getNodeIpAddress(nodeid));
    } catch (UnknownHostException e) {
      return null;
    }
  }
}
