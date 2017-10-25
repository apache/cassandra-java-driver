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
package com.datastax.oss.driver.internal.core.loadbalancing;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.google.common.collect.ImmutableList;
import java.net.InetSocketAddress;
import java.util.function.Predicate;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public abstract class DefaultLoadBalancingPolicyTestBase {

  protected static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  protected static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);
  protected static final InetSocketAddress ADDRESS3 = new InetSocketAddress("127.0.0.3", 9042);
  protected static final InetSocketAddress ADDRESS4 = new InetSocketAddress("127.0.0.4", 9042);
  protected static final InetSocketAddress ADDRESS5 = new InetSocketAddress("127.0.0.5", 9042);

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock protected Node node1;
  @Mock protected Node node2;
  @Mock protected Node node3;
  @Mock protected Node node4;
  @Mock protected Node node5;
  @Mock protected InternalDriverContext context;
  @Mock protected Predicate<Node> filter;
  @Mock protected LoadBalancingPolicy.DistanceReporter distanceReporter;
  @Mock protected Appender<ILoggingEvent> appender;

  @Captor protected ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  protected Logger logger;

  @Before
  public void setup() {
    Mockito.when(filter.test(any(Node.class))).thenReturn(true);
    logger = (Logger) LoggerFactory.getLogger(DefaultLoadBalancingPolicy.class);
    logger.addAppender(appender);

    for (Node node : ImmutableList.of(node1, node2, node3, node4, node5)) {
      Mockito.when(node.getDatacenter()).thenReturn("dc1");
    }
  }

  @After
  public void teardown() {
    logger.detachAppender(appender);
  }
}
