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

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public abstract class DefaultLoadBalancingPolicyTestBase {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock protected DefaultNode node1;
  @Mock protected DefaultNode node2;
  @Mock protected DefaultNode node3;
  @Mock protected DefaultNode node4;
  @Mock protected DefaultNode node5;
  @Mock protected InternalDriverContext context;
  @Mock protected DriverConfig config;
  @Mock protected DriverExecutionProfile defaultProfile;
  @Mock protected LoadBalancingPolicy.DistanceReporter distanceReporter;
  @Mock protected Appender<ILoggingEvent> appender;
  @Mock protected MetadataManager metadataManager;

  @Captor protected ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  protected Logger logger;

  @Before
  public void setup() {
    when(context.getSessionName()).thenReturn("test");
    when(context.getConfig()).thenReturn(config);
    when(config.getProfile(DriverExecutionProfile.DEFAULT_NAME)).thenReturn(defaultProfile);

    when(defaultProfile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null))
        .thenReturn("dc1");

    when(context.getMetadataManager()).thenReturn(metadataManager);

    logger = (Logger) LoggerFactory.getLogger(DefaultLoadBalancingPolicy.class);
    logger.addAppender(appender);

    for (Node node : ImmutableList.of(node1, node2, node3, node4, node5)) {
      when(node.getDatacenter()).thenReturn("dc1");
    }

    when(context.getLocalDatacenter(anyString())).thenReturn(null);
  }

  @After
  public void teardown() {
    logger.detachAppender(appender);
  }
}
