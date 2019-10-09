/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.loadbalancing;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER;
import static com.datastax.oss.driver.api.core.config.DriverExecutionProfile.DEFAULT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.dse.driver.internal.core.tracker.MultiplexingRequestTracker;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import java.util.function.Predicate;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public abstract class DseLoadBalancingPolicyTestBase {

  @Mock DefaultNode node1;
  @Mock DefaultNode node2;
  @Mock DefaultNode node3;
  @Mock InternalDriverContext context;
  @Mock DriverConfig config;
  @Mock DriverExecutionProfile profile;
  @Mock Predicate<Node> filter;
  @Mock LoadBalancingPolicy.DistanceReporter distanceReporter;
  @Mock Appender<ILoggingEvent> appender;
  @Mock Request request;
  @Mock MetadataManager metadataManager;
  final String logPrefix = "lbp-test-log-prefix";

  @Captor ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  private Logger logger;

  @Before
  public void setUp() {
    logger = (Logger) LoggerFactory.getLogger(DseLoadBalancingPolicy.class);
    logger.addAppender(appender);
    given(node1.getDatacenter()).willReturn("dc1");
    given(node2.getDatacenter()).willReturn("dc1");
    given(node3.getDatacenter()).willReturn("dc1");
    given(filter.test(any(Node.class))).willReturn(true);
    given(context.getNodeFilter(DEFAULT_NAME)).willReturn(filter);
    given(context.getConfig()).willReturn(config);
    given(config.getProfile(DEFAULT_NAME)).willReturn(profile);
    given(profile.getString(LOAD_BALANCING_LOCAL_DATACENTER, null)).willReturn("dc1");
    given(context.getMetadataManager()).willReturn(metadataManager);
    given(context.getRequestTracker()).willReturn(new MultiplexingRequestTracker());
  }

  @After
  public void tearDown() {
    logger.detachAppender(appender);
  }
}
