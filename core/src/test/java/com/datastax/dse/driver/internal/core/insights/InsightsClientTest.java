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
package com.datastax.dse.driver.internal.core.insights;

import static com.datastax.dse.driver.api.core.DseProtocolVersion.DSE_V2;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.mockDefaultExecutionProfile;
import static com.datastax.dse.driver.internal.core.insights.ExecutionProfileMockUtil.mockNonDefaultRequestTimeoutExecutionProfile;
import static com.datastax.dse.driver.internal.core.insights.PackageUtil.DEFAULT_AUTH_PROVIDER_PACKAGE;
import static com.datastax.dse.driver.internal.core.insights.PackageUtil.DEFAULT_LOAD_BALANCING_PACKAGE;
import static com.datastax.dse.driver.internal.core.insights.PackageUtil.DEFAULT_SPECULATIVE_EXECUTION_PACKAGE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.dse.driver.internal.core.insights.configuration.InsightsConfiguration;
import com.datastax.dse.driver.internal.core.insights.schema.AuthProviderType;
import com.datastax.dse.driver.internal.core.insights.schema.Insight;
import com.datastax.dse.driver.internal.core.insights.schema.InsightMetadata;
import com.datastax.dse.driver.internal.core.insights.schema.InsightType;
import com.datastax.dse.driver.internal.core.insights.schema.InsightsPlatformInfo;
import com.datastax.dse.driver.internal.core.insights.schema.InsightsPlatformInfo.CPUS;
import com.datastax.dse.driver.internal.core.insights.schema.InsightsPlatformInfo.OS;
import com.datastax.dse.driver.internal.core.insights.schema.InsightsPlatformInfo.RuntimeAndCompileTimeVersions;
import com.datastax.dse.driver.internal.core.insights.schema.InsightsStartupData;
import com.datastax.dse.driver.internal.core.insights.schema.InsightsStatusData;
import com.datastax.dse.driver.internal.core.insights.schema.LoadBalancingInfo;
import com.datastax.dse.driver.internal.core.insights.schema.PoolSizeByHostDistance;
import com.datastax.dse.driver.internal.core.insights.schema.ReconnectionPolicyInfo;
import com.datastax.dse.driver.internal.core.insights.schema.SSL;
import com.datastax.dse.driver.internal.core.insights.schema.SessionStateForNode;
import com.datastax.dse.driver.internal.core.insights.schema.SpecificExecutionProfile;
import com.datastax.dse.driver.internal.core.insights.schema.SpeculativeExecutionInfo;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.StartupOptionsBuilder;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.PoolManager;
import com.datastax.oss.driver.shaded.guava.common.base.Suppliers;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import io.netty.channel.DefaultEventLoop;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(DataProviderRunner.class)
public class InsightsClientTest {
  private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
  private static final Map<String, Object> EMPTY_OBJECT_MAP = Collections.emptyMap();
  private static final Supplier<Long> MOCK_TIME_SUPPLIER = Suppliers.ofInstance(1L);
  private static final InsightsConfiguration INSIGHTS_CONFIGURATION =
      new InsightsConfiguration(true, 300000L, new DefaultEventLoop());

  @Test
  public void should_construct_json_event_startup_message() throws IOException {
    // given
    DefaultDriverContext context = mockDefaultDriverContext();
    PlatformInfoFinder platformInfoFinder = mock(PlatformInfoFinder.class);
    OS os = new OS("linux", "1.2", "x64");
    CPUS cpus = new CPUS(8, "intel i7");
    Map<String, RuntimeAndCompileTimeVersions> javaDeps =
        ImmutableMap.of("version", new RuntimeAndCompileTimeVersions("1.8.0", "1.8.0", false));
    Map<String, Map<String, RuntimeAndCompileTimeVersions>> runtimeInfo =
        ImmutableMap.of("java", javaDeps);
    InsightsPlatformInfo insightsPlatformInfo = new InsightsPlatformInfo(os, cpus, runtimeInfo);
    when(platformInfoFinder.getInsightsPlatformInfo()).thenReturn(insightsPlatformInfo);

    ConfigAntiPatternsFinder configAntiPatternsFinder = mock(ConfigAntiPatternsFinder.class);
    when(configAntiPatternsFinder.findAntiPatterns(any(DefaultDriverContext.class)))
        .thenReturn(
            ImmutableMap.of(
                "contactPointsMultipleDCs",
                "Contact points contain hosts from multiple data centers"));

    DataCentersFinder dataCentersFinder = mock(DataCentersFinder.class);
    when(dataCentersFinder.getDataCenters(any(DefaultDriverContext.class)))
        .thenReturn(Sets.newHashSet("dc1", "dc2"));
    ReconnectionPolicyInfoFinder reconnectionPolicyInfoFinder =
        mock(ReconnectionPolicyInfoFinder.class);
    when(reconnectionPolicyInfoFinder.getReconnectionPolicyInfo(any(), any()))
        .thenReturn(
            new ReconnectionPolicyInfo(
                "reconnection-policy-a", ImmutableMap.of("opt-a", 1), "com.datastax.dse"));

    InsightsClient insightsClient =
        new InsightsClient(
            context,
            MOCK_TIME_SUPPLIER,
            INSIGHTS_CONFIGURATION,
            platformInfoFinder,
            reconnectionPolicyInfoFinder,
            new ExecutionProfilesInfoFinder(),
            configAntiPatternsFinder,
            dataCentersFinder,
            EMPTY_STACK_TRACE);

    // when
    String startupMessage = insightsClient.createStartupMessage();
    Insight<InsightsStartupData> insight =
        new ObjectMapper()
            .readValue(startupMessage, new TypeReference<Insight<InsightsStartupData>>() {});

    // then
    assertThat(insight.getMetadata())
        .isEqualTo(
            new InsightMetadata(
                "driver.startup",
                1L,
                ImmutableMap.of("language", "java"),
                InsightType.EVENT,
                "v1"));

    InsightsStartupData insightData = insight.getInsightData();
    assertThat(insightData.getClientId()).isEqualTo("client-id");
    assertThat(insightData.getSessionId()).isNotNull();
    assertThat(insightData.getDriverName()).isEqualTo("DataStax Enterprise Java Driver");
    assertThat(insightData.getDriverVersion()).isNotEmpty();
    assertThat(insightData.getApplicationName()).isEqualTo("app-name");
    assertThat(insightData.getApplicationVersion()).isEqualTo("1.0.0");
    assertThat(insightData.isApplicationNameWasGenerated()).isEqualTo(false);
    assertThat(insightData.getContactPoints())
        .isEqualTo(ImmutableMap.of("localhost", Collections.singletonList("127.0.0.1:9999")));

    assertThat(insightData.getInitialControlConnection()).isEqualTo("127.0.0.1:10");
    assertThat(insightData.getLocalAddress()).isEqualTo("127.0.0.1");
    assertThat(insightData.getHostName()).isNotEmpty();
    assertThat(insightData.getProtocolVersion()).isEqualTo(DSE_V2.getCode());
    assertThat(insightData.getExecutionProfiles())
        .isEqualTo(
            ImmutableMap.of(
                "default",
                new SpecificExecutionProfile(
                    100,
                    new LoadBalancingInfo(
                        "LoadBalancingPolicyImpl",
                        ImmutableMap.of("localDataCenter", "local-dc", "filterFunction", true),
                        DEFAULT_LOAD_BALANCING_PACKAGE),
                    new SpeculativeExecutionInfo(
                        "SpeculativeExecutionImpl",
                        ImmutableMap.of("maxSpeculativeExecutions", 100, "delay", 20),
                        DEFAULT_SPECULATIVE_EXECUTION_PACKAGE),
                    "LOCAL_ONE",
                    "SERIAL",
                    ImmutableMap.of("source", "src-graph")),
                "non-default",
                new SpecificExecutionProfile(50, null, null, null, null, null)));
    assertThat(insightData.getPoolSizeByHostDistance())
        .isEqualTo(new PoolSizeByHostDistance(2, 1, 0));
    assertThat(insightData.getHeartbeatInterval()).isEqualTo(100);
    assertThat(insightData.getCompression()).isEqualTo("none");
    assertThat(insightData.getReconnectionPolicy())
        .isEqualTo(
            new ReconnectionPolicyInfo(
                "reconnection-policy-a", ImmutableMap.of("opt-a", 1), "com.datastax.dse"));
    assertThat(insightData.getSsl()).isEqualTo(new SSL(true, false));
    assertThat(insightData.getAuthProvider())
        .isEqualTo(new AuthProviderType("AuthProviderImpl", DEFAULT_AUTH_PROVIDER_PACKAGE));
    assertThat(insightData.getOtherOptions()).isEqualTo(EMPTY_OBJECT_MAP);
    assertThat(insightData.getPlatformInfo()).isEqualTo(insightsPlatformInfo);
    assertThat(insightData.getConfigAntiPatterns())
        .isEqualTo(
            ImmutableMap.of(
                "contactPointsMultipleDCs",
                "Contact points contain hosts from multiple data centers"));
    assertThat(insightData.getPeriodicStatusInterval()).isEqualTo(300);
    assertThat(insightData.getDataCenters()).isEqualTo(Sets.newHashSet("dc1", "dc2"));
  }

  @Test
  public void should_group_contact_points_by_host_name() {
    // given
    Set<InetSocketAddress> contactPoints =
        ImmutableSet.of(
            InetSocketAddress.createUnresolved("127.0.0.1", 8080),
            InetSocketAddress.createUnresolved("127.0.0.1", 8081),
            InetSocketAddress.createUnresolved("127.0.0.2", 8081));

    Map<String, List<String>> expected =
        ImmutableMap.of(
            "127.0.0.1",
            ImmutableList.of("127.0.0.1:8080", "127.0.0.1:8081"),
            "127.0.0.2",
            ImmutableList.of("127.0.0.2:8081"));

    // when
    Map<String, List<String>> resolvedContactPoints =
        InsightsClient.getResolvedContactPoints(contactPoints);

    // then
    assertThat(resolvedContactPoints).isEqualTo(expected);
  }

  @Test
  public void should_construct_json_event_status_message() throws IOException {
    // given
    InsightsClient insightsClient =
        new InsightsClient(
            mockDefaultDriverContext(),
            MOCK_TIME_SUPPLIER,
            INSIGHTS_CONFIGURATION,
            null,
            null,
            null,
            null,
            null,
            EMPTY_STACK_TRACE);

    // when
    String statusMessage = insightsClient.createStatusMessage();

    // then
    Insight<InsightsStatusData> insight =
        new ObjectMapper()
            .readValue(statusMessage, new TypeReference<Insight<InsightsStatusData>>() {});
    assertThat(insight.getMetadata())
        .isEqualTo(
            new InsightMetadata(
                "driver.status", 1L, ImmutableMap.of("language", "java"), InsightType.EVENT, "v1"));
    InsightsStatusData insightData = insight.getInsightData();
    assertThat(insightData.getClientId()).isEqualTo("client-id");
    assertThat(insightData.getSessionId()).isNotNull();
    assertThat(insightData.getControlConnection()).isEqualTo("127.0.0.1:10");
    assertThat(insightData.getConnectedNodes())
        .isEqualTo(
            ImmutableMap.of(
                "127.0.0.1:10", new SessionStateForNode(1, 10),
                "127.0.0.1:20", new SessionStateForNode(2, 20)));
  }

  @Test
  public void should_schedule_task_with_initial_delay() {
    // given
    final AtomicInteger counter = new AtomicInteger();
    Runnable runnable = counter::incrementAndGet;

    // when
    InsightsClient.scheduleInsightsTask(100L, Executors.newScheduledThreadPool(1), runnable);

    // then
    await().atMost(1, SECONDS).until(() -> counter.get() >= 1);
  }

  @Test
  @UseDataProvider(value = "stackTraceProvider")
  public void should_get_caller_of_create_cluster(StackTraceElement[] stackTrace, String expected) {
    // when
    String result = InsightsClient.getClusterCreateCaller(stackTrace);

    // then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void should_execute_should_send_event_check_only_once()
      throws UnknownHostException, InterruptedException {
    // given
    InsightsConfiguration insightsConfiguration = mock(InsightsConfiguration.class);
    when(insightsConfiguration.isMonitorReportingEnabled()).thenReturn(true);
    when(insightsConfiguration.getStatusEventDelayMillis()).thenReturn(10L);
    when(insightsConfiguration.getExecutor()).thenReturn(new DefaultEventLoop());

    InsightsClient insightsClient =
        new InsightsClient(
            mockDefaultDriverContext(),
            MOCK_TIME_SUPPLIER,
            insightsConfiguration,
            null,
            null,
            null,
            null,
            null,
            EMPTY_STACK_TRACE);

    // when
    insightsClient.scheduleStatusMessageSend();
    // emulate periodic calls to sendStatusMessage
    insightsClient.sendStatusMessage();
    insightsClient.sendStatusMessage();
    insightsClient.sendStatusMessage();

    // then
    verify(insightsConfiguration, times(1)).isMonitorReportingEnabled();
  }

  @DataProvider
  public static Object[][] stackTraceProvider() {
    StackTraceElement[] onlyInitCall =
        new StackTraceElement[] {
          new StackTraceElement(
              "com.datastax.oss.driver.internal.core.context.DefaultDriverContext",
              "<init>",
              "DefaultDriverContext.java",
              94),
        };

    StackTraceElement[] stackTraceElementsWithoutInitCall =
        new StackTraceElement[] {
          new StackTraceElement("java.lang.Thread", "getStackTrace", "Thread.java", 1559),
          new StackTraceElement(
              "com.datastax.driver.core.InsightsClient",
              "getClusterCreateCaller",
              "InsightsClient.java",
              302)
        };
    StackTraceElement[] stackTraceWithOneInitCall =
        new StackTraceElement[] {
          new StackTraceElement("java.lang.Thread", "getStackTrace", "Thread.java", 1559),
          new StackTraceElement(
              "com.datastax.oss.driver.internal.core.context.DefaultDriverContext",
              "<init>",
              "DefaultDriverContext.java",
              243),
        };
    StackTraceElement[] stackTraceWithOneInitCallAndCaller =
        new StackTraceElement[] {
          new StackTraceElement("java.lang.Thread", "getStackTrace", "Thread.java", 1559),
          new StackTraceElement(
              "com.datastax.oss.driver.internal.core.context.DefaultDriverContext",
              "<init>",
              "DefaultDriverContext.java",
              243),
          new StackTraceElement(
              "com.example.ActualCallerNameApp", "main", "ActualCallerNameApp.java", 1)
        };

    StackTraceElement[] stackTraceWithTwoInitCallsAndCaller =
        new StackTraceElement[] {
          new StackTraceElement("java.lang.Thread", "getStackTrace", "Thread.java", 1559),
          new StackTraceElement(
              "com.datastax.oss.driver.internal.core.context.DefaultDriverContext",
              "<init>",
              "DefaultDriverContext.java",
              243),
          new StackTraceElement(
              "com.datastax.oss.driver.api.core.session.SessionBuilder",
              "buildDefaultSessionAsync",
              "SessionBuilder.java",
              300),
          new StackTraceElement(
              "com.example.ActualCallerNameApp", "main", "ActualCallerNameApp.java", 1)
        };
    StackTraceElement[] stackTraceWithChainOfInitCalls =
        new StackTraceElement[] {
          new StackTraceElement(
              "com.datastax.oss.driver.internal.core.context.DefaultDriverContext",
              "<init>",
              "DefaultDriverContext.java",
              243),
          new StackTraceElement(
              "com.datastax.oss.driver.api.core.session.SessionBuilder",
              "buildDefaultSessionAsync",
              "SessionBuilder.java",
              332),
          new StackTraceElement(
              "com.datastax.oss.driver.api.core.session.SessionBuilder",
              "buildAsync",
              "SessionBuilder.java",
              291),
          new StackTraceElement(
              "com.datastax.oss.driver.api.core.session.SessionBuilder",
              "build",
              "SessionBuilder.java",
              306)
        };
    StackTraceElement[] stackTraceWithChainOfInitCallsAndCaller =
        new StackTraceElement[] {
          new StackTraceElement("java.lang.Thread", "getStackTrace", "Thread.java", 1559),
          new StackTraceElement(
              "com.datastax.oss.driver.internal.core.context.DefaultDriverContext",
              "<init>",
              "DefaultDriverContext.java",
              243),
          new StackTraceElement(
              "com.datastax.oss.driver.api.core.session.SessionBuilder",
              "buildContext",
              "SessionBuilder.java",
              687),
          new StackTraceElement(
              "com.datastax.oss.driver.api.core.session.SessionBuilder",
              "buildDefaultSessionAsync",
              "SessionBuilder.java",
              332),
          new StackTraceElement(
              "com.datastax.oss.driver.api.core.session.SessionBuilder",
              "buildAsync",
              "SessionBuilder.java",
              291),
          new StackTraceElement(
              "com.datastax.oss.driver.api.core.session.SessionBuilder",
              "build",
              "SessionBuilder.java",
              306),
          new StackTraceElement(
              "com.example.ActualCallerNameApp", "main", "ActualCallerNameApp.java", 8)
        };

    return new Object[][] {
      {new StackTraceElement[] {}, InsightsClient.DEFAULT_JAVA_APPLICATION},
      {stackTraceElementsWithoutInitCall, InsightsClient.DEFAULT_JAVA_APPLICATION},
      {stackTraceWithOneInitCall, InsightsClient.DEFAULT_JAVA_APPLICATION},
      {onlyInitCall, InsightsClient.DEFAULT_JAVA_APPLICATION},
      {stackTraceWithOneInitCallAndCaller, "com.example.ActualCallerNameApp"},
      {stackTraceWithTwoInitCallsAndCaller, "com.example.ActualCallerNameApp"},
      {stackTraceWithChainOfInitCalls, InsightsClient.DEFAULT_JAVA_APPLICATION},
      {stackTraceWithChainOfInitCallsAndCaller, "com.example.ActualCallerNameApp"}
    };
  }

  private DefaultDriverContext mockDefaultDriverContext() throws UnknownHostException {
    DefaultDriverContext context = mock(DefaultDriverContext.class);
    mockConnectionPools(context);
    MetadataManager manager = mock(MetadataManager.class);
    when(context.getMetadataManager()).thenReturn(manager);
    Metadata metadata = mock(Metadata.class);
    when(manager.getMetadata()).thenReturn(metadata);
    Node node = mock(Node.class);
    when(node.getExtras())
        .thenReturn(
            ImmutableMap.of(
                DseNodeProperties.DSE_VERSION, Objects.requireNonNull(Version.parse("6.0.5"))));
    when(metadata.getNodes()).thenReturn(ImmutableMap.of(UUID.randomUUID(), node));
    DriverExecutionProfile defaultExecutionProfile = mockDefaultExecutionProfile();
    DriverExecutionProfile nonDefaultExecutionProfile =
        mockNonDefaultRequestTimeoutExecutionProfile();

    Map<String, String> startupOptions = new HashMap<>();
    startupOptions.put(StartupOptionsBuilder.CLIENT_ID_KEY, "client-id");
    startupOptions.put(StartupOptionsBuilder.APPLICATION_VERSION_KEY, "1.0.0");
    startupOptions.put(StartupOptionsBuilder.APPLICATION_NAME_KEY, "app-name");
    startupOptions.put(StartupOptionsBuilder.DRIVER_VERSION_KEY, "2.x");
    startupOptions.put(StartupOptionsBuilder.DRIVER_NAME_KEY, "DataStax Enterprise Java Driver");

    when(context.getStartupOptions()).thenReturn(startupOptions);
    when(context.getProtocolVersion()).thenReturn(DSE_V2);
    DefaultNode contactPoint = mock(DefaultNode.class);
    EndPoint contactEndPoint = mock(EndPoint.class);
    when(contactEndPoint.resolve()).thenReturn(new InetSocketAddress("127.0.0.1", 9999));
    when(contactPoint.getEndPoint()).thenReturn(contactEndPoint);
    when(manager.getContactPoints()).thenReturn(ImmutableSet.of(contactPoint));

    DriverConfig driverConfig = mock(DriverConfig.class);
    when(context.getConfig()).thenReturn(driverConfig);
    Map<String, DriverExecutionProfile> profiles =
        ImmutableMap.of(
            "default", defaultExecutionProfile, "non-default", nonDefaultExecutionProfile);
    Mockito.<Map<String, ? extends DriverExecutionProfile>>when(driverConfig.getProfiles())
        .thenReturn(profiles);
    when(driverConfig.getDefaultProfile()).thenReturn(defaultExecutionProfile);

    ControlConnection controlConnection = mock(ControlConnection.class);
    DriverChannel channel = mock(DriverChannel.class);
    EndPoint controlConnectionEndpoint = mock(EndPoint.class);
    when(controlConnectionEndpoint.resolve()).thenReturn(new InetSocketAddress("127.0.0.1", 10));

    when(channel.getEndPoint()).thenReturn(controlConnectionEndpoint);
    when(channel.localAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 10));
    when(controlConnection.channel()).thenReturn(channel);
    when(context.getControlConnection()).thenReturn(controlConnection);
    return context;
  }

  private void mockConnectionPools(DefaultDriverContext driverContext) {
    Node node1 = mock(Node.class);
    EndPoint endPoint1 = mock(EndPoint.class);
    when(endPoint1.resolve()).thenReturn(new InetSocketAddress("127.0.0.1", 10));
    when(node1.getEndPoint()).thenReturn(endPoint1);
    when(node1.getOpenConnections()).thenReturn(1);
    ChannelPool channelPool1 = mock(ChannelPool.class);
    when(channelPool1.getInFlight()).thenReturn(10);

    Node node2 = mock(Node.class);
    EndPoint endPoint2 = mock(EndPoint.class);
    when(endPoint2.resolve()).thenReturn(new InetSocketAddress("127.0.0.1", 20));
    when(node2.getEndPoint()).thenReturn(endPoint2);
    when(node2.getOpenConnections()).thenReturn(2);
    ChannelPool channelPool2 = mock(ChannelPool.class);
    when(channelPool2.getInFlight()).thenReturn(20);

    Map<Node, ChannelPool> channelPools = ImmutableMap.of(node1, channelPool1, node2, channelPool2);
    PoolManager poolManager = mock(PoolManager.class);
    when(poolManager.getPools()).thenReturn(channelPools);
    when(driverContext.getPoolManager()).thenReturn(poolManager);
  }
}
