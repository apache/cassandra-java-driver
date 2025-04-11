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

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.HEARTBEAT_INTERVAL;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.PROTOCOL_COMPRESSION;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_HOSTNAME_VALIDATION;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.internal.core.insights.PackageUtil.ClassSettingDetails;
import com.datastax.dse.driver.internal.core.insights.configuration.InsightsConfiguration;
import com.datastax.dse.driver.internal.core.insights.exceptions.InsightEventFormatException;
import com.datastax.dse.driver.internal.core.insights.schema.AuthProviderType;
import com.datastax.dse.driver.internal.core.insights.schema.Insight;
import com.datastax.dse.driver.internal.core.insights.schema.InsightMetadata;
import com.datastax.dse.driver.internal.core.insights.schema.InsightType;
import com.datastax.dse.driver.internal.core.insights.schema.InsightsStartupData;
import com.datastax.dse.driver.internal.core.insights.schema.InsightsStatusData;
import com.datastax.dse.driver.internal.core.insights.schema.PoolSizeByHostDistance;
import com.datastax.dse.driver.internal.core.insights.schema.SSL;
import com.datastax.dse.driver.internal.core.insights.schema.SessionStateForNode;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.StartupOptionsBuilder;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsightsClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsightsClient.class);
  private static final String STARTUP_MESSAGE_NAME = "driver.startup";
  private static final String STATUS_MESSAGE_NAME = "driver.status";
  private static final String REPORT_INSIGHT_RPC = "CALL InsightsRpc.reportInsight(?)";
  private static final Map<String, String> TAGS = ImmutableMap.of("language", "java");
  private static final String STARTUP_VERSION_1_ID = "v1";
  private static final String STATUS_VERSION_1_ID = "v1";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final int MAX_NUMBER_OF_STATUS_ERROR_LOGS = 5;
  static final String DEFAULT_JAVA_APPLICATION = "Default Java Application";

  private final ControlConnection controlConnection;
  private final String id = Uuids.random().toString();
  private final InsightsConfiguration insightsConfiguration;
  private final AtomicInteger numberOfStatusEventErrors = new AtomicInteger();

  private final InternalDriverContext driverContext;
  private final Supplier<Long> timestampSupplier;
  private final PlatformInfoFinder platformInfoFinder;
  private final ReconnectionPolicyInfoFinder reconnectionPolicyInfoInfoFinder;
  private final ExecutionProfilesInfoFinder executionProfilesInfoFinder;
  private final ConfigAntiPatternsFinder configAntiPatternsFinder;
  private final DataCentersFinder dataCentersFinder;
  private final StackTraceElement[] initCallStackTrace;

  private volatile ScheduledFuture<?> scheduleInsightsTask;

  public static InsightsClient createInsightsClient(
      InsightsConfiguration insightsConfiguration,
      InternalDriverContext driverContext,
      StackTraceElement[] initCallStackTrace) {
    DataCentersFinder dataCentersFinder = new DataCentersFinder();
    return new InsightsClient(
        driverContext,
        () -> new Date().getTime(),
        insightsConfiguration,
        new PlatformInfoFinder(),
        new ReconnectionPolicyInfoFinder(),
        new ExecutionProfilesInfoFinder(),
        new ConfigAntiPatternsFinder(),
        dataCentersFinder,
        initCallStackTrace);
  }

  InsightsClient(
      InternalDriverContext driverContext,
      Supplier<Long> timestampSupplier,
      InsightsConfiguration insightsConfiguration,
      PlatformInfoFinder platformInfoFinder,
      ReconnectionPolicyInfoFinder reconnectionPolicyInfoInfoFinder,
      ExecutionProfilesInfoFinder executionProfilesInfoFinder,
      ConfigAntiPatternsFinder configAntiPatternsFinder,
      DataCentersFinder dataCentersFinder,
      StackTraceElement[] initCallStackTrace) {
    this.driverContext = driverContext;
    this.controlConnection = driverContext.getControlConnection();
    this.timestampSupplier = timestampSupplier;
    this.insightsConfiguration = insightsConfiguration;
    this.platformInfoFinder = platformInfoFinder;
    this.reconnectionPolicyInfoInfoFinder = reconnectionPolicyInfoInfoFinder;
    this.executionProfilesInfoFinder = executionProfilesInfoFinder;
    this.configAntiPatternsFinder = configAntiPatternsFinder;
    this.dataCentersFinder = dataCentersFinder;
    this.initCallStackTrace = initCallStackTrace;
  }

  public CompletionStage<Void> sendStartupMessage() {
    try {
      if (!shouldSendEvent()) {
        return CompletableFuture.completedFuture(null);
      } else {
        String startupMessage = createStartupMessage();
        return sendJsonMessage(startupMessage)
            .whenComplete(
                (aVoid, throwable) -> {
                  if (throwable != null) {
                    LOGGER.debug(
                        "Error while sending startup message to Insights. Message was: "
                            + trimToFirst500characters(startupMessage),
                        throwable);
                  }
                });
      }
    } catch (Exception e) {
      LOGGER.debug("Unexpected error while sending startup message to Insights.", e);
      return CompletableFutures.failedFuture(e);
    }
  }

  private static String trimToFirst500characters(String startupMessage) {
    return startupMessage.substring(0, Math.min(startupMessage.length(), 500));
  }

  public void scheduleStatusMessageSend() {
    if (!shouldSendEvent()) {
      return;
    }
    scheduleInsightsTask =
        scheduleInsightsTask(
            insightsConfiguration.getStatusEventDelayMillis(),
            insightsConfiguration.getExecutor(),
            this::sendStatusMessage);
  }

  public void shutdown() {
    if (scheduleInsightsTask != null) {
      scheduleInsightsTask.cancel(false);
    }
  }

  @VisibleForTesting
  public CompletionStage<Void> sendStatusMessage() {
    try {
      String statusMessage = createStatusMessage();
      CompletionStage<Void> result = sendJsonMessage(statusMessage);
      return result.whenComplete(
          (aVoid, throwable) -> {
            if (throwable != null) {
              if (numberOfStatusEventErrors.getAndIncrement() < MAX_NUMBER_OF_STATUS_ERROR_LOGS) {
                LOGGER.debug(
                    "Error while sending status message to Insights. Message was: "
                        + trimToFirst500characters(statusMessage),
                    throwable);
              }
            }
          });
    } catch (Exception e) {
      LOGGER.debug("Unexpected error while sending status message to Insights.", e);
      return CompletableFutures.failedFuture(e);
    }
  }

  private CompletionStage<Void> sendJsonMessage(String jsonMessage) {

    QueryOptions queryOptions = createQueryOptionsWithJson(jsonMessage);
    String logPrefix = driverContext.getSessionName();
    Duration timeout =
        driverContext
            .getConfig()
            .getDefaultProfile()
            .getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT);
    LOGGER.debug("sending JSON message: {}", jsonMessage);

    Query query = new Query(REPORT_INSIGHT_RPC, queryOptions);
    return AdminRequestHandler.call(controlConnection.channel(), query, timeout, logPrefix).start();
  }

  private QueryOptions createQueryOptionsWithJson(String json) {
    TypeCodec<String> codec =
        driverContext.getCodecRegistry().codecFor(DataTypes.TEXT, String.class);
    ByteBuffer startupMessageSerialized = codec.encode(json, DseProtocolVersion.DSE_V2);
    return new QueryOptions(
        QueryOptions.DEFAULT.consistency,
        Collections.singletonList(startupMessageSerialized),
        QueryOptions.DEFAULT.namedValues,
        QueryOptions.DEFAULT.skipMetadata,
        QueryOptions.DEFAULT.pageSize,
        QueryOptions.DEFAULT.pagingState,
        QueryOptions.DEFAULT.serialConsistency,
        QueryOptions.DEFAULT.defaultTimestamp,
        QueryOptions.DEFAULT.keyspace,
        QueryOptions.DEFAULT.nowInSeconds);
  }

  private boolean shouldSendEvent() {
    try {
      return insightsConfiguration.isMonitorReportingEnabled()
          && InsightsSupportVerifier.supportsInsights(
              driverContext.getMetadataManager().getMetadata().getNodes().values());
    } catch (Exception e) {
      LOGGER.debug("Unexpected error while checking Insights support.", e);
      return false;
    }
  }

  @VisibleForTesting
  String createStartupMessage() {
    InsightMetadata insightMetadata = createMetadata(STARTUP_MESSAGE_NAME, STARTUP_VERSION_1_ID);
    InsightsStartupData data = createStartupData();

    try {
      return OBJECT_MAPPER.writeValueAsString(new Insight<>(insightMetadata, data));
    } catch (JsonProcessingException e) {
      throw new InsightEventFormatException("Problem when creating: " + STARTUP_MESSAGE_NAME, e);
    }
  }

  @VisibleForTesting
  String createStatusMessage() {
    InsightMetadata insightMetadata = createMetadata(STATUS_MESSAGE_NAME, STATUS_VERSION_1_ID);
    InsightsStatusData data = createStatusData();

    try {
      return OBJECT_MAPPER.writeValueAsString(new Insight<>(insightMetadata, data));
    } catch (JsonProcessingException e) {
      throw new InsightEventFormatException("Problem when creating: " + STATUS_MESSAGE_NAME, e);
    }
  }

  private InsightsStatusData createStatusData() {
    Map<String, String> startupOptions = driverContext.getStartupOptions();
    return InsightsStatusData.builder()
        .withClientId(getClientId(startupOptions))
        .withSessionId(id)
        .withControlConnection(getControlConnectionSocketAddress())
        .withConnectedNodes(getConnectedNodes())
        .build();
  }

  private Map<String, SessionStateForNode> getConnectedNodes() {
    Map<Node, ChannelPool> pools = driverContext.getPoolManager().getPools();
    return pools.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> AddressFormatter.nullSafeToString(entry.getKey().getEndPoint().retrieve()),
                this::constructSessionStateForNode));
  }

  private SessionStateForNode constructSessionStateForNode(Map.Entry<Node, ChannelPool> entry) {
    return new SessionStateForNode(
        entry.getKey().getOpenConnections(), entry.getValue().getInFlight());
  }

  private InsightsStartupData createStartupData() {
    Map<String, String> startupOptions = driverContext.getStartupOptions();
    return InsightsStartupData.builder()
        .withClientId(getClientId(startupOptions))
        .withSessionId(id)
        .withApplicationName(getApplicationName(startupOptions))
        .withApplicationVersion(getApplicationVersion(startupOptions))
        .withDriverName(getDriverName(startupOptions))
        .withDriverVersion(getDriverVersion(startupOptions))
        .withContactPoints(
            getResolvedContactPoints(
                driverContext.getMetadataManager().getContactPoints().stream()
                    .map(n -> n.getEndPoint().retrieve())
                    .filter(InetSocketAddress.class::isInstance)
                    .map(InetSocketAddress.class::cast)
                    .collect(Collectors.toSet())))
        .withInitialControlConnection(getControlConnectionSocketAddress())
        .withProtocolVersion(driverContext.getProtocolVersion().getCode())
        .withLocalAddress(getLocalAddress())
        .withExecutionProfiles(executionProfilesInfoFinder.getExecutionProfilesInfo(driverContext))
        .withPoolSizeByHostDistance(getPoolSizeByHostDistance())
        .withHeartbeatInterval(
            driverContext
                .getConfig()
                .getDefaultProfile()
                .getDuration(HEARTBEAT_INTERVAL)
                .toMillis())
        .withCompression(
            driverContext.getConfig().getDefaultProfile().getString(PROTOCOL_COMPRESSION, "none"))
        .withReconnectionPolicy(
            reconnectionPolicyInfoInfoFinder.getReconnectionPolicyInfo(
                driverContext.getReconnectionPolicy(),
                driverContext.getConfig().getDefaultProfile()))
        .withSsl(getSsl())
        .withAuthProvider(getAuthProvider())
        .withOtherOptions(getOtherOptions())
        .withPlatformInfo(platformInfoFinder.getInsightsPlatformInfo())
        .withConfigAntiPatterns(configAntiPatternsFinder.findAntiPatterns(driverContext))
        .withPeriodicStatusInterval(getPeriodicStatusInterval())
        .withHostName(getLocalHostName())
        .withApplicationNameWasGenerated(isApplicationNameGenerated(startupOptions))
        .withDataCenters(dataCentersFinder.getDataCenters(driverContext))
        .build();
  }

  private AuthProviderType getAuthProvider() {
    String authProviderClassName =
        driverContext
            .getConfig()
            .getDefaultProfile()
            .getString(AUTH_PROVIDER_CLASS, "NoAuthProvider");
    ClassSettingDetails authProviderDetails =
        PackageUtil.getAuthProviderDetails(authProviderClassName);
    return new AuthProviderType(
        authProviderDetails.getClassName(), authProviderDetails.getFullPackage());
  }

  private long getPeriodicStatusInterval() {
    return TimeUnit.MILLISECONDS.toSeconds(insightsConfiguration.getStatusEventDelayMillis());
  }

  @VisibleForTesting
  static Map<String, List<String>> getResolvedContactPoints(Set<InetSocketAddress> contactPoints) {
    if (contactPoints == null) {
      return Collections.emptyMap();
    }
    return contactPoints.stream()
        .collect(
            Collectors.groupingBy(
                InetSocketAddress::getHostName,
                Collectors.mapping(AddressFormatter::nullSafeToString, Collectors.toList())));
  }

  private String getDriverVersion(Map<String, String> startupOptions) {
    return startupOptions.get(StartupOptionsBuilder.DRIVER_VERSION_KEY);
  }

  private String getDriverName(Map<String, String> startupOptions) {
    return startupOptions.get(StartupOptionsBuilder.DRIVER_NAME_KEY);
  }

  private String getClientId(Map<String, String> startupOptions) {
    return startupOptions.get(StartupOptionsBuilder.CLIENT_ID_KEY);
  }

  private boolean isApplicationNameGenerated(Map<String, String> startupOptions) {
    return startupOptions.get(StartupOptionsBuilder.APPLICATION_NAME_KEY) == null;
  }

  private String getApplicationVersion(Map<String, String> startupOptions) {
    String applicationVersion = startupOptions.get(StartupOptionsBuilder.APPLICATION_VERSION_KEY);
    if (applicationVersion == null) {
      return "";
    }
    return applicationVersion;
  }

  private String getApplicationName(Map<String, String> startupOptions) {
    String applicationName = startupOptions.get(StartupOptionsBuilder.APPLICATION_NAME_KEY);
    if (applicationName == null || applicationName.isEmpty()) {
      return getClusterCreateCaller(initCallStackTrace);
    }
    return applicationName;
  }

  @VisibleForTesting
  static String getClusterCreateCaller(StackTraceElement[] stackTrace) {
    for (int i = 0; i < stackTrace.length - 1; i++) {
      if (isClusterStackTrace(stackTrace[i])) {
        int nextElement = i + 1;
        if (!isClusterStackTrace(stackTrace[nextElement])) {
          return stackTrace[nextElement].getClassName();
        }
      }
    }
    return DEFAULT_JAVA_APPLICATION;
  }

  private static boolean isClusterStackTrace(StackTraceElement stackTraceElement) {
    return stackTraceElement.getClassName().equals(DefaultDriverContext.class.getName())
        || stackTraceElement.getClassName().equals(SessionBuilder.class.getName());
  }

  private String getLocalHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOGGER.warn("Can not resolve the name of a host, returning null", e);
      return null;
    }
  }

  private Map<String, Object> getOtherOptions() {
    return Collections.emptyMap(); // todo
  }

  private SSL getSsl() {
    boolean isSslDefined =
        driverContext.getConfig().getDefaultProfile().isDefined(SSL_ENGINE_FACTORY_CLASS);
    boolean certValidation =
        driverContext.getConfig().getDefaultProfile().getBoolean(SSL_HOSTNAME_VALIDATION, false);
    return new SSL(isSslDefined, certValidation);
  }

  private PoolSizeByHostDistance getPoolSizeByHostDistance() {

    return new PoolSizeByHostDistance(
        driverContext.getConfig().getDefaultProfile().getInt(CONNECTION_POOL_LOCAL_SIZE),
        driverContext.getConfig().getDefaultProfile().getInt(CONNECTION_POOL_REMOTE_SIZE),
        0);
  }

  private String getControlConnectionSocketAddress() {
    SocketAddress controlConnectionAddress = controlConnection.channel().getEndPoint().retrieve();
    return AddressFormatter.nullSafeToString(controlConnectionAddress);
  }

  private String getLocalAddress() {
    SocketAddress controlConnectionLocalAddress = controlConnection.channel().localAddress();
    if (controlConnectionLocalAddress instanceof InetSocketAddress) {
      return AddressFormatter.nullSafeToString(
          ((InetSocketAddress) controlConnectionLocalAddress).getAddress());
    }
    return null;
  }

  private InsightMetadata createMetadata(String messageName, String messageVersion) {
    return new InsightMetadata(
        messageName, timestampSupplier.get(), TAGS, InsightType.EVENT, messageVersion);
  }

  @VisibleForTesting
  static ScheduledFuture<?> scheduleInsightsTask(
      long statusEventDelayMillis,
      ScheduledExecutorService scheduledTasksExecutor,
      Runnable runnable) {
    long initialDelay =
        (long) Math.floor(statusEventDelayMillis - zeroToTenPercentRandom(statusEventDelayMillis));
    return scheduledTasksExecutor.scheduleWithFixedDelay(
        runnable, initialDelay, statusEventDelayMillis, TimeUnit.MILLISECONDS);
  }

  private static double zeroToTenPercentRandom(long statusEventDelayMillis) {
    return 0.1 * statusEventDelayMillis * Math.random();
  }
}
