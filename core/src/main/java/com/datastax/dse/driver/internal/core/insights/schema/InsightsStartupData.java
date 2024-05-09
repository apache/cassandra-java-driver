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
package com.datastax.dse.driver.internal.core.insights.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InsightsStartupData {
  @JsonProperty("clientId")
  private final String clientId;

  @JsonProperty("sessionId")
  private final String sessionId;

  @JsonProperty("applicationName")
  private final String applicationName;

  @JsonProperty("applicationVersion")
  private final String applicationVersion;

  @JsonProperty("contactPoints")
  private final Map<String, List<String>> contactPoints;

  @JsonProperty("initialControlConnection")
  private final String initialControlConnection;

  @JsonProperty("protocolVersion")
  private final int protocolVersion;

  @JsonProperty("localAddress")
  private final String localAddress;

  @JsonProperty("executionProfiles")
  private final Map<String, SpecificExecutionProfile> executionProfiles;

  @JsonProperty("poolSizeByHostDistance")
  private final PoolSizeByHostDistance poolSizeByHostDistance;

  @JsonProperty("heartbeatInterval")
  private final long heartbeatInterval;

  @JsonProperty("compression")
  private final String compression;

  @JsonProperty("reconnectionPolicy")
  private final ReconnectionPolicyInfo reconnectionPolicy;

  @JsonProperty("ssl")
  private final SSL ssl;

  @JsonProperty("authProvider")
  private final AuthProviderType authProvider;

  @JsonProperty("otherOptions")
  private final Map<String, Object> otherOptions;

  @JsonProperty("configAntiPatterns")
  private final Map<String, String> configAntiPatterns;

  @JsonProperty("periodicStatusInterval")
  private final long periodicStatusInterval;

  @JsonProperty("platformInfo")
  private final InsightsPlatformInfo platformInfo;

  @JsonProperty("hostName")
  private final String hostName;

  @JsonProperty("driverName")
  private String driverName;

  @JsonProperty("applicationNameWasGenerated")
  private boolean applicationNameWasGenerated;

  @JsonProperty("driverVersion")
  private String driverVersion;

  @JsonProperty("dataCenters")
  private Set<String> dataCenters;

  @JsonCreator
  private InsightsStartupData(
      @JsonProperty("clientId") String clientId,
      @JsonProperty("sessionId") String sessionId,
      @JsonProperty("applicationName") String applicationName,
      @JsonProperty("applicationVersion") String applicationVersion,
      @JsonProperty("contactPoints") Map<String, List<String>> contactPoints,
      @JsonProperty("initialControlConnection") String initialControlConnection,
      @JsonProperty("protocolVersion") int protocolVersion,
      @JsonProperty("localAddress") String localAddress,
      @JsonProperty("executionProfiles") Map<String, SpecificExecutionProfile> executionProfiles,
      @JsonProperty("poolSizeByHostDistance") PoolSizeByHostDistance poolSizeByHostDistance,
      @JsonProperty("heartbeatInterval") long heartbeatInterval,
      @JsonProperty("compression") String compression,
      @JsonProperty("reconnectionPolicy") ReconnectionPolicyInfo reconnectionPolicy,
      @JsonProperty("ssl") SSL ssl,
      @JsonProperty("authProvider") AuthProviderType authProvider,
      @JsonProperty("otherOptions") Map<String, Object> otherOptions,
      @JsonProperty("configAntiPatterns") Map<String, String> configAntiPatterns,
      @JsonProperty("periodicStatusInterval") long periodicStatusInterval,
      @JsonProperty("platformInfo") InsightsPlatformInfo platformInfo,
      @JsonProperty("hostName") String hostName,
      @JsonProperty("driverName") String driverName,
      @JsonProperty("applicationNameWasGenerated") boolean applicationNameWasGenerated,
      @JsonProperty("driverVersion") String driverVersion,
      @JsonProperty("dataCenters") Set<String> dataCenters) {
    this.clientId = clientId;
    this.sessionId = sessionId;
    this.applicationName = applicationName;
    this.applicationVersion = applicationVersion;
    this.contactPoints = contactPoints;
    this.initialControlConnection = initialControlConnection;
    this.protocolVersion = protocolVersion;
    this.localAddress = localAddress;
    this.executionProfiles = executionProfiles;
    this.poolSizeByHostDistance = poolSizeByHostDistance;
    this.heartbeatInterval = heartbeatInterval;
    this.compression = compression;
    this.reconnectionPolicy = reconnectionPolicy;
    this.ssl = ssl;
    this.authProvider = authProvider;
    this.otherOptions = otherOptions;
    this.configAntiPatterns = configAntiPatterns;
    this.periodicStatusInterval = periodicStatusInterval;
    this.platformInfo = platformInfo;
    this.hostName = hostName;
    this.driverName = driverName;
    this.applicationNameWasGenerated = applicationNameWasGenerated;
    this.driverVersion = driverVersion;
    this.dataCenters = dataCenters;
  }

  public String getClientId() {
    return clientId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public String getApplicationVersion() {
    return applicationVersion;
  }

  public Map<String, List<String>> getContactPoints() {
    return contactPoints;
  }

  public String getInitialControlConnection() {
    return initialControlConnection;
  }

  public int getProtocolVersion() {
    return protocolVersion;
  }

  public String getLocalAddress() {
    return localAddress;
  }

  public Map<String, SpecificExecutionProfile> getExecutionProfiles() {
    return executionProfiles;
  }

  public PoolSizeByHostDistance getPoolSizeByHostDistance() {
    return poolSizeByHostDistance;
  }

  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  public String getCompression() {
    return compression;
  }

  public ReconnectionPolicyInfo getReconnectionPolicy() {
    return reconnectionPolicy;
  }

  public SSL getSsl() {
    return ssl;
  }

  public AuthProviderType getAuthProvider() {
    return authProvider;
  }

  public Map<String, Object> getOtherOptions() {
    return otherOptions;
  }

  public Map<String, String> getConfigAntiPatterns() {
    return configAntiPatterns;
  }

  public long getPeriodicStatusInterval() {
    return periodicStatusInterval;
  }

  public InsightsPlatformInfo getPlatformInfo() {
    return platformInfo;
  }

  public String getHostName() {
    return hostName;
  }

  public String getDriverName() {
    return driverName;
  }

  public boolean isApplicationNameWasGenerated() {
    return applicationNameWasGenerated;
  }

  public String getDriverVersion() {
    return driverVersion;
  }

  public Set<String> getDataCenters() {
    return dataCenters;
  }

  public static InsightsStartupData.Builder builder() {
    return new InsightsStartupData.Builder();
  }

  public static class Builder {
    private String clientId;
    private String sessionId;
    private String applicationName;
    private String applicationVersion;
    private Map<String, List<String>> contactPoints;
    private String initialControlConnection;
    private int protocolVersion;
    private String localAddress;
    private Map<String, SpecificExecutionProfile> executionProfiles;
    private PoolSizeByHostDistance poolSizeByHostDistance;
    private long heartbeatInterval;
    private String compression;
    private ReconnectionPolicyInfo reconnectionPolicy;
    private SSL ssl;
    private AuthProviderType authProvider;
    private Map<String, Object> otherOptions;
    private Map<String, String> configAntiPatterns;
    private long periodicStatusInterval;
    private InsightsPlatformInfo platformInfo;
    private String hostName;
    private String driverName;
    private String driverVersion;
    private boolean applicationNameWasGenerated;
    private Set<String> dataCenters;

    public InsightsStartupData build() {
      return new InsightsStartupData(
          clientId,
          sessionId,
          applicationName,
          applicationVersion,
          contactPoints,
          initialControlConnection,
          protocolVersion,
          localAddress,
          executionProfiles,
          poolSizeByHostDistance,
          heartbeatInterval,
          compression,
          reconnectionPolicy,
          ssl,
          authProvider,
          otherOptions,
          configAntiPatterns,
          periodicStatusInterval,
          platformInfo,
          hostName,
          driverName,
          applicationNameWasGenerated,
          driverVersion,
          dataCenters);
    }

    public Builder withClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder withSessionId(String id) {
      this.sessionId = id;
      return this;
    }

    public Builder withApplicationName(String applicationName) {
      this.applicationName = applicationName;
      return this;
    }

    public Builder withApplicationVersion(String applicationVersion) {
      this.applicationVersion = applicationVersion;
      return this;
    }

    public Builder withContactPoints(Map<String, List<String>> contactPoints) {
      this.contactPoints = contactPoints;
      return this;
    }

    public Builder withInitialControlConnection(String inetSocketAddress) {
      this.initialControlConnection = inetSocketAddress;
      return this;
    }

    public Builder withProtocolVersion(int protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
    }

    public Builder withLocalAddress(String localAddress) {
      this.localAddress = localAddress;
      return this;
    }

    public Builder withExecutionProfiles(Map<String, SpecificExecutionProfile> executionProfiles) {
      this.executionProfiles = executionProfiles;
      return this;
    }

    public Builder withPoolSizeByHostDistance(PoolSizeByHostDistance poolSizeByHostDistance) {
      this.poolSizeByHostDistance = poolSizeByHostDistance;
      return this;
    }

    public Builder withHeartbeatInterval(long heartbeatInterval) {
      this.heartbeatInterval = heartbeatInterval;
      return this;
    }

    public Builder withCompression(String compression) {
      this.compression = compression;
      return this;
    }

    public Builder withReconnectionPolicy(ReconnectionPolicyInfo reconnectionPolicy) {
      this.reconnectionPolicy = reconnectionPolicy;
      return this;
    }

    public Builder withSsl(SSL ssl) {
      this.ssl = ssl;
      return this;
    }

    public Builder withAuthProvider(AuthProviderType authProvider) {
      this.authProvider = authProvider;
      return this;
    }

    public Builder withOtherOptions(Map<String, Object> otherOptions) {
      this.otherOptions = otherOptions;
      return this;
    }

    public Builder withConfigAntiPatterns(Map<String, String> configAntiPatterns) {
      this.configAntiPatterns = configAntiPatterns;
      return this;
    }

    public Builder withPeriodicStatusInterval(long periodicStatusInterval) {
      this.periodicStatusInterval = periodicStatusInterval;
      return this;
    }

    public Builder withPlatformInfo(InsightsPlatformInfo insightsPlatformInfo) {
      this.platformInfo = insightsPlatformInfo;
      return this;
    }

    public Builder withHostName(String hostName) {
      this.hostName = hostName;
      return this;
    }

    public Builder withDriverName(String driverName) {
      this.driverName = driverName;
      return this;
    }

    public Builder withDriverVersion(String driverVersion) {
      this.driverVersion = driverVersion;
      return this;
    }

    public Builder withApplicationNameWasGenerated(boolean applicationNameWasGenerated) {
      this.applicationNameWasGenerated = applicationNameWasGenerated;
      return this;
    }

    public Builder withDataCenters(Set<String> dataCenters) {
      this.dataCenters = dataCenters;
      return this;
    }
  }
}
