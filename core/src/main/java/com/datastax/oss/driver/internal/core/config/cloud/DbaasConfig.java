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
package com.datastax.oss.driver.internal.core.config.cloud;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.nio.file.Path;
import java.util.List;

/**
 * The POJO representation of the config.json that is distributed as part of the creds.zip. It is
 * populated mostly by the config.json. With the hostIds, and localDc being filled in by the
 * metadata service.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DbaasConfig {

  private String username;
  private String password;
  private String host;
  private int port;
  private String sniHost;
  private int sniPort;
  private List<String> hostIds;
  private String localDC;
  private String keyStorePassword;
  private String trustStorePassword;
  private Path secureConnectBundlePath;

  public void setUsername(String username) {
    this.username = username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setLocalDC(String localDC) {
    this.localDC = localDC;
  }

  public void setKeyStorePassword(String keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
  }

  public void setTrustStorePassword(String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getLocalDataCenter() {
    return localDC;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public String getSniHost() {
    return sniHost;
  }

  public void setSniHost(String sniHost) {
    this.sniHost = sniHost;
  }

  public int getSniPort() {
    return sniPort;
  }

  public void setSniPort(int sniPort) {
    this.sniPort = sniPort;
  }

  public List<String> getHostIds() {
    return hostIds;
  }

  public void setHostIds(List<String> hostIds) {
    this.hostIds = hostIds;
  }

  public Path getSecureConnectBundlePath() {
    return secureConnectBundlePath;
  }

  public void setSecureConnectBundlePath(Path secureConnectBundlePath) {
    this.secureConnectBundlePath = secureConnectBundlePath;
  }
}
