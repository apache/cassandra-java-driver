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
package com.datastax.driver.core;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DbaasConfiguration {
  private String username;
  private String password;
  private String host;
  private int port;
  private String sniHost;
  private int sniPort;
  private List<String> hostIds;
  private String keyspace;
  private String localDC;
  private String keyStorePassword;
  private String trustStorePassword;
  private String secureBundlePath;
  private boolean zip;

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
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

  public String getKeyspace() {
    return keyspace;
  }

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  public String getLocalDC() {
    return localDC;
  }

  public void setLocalDC(String localDC) {
    this.localDC = localDC;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public void setKeyStorePassword(String keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public void setTrustStorePassword(String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
  }

  public String getSecureBundlePath() {
    return secureBundlePath;
  }

  public void setSecureBundlePath(String secureBundlePath) {
    this.secureBundlePath = secureBundlePath;
  }

  public boolean isZip() {
    return zip;
  }

  public void setZip(boolean zip) {
    this.zip = zip;
  }
}
