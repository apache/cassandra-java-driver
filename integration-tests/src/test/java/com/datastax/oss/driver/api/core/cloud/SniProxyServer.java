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
package com.datastax.oss.driver.api.core.cloud;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteStreamHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SniProxyServer {

  private static final Logger LOG = LoggerFactory.getLogger(SniProxyServer.class);

  private final Path proxyPath;
  private final Path bundlesRootPath;
  private final Path defaultBundlePath;
  private final Path bundleWithoutCredentialsPath;
  private final Path bundleWithoutClientCertificatesPath;
  private final Path bundleWithInvalidCAPath;
  private final Path bundleWithUnreachableMetadataServicePath;

  private volatile boolean running = false;

  public SniProxyServer() {
    this(Paths.get(System.getProperty("proxy.path", "./")));
  }

  public SniProxyServer(Path proxyPath) {
    this.proxyPath = proxyPath.normalize().toAbsolutePath();
    bundlesRootPath = proxyPath.resolve("certs/bundles/");
    defaultBundlePath = bundlesRootPath.resolve("creds-v1.zip");
    bundleWithoutCredentialsPath = bundlesRootPath.resolve("creds-v1-wo-creds.zip");
    bundleWithoutClientCertificatesPath = bundlesRootPath.resolve("creds-v1-wo-cert.zip");
    bundleWithInvalidCAPath = bundlesRootPath.resolve("creds-v1-invalid-ca.zip");
    bundleWithUnreachableMetadataServicePath = bundlesRootPath.resolve("creds-v1-unreachable.zip");
  }

  public void startProxy() {
    CommandLine run = CommandLine.parse(proxyPath + "/run.sh");
    execute(run);
    running = true;
  }

  public void stopProxy() {
    if (running) {
      CommandLine findImageId =
          CommandLine.parse("docker ps -a -q --filter ancestor=single_endpoint");
      String id = execute(findImageId);
      CommandLine stop = CommandLine.parse("docker kill " + id);
      execute(stop);
      running = false;
    }
  }

  /** @return The root folder of the SNI proxy server docker image. */
  public Path getProxyPath() {
    return proxyPath;
  }

  /**
   * @return The root folder where secure connect bundles exposed by this SNI proxy for testing
   *     purposes can be found.
   */
  public Path getBundlesRootPath() {
    return bundlesRootPath;
  }

  /**
   * @return The default secure connect bundle. It contains credentials and all certificates
   *     required to connect.
   */
  public Path getDefaultBundlePath() {
    return defaultBundlePath;
  }

  /** @return A secure connect bundle without credentials in config.json. */
  public Path getBundleWithoutCredentialsPath() {
    return bundleWithoutCredentialsPath;
  }

  /** @return A secure connect bundle without client certificates (no identity.jks). */
  public Path getBundleWithoutClientCertificatesPath() {
    return bundleWithoutClientCertificatesPath;
  }

  /** @return A secure connect bundle with an invalid Certificate Authority. */
  public Path getBundleWithInvalidCAPath() {
    return bundleWithInvalidCAPath;
  }

  /** @return A secure connect bundle with an invalid address for the Proxy Metadata Service. */
  public Path getBundleWithUnreachableMetadataServicePath() {
    return bundleWithUnreachableMetadataServicePath;
  }

  private String execute(CommandLine cli) {
    LOG.debug("Executing: " + cli);
    ExecuteWatchdog watchDog = new ExecuteWatchdog(TimeUnit.MINUTES.toMillis(10));
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    try (LogOutputStream errStream =
        new LogOutputStream() {
          @Override
          protected void processLine(String line, int logLevel) {
            LOG.error("sniendpointerr> {}", line);
          }
        }) {
      Executor executor = new DefaultExecutor();
      ExecuteStreamHandler streamHandler = new PumpStreamHandler(outStream, errStream);
      executor.setStreamHandler(streamHandler);
      executor.setWatchdog(watchDog);
      executor.setWorkingDirectory(proxyPath.toFile());
      int retValue = executor.execute(cli);
      if (retValue != 0) {
        LOG.error("Non-zero exit code ({}) returned from executing ccm command: {}", retValue, cli);
      }
      return outStream.toString();
    } catch (IOException ex) {
      if (watchDog.killedProcess()) {
        throw new RuntimeException("The command '" + cli + "' was killed after 10 minutes");
      } else {
        throw new RuntimeException("The command '" + cli + "' failed to execute", ex);
      }
    }
  }
}
