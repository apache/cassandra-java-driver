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
package com.datastax.oss.driver.api.core.cloud;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
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
  private static final Logger logger = LoggerFactory.getLogger(SniProxyServer.class);
  static final String CERTS_BUNDLE_SUFFIX = "/certs/bundles/creds-v1.zip";
  private static final String CERTS_BUNDLE_SUFFIX_RELATIVE =
      "/certs/bundles/../bundles/creds-v1.zip";
  private final String proxyPath;
  private boolean isRunning = false;

  public SniProxyServer() {
    proxyPath = System.getProperty("proxy.path", "./");
  }

  public void startProxy() {
    CommandLine run = CommandLine.parse(proxyPath + "/run.sh");
    execute(run);
    isRunning = true;
  }

  public void stopProxy() {
    if (isRunning) {
      CommandLine findImageId =
          CommandLine.parse("docker ps -a -q --filter ancestor=single_endpoint");
      String id = execute(findImageId);
      CommandLine stop = CommandLine.parse("docker kill " + id);
      execute(stop);
      isRunning = false;
    }
  }

  public boolean isRunning() {
    return isRunning;
  }

  public String getSecureBundlePath() {
    return proxyPath + CERTS_BUNDLE_SUFFIX;
  }

  public String getSecureBundleRelativePath() {
    return proxyPath + CERTS_BUNDLE_SUFFIX_RELATIVE;
  }

  public String getSecureBundleNoCredsPath() {
    return proxyPath + "/certs/bundles/creds-v1-wo-creds.zip";
  }

  public String getSecureBundleUnreachable() {
    return proxyPath + "/certs/bundles/creds-v1-unreachable.zip";
  }

  private String execute(CommandLine cli) {
    logger.debug("Executing: " + cli);
    ExecuteWatchdog watchDog = new ExecuteWatchdog(TimeUnit.MINUTES.toMillis(10));
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    try (LogOutputStream errStream =
        new LogOutputStream() {
          @Override
          protected void processLine(String line, int logLevel) {
            logger.error("sniendpointerr> {}", line);
          }
        }) {
      Executor executor = new DefaultExecutor();
      ExecuteStreamHandler streamHandler = new PumpStreamHandler(outStream, errStream);
      executor.setStreamHandler(streamHandler);
      executor.setWatchdog(watchDog);
      executor.setWorkingDirectory(new File(proxyPath));
      int retValue = executor.execute(cli);
      if (retValue != 0) {
        logger.error(
            "Non-zero exit code ({}) returned from executing ccm command: {}", retValue, cli);
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
