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
package com.datastax.oss.driver.api.testinfra.cqlproxy;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.shaded.guava.common.base.Predicates;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlProxyBridge {

  public static final String DEFAULT_BIND_ADDRESS = "127.0.0.2";

  private static final Logger LOG = LoggerFactory.getLogger(CqlProxyBridge.class);

  private final String bindAddress;
  private final Collection<String> contactPoints;

  private final AtomicBoolean started = new AtomicBoolean();
  private Process process;

  public CqlProxyBridge(Collection<EndPoint> contactPoints) {
    this(DEFAULT_BIND_ADDRESS, contactPoints);
  }

  public CqlProxyBridge(String bindAddress, Collection<EndPoint> contactPoints) {
    this.contactPoints =
        contactPoints.stream()
            .map(
                (cp) -> {
                  SocketAddress sockAddr = cp.resolve();
                  return (sockAddr instanceof InetSocketAddress)
                      ? ((InetSocketAddress) sockAddr).getHostName()
                      : null;
                })
            .filter(Predicates.notNull())
            .collect(Collectors.toList());
    this.bindAddress = bindAddress;
  }

  private Thread loggingThread(InputStream inputStream, String prefix) {
    return new Thread(
        () -> {
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
          String line = null;
          try {
            while (true) {
              line = reader.readLine();
              if (line == null) {
                LOG.debug("End of stream reached for {}, exiting reader thread", prefix);
              } else {
                LOG.error("{}> {}", prefix, line);
              }
            }
          } catch (IOException ioe) {
            LOG.error("IOException for {}, exiting reader thread", prefix);
          }
        });
  }

  private String contactPointArg() {
    return String.format("--contact-points=%s", String.join(",", this.contactPoints));
  }

  private String bindAddressArg() {
    return String.format("--bind=%s", this.bindAddress);
  }

  public void start() {

    if (started.compareAndSet(false, true)) {

      try {

        ProcessBuilder builder =
            new ProcessBuilder("cql-proxy", this.contactPointArg(), this.bindAddressArg());
        process = builder.start();
        loggingThread(process.getInputStream(), "proxyout").start();
        loggingThread(process.getErrorStream(), "proxyerr").start();
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to start cql proxy", ioe);
      }
    }
  }

  public void stop() {

    if (started.compareAndSet(true, false)) {

      try {

        LOG.info("Trying to stop cql-proxy normally");
        process.destroy();
        if (!process.waitFor(3, TimeUnit.SECONDS)) {

          LOG.warn("cql-proxy process did not exit cleanly, forcing shutdown");
          process.destroyForcibly();
        }
        if (process.waitFor(3, TimeUnit.SECONDS)) {
          LOG.info("cql-proxy process terminated cleanly");
        } else {
          LOG.info("cql-proxy process could not be terminated");
        }
      } catch (InterruptedException ie) {
        throw new RuntimeException("Interrupt while waiting for cql-proxy to exit", ie);
      }
    }
  }
}
