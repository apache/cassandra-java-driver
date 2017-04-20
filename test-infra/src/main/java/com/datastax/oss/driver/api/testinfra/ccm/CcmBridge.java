/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.testinfra.ccm;

import com.datastax.oss.driver.api.core.CassandraVersion;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteStreamHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.util.internal.PlatformDependent.isWindows;

public class CcmBridge implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(CcmBridge.class);

  private final CassandraVersion cassandraVersion;

  private final int nodes[];

  private final Path directory;

  private final AtomicBoolean started = new AtomicBoolean();

  private final AtomicBoolean created = new AtomicBoolean();

  private final String ipPrefix;

  private final Map<String, Object> initialConfiguration;

  private final String jvmArgs;

  public static final CassandraVersion DEFAULT_CASSANDRA_VERSION =
      CassandraVersion.parse(System.getProperty("ccm.cassandraVersion", "3.11.0"));

  public static final String DEFAULT_CLIENT_TRUSTSTORE_PASSWORD = "cassandra1sfun";
  public static final String DEFAULT_CLIENT_TRUSTSTORE_PATH = "/client.truststore";

  public static final File DEFAULT_CLIENT_TRUSTSTORE_FILE =
      createTempStore(DEFAULT_CLIENT_TRUSTSTORE_PATH);

  public static final String DEFAULT_CLIENT_KEYSTORE_PASSWORD = "cassandra1sfun";
  public static final String DEFAULT_CLIENT_KEYSTORE_PATH = "/client.keystore";

  public static final File DEFAULT_CLIENT_KEYSTORE_FILE =
      createTempStore(DEFAULT_CLIENT_KEYSTORE_PATH);

  // Contains the same keypair as the client keystore, but in format usable by OpenSSL
  public static final File DEFAULT_CLIENT_PRIVATE_KEY_FILE = createTempStore("/client.key");
  public static final File DEFAULT_CLIENT_CERT_CHAIN_FILE = createTempStore("/client.crt");

  public static final String DEFAULT_SERVER_TRUSTSTORE_PASSWORD = "cassandra1sfun";
  public static final String DEFAULT_SERVER_TRUSTSTORE_PATH = "/server.truststore";

  private static final File DEFAULT_SERVER_TRUSTSTORE_FILE =
      createTempStore(DEFAULT_SERVER_TRUSTSTORE_PATH);

  public static final String DEFAULT_SERVER_KEYSTORE_PASSWORD = "cassandra1sfun";
  public static final String DEFAULT_SERVER_KEYSTORE_PATH = "/server.keystore";

  private static final File DEFAULT_SERVER_KEYSTORE_FILE =
      createTempStore(DEFAULT_SERVER_KEYSTORE_PATH);

  private CcmBridge(
      Path directory,
      CassandraVersion cassandraVersion,
      int nodes[],
      String ipPrefix,
      Map<String, Object> initialConfiguration,
      Collection<String> jvmArgs) {
    this.directory = directory;
    this.cassandraVersion = cassandraVersion;
    this.nodes = nodes;
    this.ipPrefix = ipPrefix;
    this.initialConfiguration = initialConfiguration;

    StringBuilder allJvmArgs = new StringBuilder("");
    String quote = isWindows() ? "\"" : "";
    for (String jvmArg : jvmArgs) {
      // Windows requires jvm arguments to be quoted, while *nix requires unquoted.
      allJvmArgs.append(" ");
      allJvmArgs.append(quote);
      allJvmArgs.append("--jvm_arg=");
      allJvmArgs.append(jvmArg);
      allJvmArgs.append(quote);
    }
    this.jvmArgs = allJvmArgs.toString();
  }

  public CassandraVersion getCassandraVersion() {
    return cassandraVersion;
  }

  public void create() {
    if (created.compareAndSet(false, true)) {
      execute(
          "create",
          "ccm_1",
          "-v",
          cassandraVersion.toString(),
          "-i",
          ipPrefix,
          "-n",
          Arrays.stream(nodes).mapToObj(n -> "" + n).collect(Collectors.joining(":")));

      for (Map.Entry<String, Object> conf : initialConfiguration.entrySet()) {
        execute("updateconf", String.format("%s:%s", conf.getKey(), conf.getValue()));
      }
    }
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      execute("start", jvmArgs, "--wait-for-binary-proto");
    }
  }

  public void stop() {
    if (started.compareAndSet(true, false)) {
      execute("stop");
    }
  }

  public void remove() {
    execute("remove");
  }

  synchronized void execute(String... args) {
    String command =
        "ccm " + String.join(" ", args) + " --config-dir=" + directory.toFile().getAbsolutePath();
    logger.debug("Executing: " + command);

    CommandLine cli = CommandLine.parse(command);
    ExecuteWatchdog watchDog = new ExecuteWatchdog(TimeUnit.MINUTES.toMillis(10));
    try (LogOutputStream outStream =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                logger.debug("ccmout> {}", line);
              }
            };
        LogOutputStream errStream =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                logger.warn("ccmerr> {}", line);
              }
            }) {
      Executor executor = new DefaultExecutor();
      ExecuteStreamHandler streamHandler = new PumpStreamHandler(outStream, errStream);
      executor.setStreamHandler(streamHandler);
      executor.setWatchdog(watchDog);

      int retValue = executor.execute(cli);
      if (retValue != 0) {
        logger.error(
            "Non-zero exit code ({}) returned from executing ccm command: {}", retValue, command);
      }
    } catch (IOException ex) {
      if (watchDog.killedProcess()) {
        throw new RuntimeException("The command '" + command + "' was killed after 10 minutes");
      } else {
        throw new RuntimeException("The command '" + command + "' failed to execute");
      }
    }
  }

  @Override
  public void close() {
    remove();
  }

  /**
   * Extracts a keystore from the classpath into a temporary file.
   *
   * <p>This is needed as the keystore could be part of a built test jar used by other projects, and
   * they need to be extracted to a file system so cassandra may use them.
   *
   * @param storePath Path in classpath where the keystore exists.
   * @return The generated File.
   */
  private static File createTempStore(String storePath) {
    File f = null;
    try (InputStream trustStoreIs = CcmBridge.class.getResourceAsStream(storePath)) {
      f = File.createTempFile("server", ".store");
      logger.debug("Created store file {} for {}.", f, storePath);
      try (OutputStream trustStoreOs = new FileOutputStream(f)) {
        byte[] buffer = new byte[1024];
        int len;
        while ((len = trustStoreIs.read(buffer)) != -1) {
          trustStoreOs.write(buffer, 0, len);
        }
      }
    } catch (IOException e) {
      logger.warn("Failure to write keystore, SSL-enabled servers may fail to start.", e);
    }
    return f;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private int[] nodes = {1};
    private final Map<String, Object> cassandraConfiguration = new LinkedHashMap<>();
    private final List<String> jvmArgs = new ArrayList<>();
    private String ipPrefix = "127.0.0.";
    private CassandraVersion cassandraVersion = CcmBridge.DEFAULT_CASSANDRA_VERSION;

    private final Path directory;

    private Builder() {
      try {
        this.directory = Files.createTempDirectory("ccm");
      } catch (IOException e) {
        // change to unchecked for now.
        throw new RuntimeException(e);
      }
      // disable auto_snapshot by default to reduce disk usage when destroying schema.
      withCassandraConfiguration("auto_snapshot", "false");
    }

    public Builder withCassandraConfiguration(String key, Object value) {
      cassandraConfiguration.put(key, value);
      return this;
    }

    public Builder withJvmArgs(String... jvmArgs) {
      Collections.addAll(this.jvmArgs, jvmArgs);
      return this;
    }

    public Builder withCassandraVersion(CassandraVersion cassandraVersion) {
      this.cassandraVersion = cassandraVersion;
      return this;
    }

    public Builder withNodes(int... nodes) {
      this.nodes = nodes;
      return this;
    }

    public Builder withIpPrefix(String ipPrefix) {
      this.ipPrefix = ipPrefix;
      return this;
    }

    /** Enables SSL encryption. */
    public Builder withSsl() {
      cassandraConfiguration.put("client_encryption_options.enabled", "true");
      cassandraConfiguration.put(
          "client_encryption_options.keystore", DEFAULT_SERVER_KEYSTORE_FILE.getAbsolutePath());
      cassandraConfiguration.put(
          "client_encryption_options.keystore_password", DEFAULT_SERVER_KEYSTORE_PASSWORD);
      return this;
    }

    /** Enables client authentication. This also enables encryption ({@link #withSsl()}. */
    public Builder withSslAuth() {
      withSsl();
      cassandraConfiguration.put("client_encryption_options.require_client_auth", "true");
      cassandraConfiguration.put(
          "client_encryption_options.truststore", DEFAULT_SERVER_TRUSTSTORE_FILE.getAbsolutePath());
      cassandraConfiguration.put(
          "client_encryption_options.truststore_password", DEFAULT_SERVER_TRUSTSTORE_PASSWORD);
      return this;
    }

    public CcmBridge build() {
      if (cassandraVersion.compareTo(CassandraVersion.V2_2_0) >= 0) {
        cassandraConfiguration.put("enable_user_defined_functions", "true");
      }
      return new CcmBridge(
          directory, cassandraVersion, nodes, ipPrefix, cassandraConfiguration, jvmArgs);
    }
  }
}
