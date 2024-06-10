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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.api.testinfra.ccm;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.driver.shaded.guava.common.io.Resources;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

public class CcmBridge implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(CcmBridge.class);

  public static final Version VERSION =
      Objects.requireNonNull(Version.parse(System.getProperty("ccm.version", "4.0.0")));

  public static final String INSTALL_DIRECTORY = System.getProperty("ccm.directory");

  public static final String BRANCH = System.getProperty("ccm.branch");

  public static final Boolean DSE_ENABLEMENT = Boolean.getBoolean("ccm.dse");

  public static final Boolean SCYLLA_ENABLEMENT = Boolean.getBoolean("ccm.scylla");

  public static final Boolean SCYLLA_ENTERPRISE =
      String.valueOf(VERSION.getMajor()).matches("\\d{4}");

  public static final String CLUSTER_NAME = "ccm_1";

  public static final String DEFAULT_CLIENT_TRUSTSTORE_PASSWORD = "scylla1sfun";
  public static final String DEFAULT_CLIENT_TRUSTSTORE_PATH = "/client.truststore";

  public static final File DEFAULT_CLIENT_TRUSTSTORE_FILE =
      createTempStore(DEFAULT_CLIENT_TRUSTSTORE_PATH);

  public static final String DEFAULT_CLIENT_KEYSTORE_PASSWORD = "scylla1sfun";
  public static final String DEFAULT_CLIENT_KEYSTORE_PATH = "/client.keystore";

  public static final File DEFAULT_CLIENT_KEYSTORE_FILE =
      createTempStore(DEFAULT_CLIENT_KEYSTORE_PATH);

  // Contains the same keypair as the client keystore, but in format usable by OpenSSL
  public static final File DEFAULT_CLIENT_PRIVATE_KEY_FILE = createTempStore("/client.key");
  public static final File DEFAULT_CLIENT_CERT_CHAIN_FILE = createTempStore("/client.crt");

  public static final String DEFAULT_SERVER_TRUSTSTORE_PASSWORD = "scylla1sfun";
  public static final String DEFAULT_SERVER_TRUSTSTORE_PATH = "/server.truststore";

  public static final String DEFAULT_SERVER_TRUSTSTORE_PEM_PATH = "/server.truststore.pem";

  private static final File DEFAULT_SERVER_TRUSTSTORE_FILE =
      createTempStore(DEFAULT_SERVER_TRUSTSTORE_PATH);
  private static final File DEFAULT_SERVER_TRUSTSTORE_PEM_FILE =
      createTempStore(DEFAULT_SERVER_TRUSTSTORE_PEM_PATH);

  public static final String DEFAULT_SERVER_KEYSTORE_PASSWORD = "scylla1sfun";
  public static final String DEFAULT_SERVER_KEYSTORE_PATH = "/server.keystore";

  // Contain the same keypair as the server keystore, but in format usable by Scylla
  public static final String DEFAULT_SERVER_PRIVATE_KEY_PATH = "/server.key";
  public static final String DEFAULT_SERVER_CERT_CHAIN_PATH = "/server.crt";

  private static final File DEFAULT_SERVER_KEYSTORE_FILE =
      createTempStore(DEFAULT_SERVER_KEYSTORE_PATH);
  private static final File DEFAULT_SERVER_PRIVATE_KEY_FILE =
      createTempStore(DEFAULT_SERVER_PRIVATE_KEY_PATH);
  private static final File DEFAULT_SERVER_CERT_CHAIN_FILE =
      createTempStore(DEFAULT_SERVER_CERT_CHAIN_PATH);

  // A separate keystore where the certificate has a CN of localhost, used for hostname
  // validation testing.
  public static final String DEFAULT_SERVER_LOCALHOST_KEYSTORE_PATH = "/server_localhost.keystore";

  private static final File DEFAULT_SERVER_LOCALHOST_KEYSTORE_FILE = null;
  // @IntegrationTestDisabledCassandra3Failure @IntegrationTestDisabledSSL
  // = createTempStore(DEFAULT_SERVER_LOCALHOST_KEYSTORE_PATH);

  // major DSE versions
  private static final Version V6_0_0 = Version.parse("6.0.0");
  private static final Version V5_1_0 = Version.parse("5.1.0");
  private static final Version V5_0_0 = Version.parse("5.0.0");

  // mapped C* versions from DSE versions
  private static final Version V4_0_0 = Version.parse("4.0.0");
  private static final Version V3_10 = Version.parse("3.10");
  private static final Version V3_0_15 = Version.parse("3.0.15");
  private static final Version V2_1_19 = Version.parse("2.1.19");

  private static final Map<String, String> ENVIRONMENT_MAP;

  static {
    Map<String, String> envMap = Maps.newHashMap(new ProcessBuilder().environment());
    if (DSE_ENABLEMENT) {
      LOG.info("CCM Bridge configured with DSE version {}", VERSION);
    } else if (SCYLLA_ENABLEMENT) {
      LOG.info("CCM Bridge configured with Scylla version {}", VERSION);
      if (SCYLLA_ENTERPRISE) {
        envMap.put("SCYLLA_PRODUCT", "enterprise");
      }
    } else {
      LOG.info("CCM Bridge configured with Apache Cassandra version {}", VERSION);
    }

    // If ccm.path is set, override the PATH variable with it.
    String ccmPath = System.getProperty("ccm.path");
    if (ccmPath != null) {
      String existingPath = envMap.get("PATH");
      if (existingPath == null) {
        existingPath = "";
      }
      envMap.put("PATH", ccmPath + File.pathSeparator + existingPath);
    }

    // If ccm.java.home is set, override the JAVA_HOME variable with it.
    String ccmJavaHome = System.getProperty("ccm.java.home");
    if (ccmJavaHome != null) {
      envMap.put("JAVA_HOME", ccmJavaHome);
    }
    ENVIRONMENT_MAP = ImmutableMap.copyOf(envMap);
  }

  private final int[] nodes;
  private final Path configDirectory;
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean created = new AtomicBoolean();
  private final String ipPrefix;
  private final Map<String, Object> cassandraConfiguration;
  private final Map<String, Object> dseConfiguration;
  private final List<String> rawDseYaml;
  private final List<String> createOptions;
  private final List<String> dseWorkloads;
  private final String jvmArgs;

  private CcmBridge(
      Path configDirectory,
      int[] nodes,
      String ipPrefix,
      Map<String, Object> cassandraConfiguration,
      Map<String, Object> dseConfiguration,
      List<String> dseConfigurationRawYaml,
      List<String> createOptions,
      Collection<String> jvmArgs,
      List<String> dseWorkloads) {
    this.configDirectory = configDirectory;
    if (nodes.length == 1) {
      // Hack to ensure that the default DC is always called 'dc1': pass a list ('-nX:0') even if
      // there is only one DC (with '-nX', CCM configures `SimpleSnitch`, which hard-codes the name
      // to 'datacenter1')
      this.nodes = new int[] {nodes[0], 0};
    } else {
      this.nodes = nodes;
    }
    this.ipPrefix = ipPrefix;
    this.cassandraConfiguration = cassandraConfiguration;
    this.dseConfiguration = dseConfiguration;
    this.rawDseYaml = dseConfigurationRawYaml;
    this.createOptions = createOptions;

    if ((getCassandraVersion().nextStable().compareTo(Version.V4_1_0) >= 0)
        && !jvmArgs.contains("-Dcassandra.allow_new_old_config_keys=false")
        && !jvmArgs.contains("-Dcassandra.allow_new_old_config_keys=true")) {
      // FIXME: This should not be needed as soon as Scylla CCM adjusts to new names
      jvmArgs.add("-Dcassandra.allow_new_old_config_keys=true");
      jvmArgs.add("-Dcassandra.allow_duplicate_config_keys=false");
    }
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
    this.dseWorkloads = dseWorkloads;
  }

  // Copied from Netty's PlatformDependent to avoid the dependency on Netty
  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase(Locale.US).contains("win");
  }

  public Optional<Version> getScyllaVersion() {
    return SCYLLA_ENABLEMENT ? Optional.of(VERSION) : Optional.empty();
  }

  public Optional<String> getScyllaUnparsedVersion() {
    return SCYLLA_ENABLEMENT ? Optional.of(System.getProperty("ccm.version")) : Optional.empty();
  }

  public Optional<Version> getDseVersion() {
    return DSE_ENABLEMENT ? Optional.of(VERSION) : Optional.empty();
  }

  public Version getCassandraVersion() {
    if (DSE_ENABLEMENT) {
      Version stableVersion = VERSION.nextStable();
      if (stableVersion.compareTo(V6_0_0) >= 0) {
        return V4_0_0;
      } else if (stableVersion.compareTo(V5_1_0) >= 0) {
        return V3_10;
      } else if (stableVersion.compareTo(V5_0_0) >= 0) {
        return V3_0_15;
      } else {
        return V2_1_19;
      }
    } else if (SCYLLA_ENABLEMENT) {
      return V4_0_0;
    } else {
      // Regular Cassandra
      return VERSION;
    }
  }

  private String getCcmVersionString(Version version) {
    if (SCYLLA_ENABLEMENT) {
      // Scylla OSS versions before 5.1 had RC versioning scheme of 5.0.rc3.
      // Scylla OSS versions after (and including 5.1) have RC versioning of 5.1.0-rc3.
      // A similar situation occurs with Scylla Enterprise after 2022.2.
      //
      // CcmBridge parses the version numbers to a newer format (5.1.0-rc3), so a replacement
      // must be performed for older Scylla version numbers.
      String versionString = version.toString();

      boolean shouldReplace =
          (SCYLLA_ENTERPRISE && version.compareTo(Version.parse("2022.2.0-rc0")) < 0)
              || (!SCYLLA_ENTERPRISE && version.compareTo(Version.parse("5.1.0-rc0")) < 0);
      if (shouldReplace) {
        versionString = versionString.replace(".0-", ".");
      }
      return "release:" + versionString;
    }
    // for 4.0 pre-releases, the CCM version string needs to be "4.0-alpha1" or "4.0-alpha2"
    // Version.toString() always adds a patch value, even if it's not specified when parsing.
    if (version.getMajor() == 4
        && version.getMinor() == 0
        && version.getPatch() == 0
        && version.getPreReleaseLabels() != null) {
      // truncate the patch version from the Version string
      StringBuilder sb = new StringBuilder();
      sb.append(version.getMajor()).append('.').append(version.getMinor());
      for (String preReleaseString : version.getPreReleaseLabels()) {
        sb.append('-').append(preReleaseString);
      }
      return sb.toString();
    }
    return version.toString();
  }

  public void create() {
    if (created.compareAndSet(false, true)) {
      if (INSTALL_DIRECTORY != null) {
        createOptions.add("--install-dir=" + new File(INSTALL_DIRECTORY).getAbsolutePath());
      } else if (BRANCH != null) {
        createOptions.add("-v git:" + BRANCH.trim().replaceAll("\"", ""));

      } else {
        createOptions.add("-v " + getCcmVersionString(VERSION));
      }
      if (DSE_ENABLEMENT) {
        createOptions.add("--dse");
      }
      if (SCYLLA_ENABLEMENT) {
        createOptions.add("--scylla");
      }
      execute(
          "create",
          CLUSTER_NAME,
          "-i",
          ipPrefix,
          "-n",
          Arrays.stream(nodes).mapToObj(n -> "" + n).collect(Collectors.joining(":")),
          createOptions.stream().collect(Collectors.joining(" ")));

      // Collect all cassandraConfiguration (and others) into a single "ccm updateconf" call.
      // Works around the behavior introduced in https://github.com/scylladb/scylla-ccm/pull/410
      StringBuilder updateConfArguments = new StringBuilder();

      for (Map.Entry<String, Object> conf : cassandraConfiguration.entrySet()) {
        updateConfArguments.append(conf.getKey()).append(':').append(conf.getValue()).append(' ');
      }
      if (getCassandraVersion().compareTo(Version.V2_2_0) >= 0 && !SCYLLA_ENABLEMENT) {
        // @IntegrationTestDisabledScyllaJVMArgs @IntegrationTestDisabledScyllaUDF
        if (getCassandraVersion().compareTo(Version.V4_1_0) >= 0) {
          updateConfArguments.append("user_defined_functions_enabled:true").append(' ');

        } else {
          updateConfArguments.append("enable_user_defined_functions:true").append(' ');
        }
      }

      if (updateConfArguments.length() > 0) {
        execute("updateconf", updateConfArguments.toString());
      }
      if (DSE_ENABLEMENT) {
        for (Map.Entry<String, Object> conf : dseConfiguration.entrySet()) {
          execute("updatedseconf", String.format("%s:%s", conf.getKey(), conf.getValue()));
        }
        for (String yaml : rawDseYaml) {
          executeUnsanitized("updatedseconf", "-y", yaml);
        }
        if (!dseWorkloads.isEmpty()) {
          execute("setworkload", String.join(",", dseWorkloads));
        }
      }
    }
  }

  public void nodetool(int node, String... args) {
    execute(String.format("node%d nodetool %s", node, Joiner.on(" ").join(args)));
  }

  public void dsetool(int node, String... args) {
    execute(String.format("node%d dsetool %s", node, Joiner.on(" ").join(args)));
  }

  public void reloadCore(int node, String keyspace, String table, boolean reindex) {
    dsetool(node, "reload_core", keyspace + "." + table, "reindex=" + reindex);
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      try {
        execute("start", jvmArgs, "--wait-for-binary-proto");
      } catch (RuntimeException re) {
        // if something went wrong starting CCM, see if we can also dump the error
        executeCheckLogError();
        throw re;
      }
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

  public void pause(int n) {
    execute("node" + n, "pause");
  }

  public void resume(int n) {
    execute("node" + n, "resume");
  }

  public void start(int n) {
    // FIXME: This should not be needed as soon as Scylla CCM adjusts to new names
    if (getCassandraVersion().compareTo(Version.V4_1_0) >= 0) {
      execute(
          "node" + n,
          "start",
          "--jvm_arg=-Dcassandra.allow_new_old_config_keys=true",
          "--jvm_arg=-Dcassandra.allow_duplicate_config_keys=false");
    } else {
      execute("node" + n, "start");
    }
  }

  public void stop(int n) {
    execute("node" + n, "stop");
  }

  public void add(int n, String dc) {
    if (getDseVersion().isPresent()) {
      execute("add", "-i", ipPrefix + n, "-d", dc, "node" + n, "--dse");
    } else {
      execute("add", "-i", ipPrefix + n, "-d", dc, "node" + n);
    }
    start(n);
  }

  public void decommission(int n) {
    nodetool(n, "decommission");
  }

  synchronized void execute(String... args) {
    String command =
        "ccm "
            + String.join(" ", args)
            + " --config-dir="
            + configDirectory.toFile().getAbsolutePath();

    execute(CommandLine.parse(command));
  }

  synchronized void executeUnsanitized(String... args) {
    String command = "ccm ";

    CommandLine cli = CommandLine.parse(command);
    for (String arg : args) {
      cli.addArgument(arg, false);
    }
    cli.addArgument("--config-dir=" + configDirectory.toFile().getAbsolutePath());

    execute(cli);
  }

  private void executeCheckLogError() {
    String command = "ccm checklogerror --config-dir=" + configDirectory.toFile().getAbsolutePath();
    execute(CommandLine.parse(command));
  }

  private void execute(CommandLine cli) {
    LOG.info("Executing: " + cli);
    ExecuteWatchdog watchDog = new ExecuteWatchdog(TimeUnit.MINUTES.toMillis(10));
    try (LogOutputStream outStream =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                LOG.info("ccmout> {}", line);
              }
            };
        LogOutputStream errStream =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                LOG.error("ccmerr> {}", line);
              }
            }) {
      Executor executor = new DefaultExecutor();
      ExecuteStreamHandler streamHandler = new PumpStreamHandler(outStream, errStream);
      executor.setStreamHandler(streamHandler);
      executor.setWatchdog(watchDog);

      int retValue = executor.execute(cli, ENVIRONMENT_MAP);
      if (retValue != 0) {
        LOG.error("Non-zero exit code ({}) returned from executing ccm command: {}", retValue, cli);
      }
    } catch (IOException ex) {
      if (watchDog.killedProcess()) {
        throw new RuntimeException("The command '" + cli + "' was killed after 10 minutes");
      } else {
        throw new RuntimeException("The command '" + cli + "' failed to execute", ex);
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
    try (OutputStream os = new FileOutputStream(f = File.createTempFile("server", ".store"))) {
      f.deleteOnExit();
      Resources.copy(CcmBridge.class.getResource(storePath), os);
    } catch (IOException e) {
      LOG.warn("Failure to write keystore, SSL-enabled servers may fail to start.", e);
    }
    return f;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private int[] nodes = {1};
    private final Map<String, Object> cassandraConfiguration = new LinkedHashMap<>();
    private final Map<String, Object> dseConfiguration = new LinkedHashMap<>();
    private final List<String> dseRawYaml = new ArrayList<>();
    private final List<String> jvmArgs = new ArrayList<>();
    private String ipPrefix = "127.0.0.";
    private final List<String> createOptions = new ArrayList<>();
    private final List<String> dseWorkloads = new ArrayList<>();

    private final Path configDirectory;

    private Builder() {
      try {
        this.configDirectory = Files.createTempDirectory("ccm");
        // mark the ccm temp directories for deletion when the JVM exits
        this.configDirectory.toFile().deleteOnExit();
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

    public Builder withDseConfiguration(String key, Object value) {
      dseConfiguration.put(key, value);
      return this;
    }

    public Builder withDseConfiguration(String rawYaml) {
      dseRawYaml.add(rawYaml);
      return this;
    }

    public Builder withJvmArgs(String... jvmArgs) {
      Collections.addAll(this.jvmArgs, jvmArgs);
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

    /** Adds an option to the {@code ccm create} command. */
    public Builder withCreateOption(String option) {
      this.createOptions.add(option);
      return this;
    }

    /** Enables SSL encryption. */
    public Builder withSsl() {
      cassandraConfiguration.put("client_encryption_options.enabled", "true");
      if (SCYLLA_ENABLEMENT) {
        cassandraConfiguration.put(
            "client_encryption_options.certificate",
            DEFAULT_SERVER_CERT_CHAIN_FILE.getAbsolutePath());
        cassandraConfiguration.put(
            "client_encryption_options.keyfile", DEFAULT_SERVER_PRIVATE_KEY_FILE.getAbsolutePath());
      } else {
        cassandraConfiguration.put("client_encryption_options.optional", "false");
        cassandraConfiguration.put(
            "client_encryption_options.keystore", DEFAULT_SERVER_KEYSTORE_FILE.getAbsolutePath());
        cassandraConfiguration.put(
            "client_encryption_options.keystore_password", DEFAULT_SERVER_KEYSTORE_PASSWORD);
      }
      return this;
    }

    public Builder withSslLocalhostCn() {
      // FIXME: Add Scylla support.
      // @IntegrationTestDisabledCassandra3Failure @IntegrationTestDisabledSSL
      cassandraConfiguration.put("client_encryption_options.enabled", "true");
      cassandraConfiguration.put("client_encryption_options.optional", "false");
      cassandraConfiguration.put(
          "client_encryption_options.keystore",
          DEFAULT_SERVER_LOCALHOST_KEYSTORE_FILE.getAbsolutePath());
      cassandraConfiguration.put(
          "client_encryption_options.keystore_password", DEFAULT_SERVER_KEYSTORE_PASSWORD);
      return this;
    }

    /** Enables client authentication. This also enables encryption ({@link #withSsl()}. */
    public Builder withSslAuth() {
      withSsl();
      cassandraConfiguration.put("client_encryption_options.require_client_auth", "true");
      if (SCYLLA_ENABLEMENT) {
        cassandraConfiguration.put(
            "client_encryption_options.truststore",
            DEFAULT_SERVER_TRUSTSTORE_PEM_FILE.getAbsolutePath());
      } else {
        cassandraConfiguration.put(
            "client_encryption_options.truststore",
            DEFAULT_SERVER_TRUSTSTORE_FILE.getAbsolutePath());
        cassandraConfiguration.put(
            "client_encryption_options.truststore_password", DEFAULT_SERVER_TRUSTSTORE_PASSWORD);
      }
      return this;
    }

    public Builder withDseWorkloads(String... workloads) {
      this.dseWorkloads.addAll(Arrays.asList(workloads));
      return this;
    }

    public CcmBridge build() {
      return new CcmBridge(
          configDirectory,
          nodes,
          ipPrefix,
          cassandraConfiguration,
          dseConfiguration,
          dseRawYaml,
          createOptions,
          jvmArgs,
          dseWorkloads);
    }
  }
}
